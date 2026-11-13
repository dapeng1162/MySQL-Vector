# smart_sched：9.7 + sched_ext 联合调度设计

状态：设计基线（2026-11-13，分支 smart_sched，基于 9.7 `008e09c28`）
前置证据：`Docs/sched-v7-redesign.md`、
`aliyun/mysql-sched-bench/results/c5n2-20261112/summary.md`（全部实测）

## 1. 目标与判据

1. **干净吞吐持平或更优**：sysbench oltp_read_write 全并发档与 TPC-C，
   相对上游 9.7（V97, CFS）≥ -2%（目标 0% 以上）。
2. **负向场景严格更优**：N1（16 外部 CPU hogs + 开环点查）、N2（rw64
   前台 + 8 个 BENCHMARK 后台会话）、高争用（latch/lock 密集）下，
   尾部延迟与吞吐韧性优于 V97。
3. 手段限定：sched_ext 联合调度；latch（InnoDB mutex/rw-lock）与
   lock（事务锁）都做调度优化。业务优先级/resource group 不在 DB 内
   做（归中间件）。

## 2. 实测证据基础（为什么长这样）

v6.4（coroutine-dev2 全局 DSQ + 100µs kick + 语义全家桶）rw t256
-21.8%、N1@50k max 117ms、N2 mixed p99 3× 劣化。v7-rc1 消融实验把
-21.8% 拆开：

| 因素 | 占比 | 证据 |
|---|---|---|
| 100µs fast-path kick（IPI 风暴） | ~11pp | R0（无 kick）比 E4（100µs kick）高 12.9% |
| mysqld 埋点 | ~3pp | C5n（CFS+埋点）-2.9% |
| 语义机制（全局 DSQ + tilt + cap 等） | ~3.4pp | v6.4 全开 vs 全惰性 |
| scx 平台真实底座 | ~6pp | R0 vs C5n |

**核心结论**：
- CFS 的原地唤醒抢占是每事件近零成本；scx 的 SCX_KICK_PREEMPT
  （IPI + slice 清零 + forced dispatch）随频率线性变贵
  （-3% @ 2k/CPU/s、-9.3% @ 8k/CPU/s）。**kick 只能按预算花在刀刃上**。
- EEVDF 的 lag 排序（池首位置）不等于 CFS 的交互倾斜——醒来后还要
  等 running 任务让出 CPU，无抢占时这步是半个 slice。混合负载的
  倾斜必须由抢占或有界语义倾斜补齐。
- CPU-bound 检测器自 v6.4 起是死代码（decay 含自身运行时间），
  修复后 hog cap 才真正生效，rw 代价 ~0-3%，N1@10k p99 11.45→3.02ms。
- 语义机制至今没有实测正收益——**每个语义机制必须单独过验收门**。

## 3. 架构

```
                 ┌──────────────────────────────────────┐
                 │  mysqld (9.7 + sched_ext bridge)     │
                 │  commit/rollback 窗口                 │
                 │  每命令 lock_holder 分类（无等待=零成本）│
                 │  InnoDB mutex/rw-latch 等待边         │
                 │  事务锁等待边（waiter→holder）         │
                 │  InnoDB 后台线程 = BG tier            │
                 └──────────────┬───────────────────────┘
                     BPF maps（/sys/fs/bpf/scx_db_sched/）
                 ┌──────────────┴───────────────────────┐
                 │  scx_db_sched（BPF 调度器，loader 常驻）│
                 │  单一 per-CPU vtime DSQ 路径          │
                 │  lag clamp + 自适应 slice + hog cap   │
                 │  语义倾斜：commit/latch/lock（开关制）  │
                 │  语义 kick：仅池首被阻挡时花 IPI       │
                 └──────────────────────────────────────┘
```

与 coroutine-dev2 的关系：只移植调度相关代码，**不使用 reactor/协程
架构**。9.7 原生 one-thread-per-connection 就是基线；bridge 未连接时
（无 loader、无 sched_ext 内核、WITH_SCX_SCHEDULER=OFF）服务器与
上游逐字节同行为，埋点零成本。

## 4. 调度器分层

### L0 底座（默认路径，目标 <2% 代价）

- 单一 per-CPU vtime DSQ：`vr = clamp(dsq_vtime, vtime_now - MAX_LAG)`，
  `insert_vtime(p, cpu_dsq, slice, vr)`。无全局队列。
- 自适应 slice：池深 0-1/2-3/4-7/≥8 → 20/10/5/2.5ms。
- CPU-bound cap（已修复的记账）：净在核 >20ms 的任务 cap 到
  `--hog-slice-us`（默认 2.5ms），界定 hog 对唤醒任务的最大加塞等待。
- 精确区间记账：running→stopping 实耗计费，kick 不清账。
- dispatch：本地池 → 邻居轮转 steal。
- **默认无 kick**（rw 上 kick 是负资产）。

### L1 语义倾斜（开关制，默认逐个过门后开启）

只倾斜 vtime，不改队列、不动 slice：
- **commit tilt**：COMMIT/ROLLBACK 窗口内 vd -= B_COMMIT（有界），
  尽快提交释放锁与日志资源。
- **lock holder tilt**：有等待者的事务锁持有者 vd -= B_HOLD，
  等价于"让锁尽快放行"。
- **latch holder tilt + PI**：latch 持有者继承最急等待者的
  vd floor（深度 ≤3，时间戳过期失效），让临界区尽快离场。
- 全部倾斜走 **borrow/repay**：窗口内借的 CPU 时间累积为 debt，
  窗口关闭后 vd += debt 偿还（cgroup cpu burst 式借还，总量公平）。
- BG tier（purge 等 DB 内部线程）低权重——数据库内部知识，非业务 QoS。

### L2 语义 kick（预算制，按需）

只有当**池首任务被 CPU-bound 任务（hog/BG）阻挡**时才发 kick：
语义知识把 IPI 花在最值的地方。粒度过滤（默认 1ms，可调 100µs）
+ 每 CPU token bucket（默认 2048/s）。无唤醒/无阻挡不 kick。

### 埋点成本控制

- 常规命令：`any_lock_waits()` 单次原子读，无锁等待时零 map 操作。
- latch/lock 等待边：仅在真实 OS 等待发生时才写 map。
- commit 窗口：每事务最多 2 次 map 写。
- 后续独立消融：若埋点 >2%，改条件发布（仅争用活跃时启用）。

## 5. 验收门（同 v7 的纪律：不过门不进下一阶段）

| 阶段 | 内容 | 干净负载门 | 负向门 |
|---|---|---|---|
| S0 | 9.7 + bridge（不挂 loader） | 与 V97 差值 ≤0.5%（验证零埋点成本） | — |
| S1 | + L0 底座（无 kick 无倾斜） | rw 各档 ≥-2%；TPC-C ≥-2% | N1/N2 不劣于 V97 |
| S2 | + commit tilt | 同上不回退 | 锁等待场景 p99 改善为期望信号 |
| S3 | + latch/lock tilt + PI | 同上不回退 | 高争用场景 p99/吞吐改善 |
| S4 | + 语义 kick（预算制） | rw 代价 ≤2% | N1@50k 不 FATAL、N2 mixed p99 ≤V97 |

测试矩阵（沿用 c5n2 协议，loader enabled 硬断言 + redo 4G）：
rw t8-2048 曲线、TPC-C、点查开环 rate ladder、N1（base/noise ×
10k/50k）、N2（solo/mixed + BG 完成数）、N3（锁争用/HTAP 场景按
论文矩阵补）。每档同日对照 V97 重测（日内漂移 ±4%）。

## 6. 与旧路线的差异（为什么这次能成）

1. **没有 coroutine 架构税**：不背 reactor/协程的额外切换与解析开销；
   one-thread-per-connection + 调度器是最小变量。
2. **kick 从本能变成预算**：默认关，只在语义判据成立时花 IPI。
3. **cap 是真 cap**：死代码已修，hog 的加塞等待有界。
4. **语义只买三种东西**：commit 早完成、锁早放行、latch 早离场——
   都是"缩短关键路径"的通用优化，不需要业务标注。
5. **每步可回退**：全部机制独立开关 + 独立计数器；任何机制连续
   两个验收周期无正收益即删除。

## 7. 风险与开放项

1. scx 底座残余 ~6%（R0 vs C5n）来自 BPF 每次 enqueue/dispatch 固定
   开销与 wake_affine 缺失；smart_sched 的 L0 先测同样的底座（无
   coroutine 干扰后数字应更干净）。
2. N1@50k / N2 的 CFS 原地抢占优势仍是最大结构差距；S4 的语义 kick
   若不能覆盖，考虑条件式 attach（平时 CFS，威胁时 scx）。
3. tid 复用与 gate stale：bridge disable() 清扫 + TTL 兜底，需
   bpftrace 验证 task_account 命中率（v6.4 的 117ms 尾部疑似相关）。
4. 9.7 的 InnoDB latch 等待边 peek_owner() 的持有者可解析率
   （latch_wait_edge/wait 比率）影响 L1 latch 倾斜的覆盖面，首轮
   计数器必须核验。
