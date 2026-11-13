# smart_sched v2 验收结果（2026-11-13）

分支 `smart_sched`（v2 native-first 设计，见 `Docs/smart-sched-design.md`）。
基线为上游 9.7 `008e09c28`。本文档记录 v2 实现（D1–D4）在 2026-11-13
基准节点上的完整测试结果：干净负载（rwscaling / ratescan）与负向
稳定性矩阵（N1 外部 CPU 噪声、N2 混合负载）。

代码版本：

- `6d89bcbc8` D1：v2 最小 bridge（等待边身份 + 引用计数 + holder TLS 边栈）
- `d240d88d4` D2：v2 最小调度器（canonical vruntime + per-CPU vtime DSQ）
- `07e43b7ea` D3：hog cap 默认 2.5ms → 6ms（rwscaling 实验依据）
- `47c083e06` D4：pin `task_account_map`/`vtime_cpu`（卡死取证路径）

## 0. 环境与配置定义

- DB：`ecs.c8i.4xlarge`（16 vCPU），loadgen：`ecs.c8i.2xlarge`（8 vCPU），
  均 spot，`cn-wulanchabu`。镜像：Ubuntu 26.04（alibase），内核
  `7.0.0-38`（EEVDF 原生可用，`CONFIG_SCHED_CLASS_EXT=y`）。
- 同一数据目录（sysbench 32 表 × 100k / tpcc 8WH），两套二进制轮流启停，
  绝不并发；每次 SCX 运行前强制校验 `/sys/kernel/sched_ext/state=enabled`
  与 `Sched_ext_connected=1`，否则测试判失败。
- 配置：

| 标签 | 含义 |
|---|---|
| V97 | 原生 9.7 mysqld，one-thread-per-connection，内核原生 EEVDF |
| SN | smart_sched 构建，loader 不 attach（bridge 空转），内核原生 EEVDF |
| SG | smart_sched + scx_db_sched 接管（GUARDED 公平底座，无语义） |
| SS | SG + `--semantic`（commit/holder 语义加权） |

- 当天节点发生过一次 spot 回收换机（同规格同镜像同内核）。所有列出的
  结论均在新节点上复测过；单节点独有现象会单独标注。

## 1. 结论总表

| 场景 | 关键数字 | 判定 |
|---|---|---|
| 零埋点成本（SN vs V97，rw scaling） | t32~t256 全档 98.5%~102% | **通过**（埋点关闭时无回归） |
| 开环点查容量内（25k–250k qps） | SG 与 V97 全档持平（偏差 <0.1%） | **通过** |
| 开环点查 300k | SG 确定性卡死，V97 正常（299,897 tps） | **不通过**（open bug，见 §3） |
| 闭环 rw（SG@cap2.5ms） | t64 -5.2%、p95 15.6ms vs 9.4ms | **未达标** || 闭环 rw（SG@cap6ms，D3 新默认） | rw64 9,551 vs V97 9,778（-2.3%） | 部分收敛（见 §2.2） |
| N2 混合负载 | SG/SS mixed tps -31%、p99 123ms vs V97 17.3ms；语义 boost 在打但无效 | **不通过**（根因=底座无 lag 携带，见 §4.1） |
| N1 外部噪声 | SG 干净 0.13ms 持平，但干扰下 p99 7.2–7.8ms（V97 的 12–30 倍）、max 381ms | **不通过**（同 N2 根因，见 §5） |

**整体判定**：v2 的两个基础性目标达成——埋点零成本（SN 全场景
与 V97 持平）、干净负载无回退（容量内开环全档持平、t32 反超）；
但 v2 的核心目标"负向韧性"**当前不成立**：N1/N2 两个负向场景
GUARDED 调度器都显著劣于原生 EEVDF，300k 存在确定性卡死。
三者共同根因是底座缺 EEVDF 的 lag 携带与唤醒放置（§4.1/§5/§3.1），
已列为 D5 最高优先级。**在 D5 落地并重测通过之前，不应在生产
窗口启用 scx_db_sched。**

## 2. sysbench oltp_read_write（闭环）

32 表 × 100k，`rand-type=uniform`，文本协议，redo 4G，`flush_at_trx_commit=2`。
t32/t64/t128/t256 四档，每档两轮交错取均值。SG 数据采集时 hog cap 仍为
旧默认 2.5ms。

| 配置 | t32 | t64 | t128 | t256 |
|---|---:|---:|---:|---:|
| V97 (tps) | 8,364 | 9,778 | 10,222 | 10,076 |
| SN (tps) | 8,236 | 9,885 | 9,933 | 9,779 |
| SG (tps, cap2.5ms) | 8,403 | 9,273 | 9,358 | 9,256 |
| SG / V97 | +0.5% | **-5.2%** | **-8.4%** | **-8.1%** |
| SG p95 (ms) | 4.21 | 15.59 | 32.53 | 61.17 |
| V97 p95 (ms) | 5.18 | 9.39 | 18.28 | 37.91 |

### 2.1 归因

- SN 全档与 V97 持平：**埋点在 loader 未 attach 时零成本**，D1 bridge
  设计目标达成。
- 差距全部来自 SCX 调度器接管（SG）。t32 单档 SG 反而略优于 V97
  （+0.5%），说明差距随并发上升、与锁等待路径相关，不是固定税。
- 300k 点查卡死（§3）同样指向等待/唤醒路径，与 rw 差距同源。

### 2.2 hog cap 消融与 D3

rw64 单点（同日节点2，V97=9,778）：

| cap | tps | 相对 V97 |
|---|---:|---:|
| 2.5ms（旧默认） | 9,213 | -5.8% |
| 6ms（**D3 新默认**） | 9,551 | **-2.3%** |
| 20ms（事实禁用） | 9,499 | -2.9% |

2.5ms 的 cap 把闭环里的忙碌连接线程也切了片，伤害了 OLTP 自身；
放宽到 20ms 与 6ms 收益相同，说明剩余 -2.3% 不是 cap 造成的，而是
底座（无 lag 放置、无唤醒 kick）与 EEVDF 的差距。据此 D3 把默认
cap 改为 6ms，保留对真实 CPU hog 的约束（§4 N2 依赖它）。

## 3. sysbench oltp_point_select 开环速率阶梯

`--rate` 从 25k 到 300k，60s/档（300k 档 30s+取证），实测吞吐 / 目标速率：

| 目标 | V97 | SN | SG |
|---:|---:|---:|---:|
| 25k | 24,995 | 25,029 | 25,021 |
| 50k | 50,096 | 49,947 | 49,949 |
| 100k | 99,950 | 99,902 | 100,013 |
| 150k | 150,026 | 150,168 | 149,837 |
| 200k | 199,869 | 199,965 | 200,005 |
| 250k | 249,944 | 249,988 | 249,862 |
| 300k | 299,897 | 299,937 | **卡死（见下）** |

容量内（≤250k）SG 与 V97、SN 完全持平。

### 3.1 open bug：SG@300k 确定性卡死

两个节点独立复现，现象完全一致：

- sysbench 64 线程只起来一部分（`thds: 21`），已连接线程 tps=0.00
  持续 800s+（`results/v2-20261113/ratescan/SG/r300000.log`）；
- DB 侧 11 个连接线程停在 `futex_wait`，其余连接空闲；
- loader 统计冻结；杀掉 sysbench 后部分连接仍不退出；
- 判定为调度器**丢唤醒**类 bug：≥300k qps 时唤醒风暴路径上存在
  队列/状态不一致，尚未定位。

取证缺陷（`bpftool map show name` 在卡死现场拿不到 loader 持有 map 的
id）已由 D4 修复：loader 现在把 `task_account_map`/`vtime_cpu` 与
`task_sem`/`wait_edges` 一起 pin 到 `/sys/fs/bpf/scx_db_sched/`，
下次复现可用 `bpftool map dump pinned ...` 直接取每任务 vruntime/debt。

下一步：kernel 侧跟踪（scx tracepoints / bpftrace on `scx_dispatch`、
`scx_kick`）对比 250k 与 300k 的唤醒/dispatch 序列，重点怀疑
per-CPU vtime DSQ 在跨核唤醒时的排序键回退路径。

## 4. N2 混合负载（前台 OLTP + 8 个 CPU hog 后台会话）

方法：warmup 30s → **solo**（rw64 120s，纯前台）→ 起 8 个 DB 内
`SELECT BENCHMARK(25000000, SHA2('x',256))` 会话 → 稳定 10s →
**mixed**（rw64 120s）→ 记录窗口内 BG 完成数与 history list。
判定标准：mixed 期 FG p99 不劣于 V97、BG 完成数同数量级（不饿死）、
history list 不失控。

| 配置 | solo tps | solo p99(ms) | mixed tps | mixed p99(ms) | BG 完成数(120s) | history list |
|---|---:|---:|---:|---:|---:|---|
| V97 | 9,995 | 11.04 | 8,047 | 17.32 | 141 | 384→476 |
| SN | 9,898 | 11.04 | 8,243 | 16.41 | 128 | 495→696 |
| SG | 9,533 | 34.95 | 5,544 | 123.28 | 257 | 2→2152 |
| SS | 9,462 | 35.59 | 5,551 | 123.28 | 256 | 1→0 |

### 4.1 判定与根因

**SG/SS 均不通过 N2 门槛**：mixed FG tps 比 V97 低 31%，p99 123ms
是 V97（17.3ms）的 7 倍；同时 BG 完成数反而多 80%（257 vs 141），
history list 在 SG 下漂移到 2,152（V97 384→476）。**这个场景下
GUARDED 调度器让前台 OLTP 输给了后台 CPU hog，比原生 EEVDF 差。**

V97 自身对照：BG hog 使 FG tps 降 19.5%、p99 +57%——原生 EEVDF
靠 lag 携带天然压住 hog 对交互负载的伤害；SG/SS 的目标本应比它更好，
实际更差。

SS 的语义路径确认在活跃工作（120s mixed 窗口 semantic_hits≈28.9 万、
commit_boost≈26.3 万、holder_boost≈2.7 万、boosted_runs≈28.9 万），
且 mysqld 侧 bridge 计数交叉验证一致（`Sched_ext_connected=1`，
latch 等待边 1.28 万/1.34 万），但 mixed 指标与 SG 完全相同。
结论：**commit/holder 微调无法弥补底座的延迟盲区**——问题出在
公平基线本身，不在语义权重。

根因分析（与 §2.1、§3.1 同源）：

1. 底座是纯吞吐公平的 canonical vruntime：睡眠唤醒后 vtime 归一到
   全局地板，前台 OLTP 线程（频繁阻塞-唤醒）不累积任何"迟交补偿"，
   与 8 个连续吃 6ms 时间片的 BENCHMARK hog 在队列里平起平坐。
   EEVDF 之所以能保护交互负载，靠的是 sleep 期间 lag 持续累积、
   eligible 时间（虚拟截止期）前移；GUARDED 底座没有这个机制。
2. 6ms hog cap 对干净闭环 OLTP 是安全值（§2.2），但在混部下单个
   hog 占核 6ms，意味着任何一个被换下的前台线程最坏要多等
   16 核 × 若干个 6ms 片；p99 从这里被直接放大。
3. 300k 卡死（§3.1）同样发生在唤醒风暴下，三者共同指向：
   **v2 底座缺 EEVDF 的 lag 携带 + 唤醒放置，这必须作为下一个
   开发项（D5），而不是参数调优。**

修复方向（按优先级）：(a) 唤醒时保留 lag（虚拟截止期随睡眠时长前移），
对齐 EEVDF；(b) 在 lag 放置就位后，把 hog cap 重新收紧到 2–3ms，
让混部下的前台延迟受控；(c) D4 已铺好卡死取证路径，300k 复现放在
lag 放置之后一起验证（很可能同根）。

## 5. N1 外部 CPU 噪声（stress-ng 干扰 + 开环点查）

方法：DB 节点本地起 16 个 stress-ng CPU worker 满核干扰，loadgen
开环点查 10k/50k qps 各 60s；每配置先测无干扰基线（base），再测
干扰期（noise），对比 p99/max。开环下吞吐由目标速率决定，比较的
是尾延迟韧性。

| 配置 | base p99@10k | noise p99@10k | base p99@50k | noise p99@50k | noise max@50k |
|---|---:|---:|---:|---:|---:|
| V97 | 0.13ms | 0.24ms | 0.13ms | 0.58ms | 4.22ms |
| SN | 0.13ms | 0.23ms | 0.13ms | 0.64ms | 2.28ms |
| SG | 0.13ms | **7.17ms** | 0.14ms | **7.84ms** | **381.08ms** |

**SG 不通过 N1**：干净基线与 V97 完全一致（0.13/0.14ms，再次印证
埋点与调度器在无干扰时无损耗）；但在 16 个 stress-ng 满核干扰下，
点查 p99 恶化到 7.2–7.8ms，是 V97（0.24–0.58ms）的 12–30 倍，
max 高达 381ms。SN 与 V97 全档一致（同一内核 EEVDF，埋点零成本）。

与 N2（§4.1）完全同源：外部 hog 挤满所有核后，前台线程每次唤醒
都要排在吃 6ms 长片的 hog 后面，且睡眠不累积 lag、无法前插。
原生 EEVDF 靠 lag 携带把交互线程的 eligible 时间提前，天然抵御
这种干扰；GUARDED 底座没有，**在"负向韧性"这个 v2 的核心设计
目标上，当前实现比原生 EEVDF 差一个数量级**。修复必须落在 D5
（lag 携带 + 唤醒放置），语义权重与参数调优已被 N1/N2 证明无效。

## 6. 已知差距与风险汇总

1. **SG 干净闭环负载 -2.3%（cap 6ms 后）**，p95 约 1.7×V97（rw64
   15.8ms vs 9.4ms）。容量内开环点查无差距，说明差距集中在闭环
   阻塞-唤醒循环的调度延迟，底座缺 EEVDF 的 lag 放置/唤醒 kick。
2. **300k 卡死 open bug**：丢唤醒类，D4 已补取证路径，未定位。
3. SS（`--semantic`）语义路径已在 N2 实测验证**在活跃工作**
   （semantic_hits≈29 万/120s）但对混部指标无改善：语义权重能微调
   队列顺序，弥补不了底座的延迟盲区（§4.1）。`--semantic` 暂不默认
   开启，待 D5（lag 携带）落地后重估其增量。
4. 数据 caveats：当天有 spot 回收换机，跨节点数据以复测为准；
   rwscaling 的 SG 行为 cap2.5ms 旧默认，与 D3 新默认的差异见 §2.2。

## 7. 下一步（按优先级）

1. **D5：底座对齐 EEVDF**——唤醒时 lag 携带（虚拟截止期随睡眠前移）
   + 定向唤醒 kick。这是 N2 失败、rw -2.3%、300k 卡死三个问题的
   公共根因修复项，优先级最高。
2. 定位并修复 300k 丢唤醒卡死（scx tracepoints 取证，D4 已铺路）；
   若确与 lag/唤醒放置同根，随 D5 一起验证。
3. **D6：detach 看门狗**：GUARDED 窗口内自动检测调度器异常
   （loader 统计冻结、bridge 心跳消失）并 detach 回 EEVDF——
   当前 detach 是手动的，卡死场景需要外部干预，达不到"韧性"承诺。
4. D5 落地后重跑 N2/N1/rwscaling 全矩阵，重估 `--semantic` 的量化
   增量，再决定 SS 是否默认开启。
