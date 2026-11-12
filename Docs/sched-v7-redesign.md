# scx_db_sched v7 重设计：以 CFS 对等为底线

状态：设计稿（2026-11-12，基于 c5n2 campaign 真实重测数据）
前置阅读：`Docs/todo.md` §5.9-5.10（v6.4 数据作废与重测记录）

## 1. 证据基线（全部为 loader 确认 enabled 的真实数据，redo 4G）

| 场景 | V97 (CFS) | C5 (v6.4) | 差距 |
|---|---|---|---|
| rw t8/16/32 | 3845/6838/9771 tps | 3535/6348/9092 | -7~-8% |
| rw t64/128/256 | 10911/11491/11320 | 9153/9065/8848 | -16~-22% |
| rw t512/1024/2048 | 11105/10459/8828 | 8527/8120/6532 | -22~-26% |
| N1 noise@10k p99/max | 0.26 / 6.31ms | 0.26 / 34.47ms | max 5.5× |
| N1 noise@50k p99/max | 0.58 / 2.72ms | 0.53 / **117.17ms** | max 43× |
| N2 mixed p99 / BG 完成数 | 16.12ms / 176 | 47.47ms / 323 | p99 3×，BG 反多拿 2.3 核 |

对照：v6.3（同样机制族、无 cap）rw256 -13.9%、N1 max 41.6ms —— v6.4 的 cap
与区间记账未修复任何一项，且在 50k 档把尾部放大到 117ms。

## 2. 根因：消融实验后的最终归因

初版分析列出三个嫌疑（全局 DSQ、kick 风暴、CPU-bound cap），E1-E4 消融
实验已全部定量。结论与直觉相反：

### 根因 A（原"全局 DSQ 是 rw 杀手"）：只值 ~3%，且语义任务需要它

E3（语义任务改插 per-CPU 池、去全局 DSQ 消费）：rw256 8848→9128
（+3.2%），但 N1 全面恶化（noise@50k p99 0.53→1.06ms、max 117→142ms）。
全局 DSQ 的单锁竞争是真实的（rw +3.2%），但语义任务失去全局池+kick 的
尾部保护后，噪声场景反而更差。**全局池+kick 是语义任务的一体两面：
成本在 rw，价值在尾部。v7 不能简单删除，需重新设计其保护形态。**

### 根因 B（原"kick 风暴"）：只解释尾部的一小部分

E2（kick 阈值 100µs→3ms + 删 commit/urgent 无条件 kick）：rw256 8601
（无改善），N1@50k max 117→93ms（有改善但仍 34× V97）。kick 率过高
是尾部问题的一部分，但不是 rw -20% 的主因。

### 根因 C（原"CPU-bound cap 引发切换风暴"）：已被 E1 证伪

E1（cap 禁用）：rw256 8506（无改善），N1@10k max 反而 34.5→108ms。
**cap 在兜底尾部**（界定 hog 的最大加塞等待），删掉更糟。v7 保留。

### 根因 D（真正的主体）：公共底座成本 —— E4 + C5n 的关键发现

E4（sem gate 永不命中，语义机制全部惰性化，一切走 fast path；
mysqld 侧发布不动）：rw256 **9158**（仅 +3.5% vs 原版，仍 **-19.0%**
vs V97）。N1 noise@50k p99 0.50ms（甚至略优于原版 0.53）。

C5n（同一 mysqld 二进制、one-thread-per-connection、不挂 loader =
CFS + 埋点）：rw256 **10995**（仅 -2.9% vs V97）。

**rw256 三方分解**（redo 4G，V97=11320）：

| 层 | tps | 该层代价 |
|---|---|---|
| V97（上游 9.7，CFS） | 11320 | — |
| + mysqld 埋点（C5n，CFS） | 10995 | **-2.9%** |
| + scx 基础路径（E4，语义惰性化） | 9158 | **-16.7%**（vs C5n） |
| + 语义机制全开（v6.4 原版） | 8848 | **-3.4%**（vs E4） |

三大嫌疑机制（全局 DSQ + kick 族 + tilt + cap）合计只贡献 ~3%；
mysqld 埋点 ~3%；**scx 基础路径一项占 -16.7%，是绝对主体**。注意
E4 的 fast path 仍是 v6.4 的 fast path（含 100µs kick 阈值与
select_cpu_dfl），所以这 -16.7% = scx 平台开销 + v6.4 fast path
策略与 CFS 的差距，两者的切分留给 R0：

1. **scx 平台固定开销**：BPF 调度器每次 enqueue/dispatch 的固定成本、
   verifier 约束下的直通性损失。
2. **策略差距**：`scx_bpf_dsq_insert_vtime` 的 vtime 池是 CFS per-CPU
   rbtree + wake_affine 的近似——CFS 的 wake_affine/wake_wide 会把
   唤醒者与被唤醒者放同核利用缓存，v6.4 的 select_cpu_dfl 没有；
   100µs kick 阈值远低于 CFS 的 granularity（~1-3ms），制造了 CFS
   不会有的 IPI 与 slice 清零。

**诚实的结论**：至 E4 为止，没有任何一个场景显示语义机制有实测正收益
（当真正运行时）。v7 的第一优先级是修平根因 D 的底座差距，语义机制
在 R2/R3 按验收门逐一重审，不过门即删。若 R0 的干净 EEVDF 仍无法
把 scx 基础路径压到 -5% 以内，则启动条件式调度备选（§7.6）。

### N2 的教训：CFS 的"交互性优待"就是通用短事务倾斜

V97 下 8 个 hogs 只拿到 2.9 核（低于公平份额），频繁休眠的事务线程
天然获得优先。这不是业务标注，是**从睡眠行为推断的通用信号**。
EEVDF 的 lag/eligibility 原生提供同样的性质（睡得久的任务 lag 为负、
更早 eligible）。v6.4 的问题是 gated 任务绕过了 per-CPU vtime 池的这
个性质（进了全局 DSQ，且被 kick/debt 机制干扰）。
**结论：N2 要的不是新机制，是正确的 EEVDF。**

## 3. v7 设计原则

- **P1 对等优先（Parity first）**：任何语义机制上线的前提是干净负载
  代价 < 2%。默认路径必须是忠实的 EEVDF：per-CPU vtime DSQ、lag clamp、
  队列深度自适应 slice。语义只允许"倾斜 vtime"，不允许"改道到别的队列"。
- **P2 语义是常态**：语义路径的性能预算 = fast path 性能预算。每次唤醒
  最多一次 map 查找；语义状态折叠为单一 per-tid 结构。
- **P3 抢占是预算**：kick 必须同时满足（a）粒度过滤（lag 领先超过
  granularity，CFS 量级 ~1ms，而非 100µs）；（b）全局 token bucket
  预算（如 ≤2k IPI/s/CPU），超限退化为"入队等待 slice 边界"。
  无条件 per-event kick 一律禁止。
- **P4 删除优先于添加**：每个机制必须有独立开关和计数器，连续两个
  验收周期无正收益的机制删除。
- **P5 测量门**：沿 §5 的阶段门推进，不过门不进下一阶段。

## 4. v7 架构

### 4.1 队列与状态

- **唯一队列族：per-CPU vtime DSQ**（沿用 v6.4 的 CPU_DSQ_BASE+i，
  `scx_bpf_dsq_insert_vtime`）。**删除全局 EEVDF_DSQ**。gated 任务与
  ungated 任务进同一个 per-CPU 池，区别只是 vd 偏移量。
- `sem_state[tid]`（单 map，取代 sem_any+commit_vip+task_tier+
  lock_holder 四次查找）：
  ```
  struct sem_state {
    u32 flags;        /* SEM_COMMIT | SEM_HOLD | SEM_WAIT */
    u32 waiter_edge;  /* blocked_by 语义：我等的 holder tid（0=无） */
    u64 ts_ns;        /* 发布时间，TTL 失效 */
  };
  ```
  mysqld bridge 侧合并写。未命中 = 纯 normal 任务（kernel 线程、外部
  进程、未发布窗口的 DB 线程）→ 纯 fast path，零额外查找。
- `holder_boost[holder_tid] = {floor, ts}`（PI floor，保留，edge 驱动，
  深度上限降为 3；v6.4 实测 PI 深度从未超过 1——InnoDB 的 latch 链短）。
- `task_account`（borrow/repay + slice_ns 记账 + cpu_budget）保留——
  E1 证实 cpu_budget cap 在兜底 hog 加塞等待，v7 原样保留该机制。

### 4.2 enqueue（唯一路径，不再分 fast/slow 两套）

```
1. sem = lookup(sem_state, tid)            ← 唯一一次 map 查找
2. vr  = clamp(p->scx.dsq_vtime, vtime_now - MAX_LAG)
3. vd  = vr + slice_of(weight)             ← BG(tier) 权重唯一保留场景
4. if (sem) {
     if (sem->flags & SEM_COMMIT && borrowed < B_MAX) vd -= B_COMMIT;
     if (sem->flags & SEM_HOLD  && borrowed < B_MAX) vd -= B_HOLD;
     if (sem->flags & SEM_WAIT) publish PI floor 给 waiter_edge;  // 写侧
   }
5. vd += st->debt;                         ← borrow/repay 偿还
6. slice = fast_slice(cpu)                 ← 队列深度自适应 + CPU-bound cap（保留）
7. insert_vtime(p, cpu_dsq(cpu), slice, vd)
8. wakeup-preempt 判定（见 4.3）
```

每次唤醒成本：1 次 hash 查找 + 1 次 vtime clamp + 1 次 DSQ insert
（per-CPU 锁）。与 v6.4 fast path 同量级，语义任务不再更贵。

### 4.3 唤醒抢占（预算制）

```
preempt := (enq & SCX_ENQ_WAKEUP)
        && lag > WAKE_GRAN_NS            /* 1ms，CFS 量级，可调 */
        && kick_budget_allow(cpu)        /* token bucket: 2k/s per cpu */
        && (sem 语义紧急                 /* SEM_COMMIT 或 SEM_HOLD 有等待者 */
            || lag > 2*WAKE_GRAN_NS)     /* 睡了很久的任务，CFS 也会抢 */
```
- 粒度过滤把 IPI 率从"每唤醒"降到"显著睡眠后唤醒"；
- 预算兜底极端唤醒风暴；
- 语义紧急仅限 commit / 有等待者的 latch 持有——且受同一预算约束
  （commit 不再有专属无条件 kick）。
- 若内核 7.0 的 `SCX_ENQ_PREEMPT` 对 `SCX_DSQ_LOCAL_ON` 插入生效
  （待验证），优先用内核原生路径替代手动 kick。

### 4.4 dispatch / stopping

- dispatch 沿用 v6.4：local 池 → 邻居 steal（轮转）。无全局 DSQ 可消费。
- stopping 沿用 v6.4 区间记账（running 记 start_ns、stopping 记差值），
  v6.4 的记账修复方向是对的；但需 bpftrace 验证 task_account 命中率
  （v6.4 的 117ms 尾部也可能是 account miss 走了 fallback 分支）。
- borrow/repay：tilt 期间 borrowed 累计，超 B_MAX 关窗结算为 debt，
  后续 vd += debt 偿还——即"短时多拿、周期内还回"（cgroup cpu burst
  思想），语义倾斜天然有界。

### 4.5 删除清单

| 删除 | 理由 |
|---|---|
| 全局 EEVDF_DSQ | 根因 A：16 核单锁竞争 |
| commit 无条件 kick | 根因 B：9k IPI/s |
| fast path 100µs kick 阈值 | 根因 B：每唤醒一 IPI → 1ms 粒度 + 预算 |
| TIER_TS（reactor/VIP 分类） | C5 架构下 reactor 已不存在；业务优先级归中间件 |
| rg_prio map | 早已忽略，清 ABI |

### 4.6 保留清单（有证据或有原则）

| 保留 | 理由 |
|---|---|
| per-CPU vtime DSQ + lag clamp | EEVDF 核心；N2 教训：这就是通用短事务倾斜 |
| fast_slice 队列深度自适应 + CPU-bound cap | E1 证实 cap 在兜底尾部（N1@10k 34.5→108ms 当禁用）；饱和粒度收敛与 hog 加塞界定的双重机制 |
| 唤醒 preempt kick（语义+fast 共用） | E1 前后 N1 p99 均 0.26-0.28ms（与 V97 持平），kick 机制本身有效；需粒度过滤+预算化（E2 验证形态） |
| commit tilt（vd -= B_COMMIT，有界） | 设计目标 1：commit 优先，但不再 per-event kick |
| latch holder tilt + PI floor | 设计目标 2：临界区优先离场；depth 3 封顶 |
| BG（purge 等 DB 内部线程）低权重 | DB 自有后台线程是数据库内部知识，非业务 QoS |
| borrow/repay | cgroup cpu burst 思想，用户明确要求 |

## 5. 阶段验收门

| 阶段 | 内容 | 干净负载门 | 负向门 |
|---|---|---|---|
| R0 | 裸 EEVDF（无任何语义 tilt，sem_state 查找但忽略） | rw8-2048 每档 ≥-2%；TPC-C ≥-2%；点查 parity | N1 max ≤8ms@10k、≤4ms@50k；N2 FG p99 ≤1.1× V97 |
| R1 | + 唤醒抢占（粒度+预算） | 同上不回退 | N1 max ≤3ms@50k（CFS 对等 2.72ms） |
| R2 | + commit tilt | rw ≥-2%；TPC-C p99 改善为期望信号 | N1/N2 不回退 |
| R3 | + latch tilt + PI | rw/TPC-C ≥-2% | N2 latch 密集场景 FG p99 改善为期望信号 |

每个机制一个 `const volatile` 运行时开关（loader 参数），同一二进制
可 A/B；每机制计数器（tilt 命中、kick 发出、预算拒绝、PI 深度分布）
经 bpftool 可读，归因不依赖 perf。

**不过门的处理**：R0 不过 → 修 EEVDF 实现本身（不许加机制）；修平后
仍不过（底座代价压不进门内）→ 触发 §7.6 条件式调度备选，v7 形态改为
"平时 CFS + 威胁时 scx"；R2/R3
不过 → 删对应 tilt，v7 以 R1 形态发布。**宁可发布一个"纯 EEVDF 对等"
的空壳，也不发布任何"负优化换概念"的版本**——这是本次重设计的第一
纪律，来自 v6.4 的教训（两次"修复"在从未运行的情况下被宣布成功）。

## 6. 归因实验（进行中）

| 实验 | 变量 | 假设 | 结果 |
|---|---|---|---|
| E1 | SLICE_CAP 禁用（其余不变） | cap 贡献 rw -20% 的大部分与 N1 尾部 | **证伪**：rw256 8506（带 cap 8848，V97 11320）无改善；N1@10k max 反而 34.5→108ms。cap 在兜底尾部，rw 损失另有主因 |
| E2 | WAKE_PREEMPT 100µs→3ms + commit/urgent kick 删除 | kick 风暴贡献 rw 损失的大部 | **部分证伪**：rw256 8601 无改善；N1@50k max 117→93ms 有改善但仍 34× V97。kick 只解释尾部的一小部分 |
| E3 | gated 任务改插 per-CPU 池 + 去全局 DSQ 消费（= v7 核心原型） | 全局 DSQ 锁竞争是 rw -20% 主因 | **大部分证伪**：rw256 9128（+3.2% vs 原版，仍 -19.3% vs V97），p95 反而恶化（66.8→112.7ms）；N1 全面恶化（noise@50k p99 1.06ms、max 142ms）——语义任务失去全局池+kick 的尾部保护 |
| E4 | sem gate 永不命中：一切走 fast path（语义机制全部惰性化，mysqld 侧发布不动） | -20% 的主体在 scx 基础路径（fast path 本身 + mysqld 埋点），不在语义机制 | **证实**：rw256 9158（+3.5% vs 原版，仍 -19.0% vs V97）；N1@50k p99 0.50ms 略优于原版——语义机制无实测正收益 |
| C5n | 同 mysqld 二进制、one-thread-per-connection、不挂 loader（= CFS + 埋点） | 切分 mysqld 埋点与 scx 平台各自的占比 | **10995（-2.9% vs V97）**：埋点 ~3%，scx 基础路径 -16.7% 为绝对主体（见 §2 根因 D 分解表） |

**E1-E4 + C5n 小结**：v6.4 的三大嫌疑机制（cap、kick、全局 DSQ）合计
只解释约 3% 的 rw 差距，且各自有反向作用（cap 兜底尾部、kick+全局池
保护语义任务延迟）；mysqld 埋点 ~3%；**scx 基础路径（平台开销 +
fast path 策略差距）以 -16.7% 占绝对主体**。v7 的战场在底座，不在
语义机制。

## 7. 风险与开放问题

1. **per-CPU 池的语义 tilt 跨 CPU 失真**：vd 偏移在任务迁移后与目标
   CPU 的 vtime 基准比较——MAX_LAG clamp 兜底，但 R2/R3 需专门测迁移
   密集场景（rw t1024+）。
2. **SCX_ENQ_PREEMPT × SCX_DSQ_LOCAL_ON 在 7.0 内核的行为**待验证；
   不行则保留手动 kick（仍受预算约束）。
3. **117ms 尾部的精确链路**：R0 落地后若 N1 尾部仍异常，优先 bpftrace
   task_account 命中率与 sem_any stale gate（tid 复用）审计。
4. **语义发布的 mysqld 侧成本**：bridge 合并为单 map 写入（sem_state）
   需要同步改 mysqld 侧发布代码，注意与 v6.4 ABI 的兼容期。
5. 调度器外的问题不许用调度器修：本 campaign 已确认 redo 容量、
    max_connections 等配置会制造假的"调度器回归"（§5.10 事故 2）。
6. **战略备选：条件式调度（conditional scheduling）**。C5n 分解显示
   scx 基础路径在干净饱和负载下代价 -16.7%。若 R0 的最小 EEVDF
   （修掉 100µs kick、补 wake-affine、最简 dispatch）经过逐项优化
   仍无法把该代价压到 -5% 以内，则 v7 转向条件式形态：平时 CFS
   （零代价），由 loader 通过信号/计数器监测威胁指标（运行队列
   深度、唤醒延迟、noise 检测），仅在负向场景 attach scx 调度器，
   威胁解除后 detach。sched_ext 支持运行时 attach/detach，切换
   代价为一次全队列迁移（毫秒级），对 N1/N2 类瞬态场景可接受；
   对持续性混合负载（N2 常驻 BG）则需滞回窗口避免抖动。R0 是
   这个决策的实验门：**先测最小 EEVDF 的底座代价，再决定 v7 是
   常驻 scx 还是条件式 scx**。

## 8. 实施计划

1. **v7-rc1（R0）**：重写 `scx_db_sched.bpf.c`（预计 ~500 行，比 v6.4
   减 1/3）：删三件套、单路径 enqueue、开关框架 + 计数器。mysqld 侧
   bridge 暂不改（sem_state 用兼容读：sem_any 仍作为 gate，四 map 读取
   只在 gate 命中时做——R0 忽略语义所以零成本）。
2. R0 过门后：bridge 合并 sem_state（mysqld 侧小改），启用 R2/R3 tilt。
3. **埋点成本消融（独立任务）**：C5n 显示 mysqld 侧发布代码在 CFS 下
   就有 -2.9% 代价。先测"完全去除发布"的形态拿到上限，再决定 v7 发布
   策略：条件发布（仅 latch 真争用/有等待者时写 map）、批量合并、或
   R1 以下形态默认关闭发布（loader 参数控制）。
4. 测试协议沿用 c5n2：rw8-2048（redo 4G、MAX_CONN 3000、低并发 3 轮）、
   N1/N2/N3 矩阵、TPC-C；所有脚本已有 enabled 硬断言。
