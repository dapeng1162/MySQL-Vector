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

### 根因 E（v7-rc1 当日发现）：CPU-bound 检测器从 v6.4 起就是死代码

v6.4 与 v7-rc1 首版的 cpu_budget 记账：stop 时先按 `now -
last_decay_ns`（**含自身运行时间**）decay、再加本段 rt。由于 rt ≤
gap 恒成立，budget 永不增长——hog cap 从未真正生效（v7-rc1 冒烟
测试中 84 次触发只是首跑 20ms grant 加计时抖动的边缘命中）。证据链：
N1 noise@10k 无 kick 时 avg **5.2ms** ≈ 10ms grant（fast_slice 深度
2-3 档）的一半——每次唤醒平均等半个 hog grant；p99 11.45 ≈ 一个
grant；cap 2.5ms→1ms 的 knob 实验**完全无效**（p99 11.24→11.45）
因为 cap 从未生效。**E1 的"cap 禁用无变化"也因此需要重新解读**：
它禁用的是死代码，其 N1 恶化（34.5→108ms）另有来源（可能是实验
补丁的副作用）。修复：decay 移到 running()，按纯离核间隙
（last_stop→本次 start）衰减，累积留在 stopping()。

修复后实测（3cde7bd33，默认 2.5ms cap）：**rw t256 10291（vs 修复前
10334，-0.4%，漂移噪声内）、t64 10078（-2.8%）**——cap 在 rw 上
几乎免费；**N1 noise@10k avg 5.20→1.43ms、p99 11.45→3.02ms**
（V97 0.26）。noise@50k 仍 FATAL：点查 3-4 个 sleep/wake 周期 ×
每周期 cap 等待（~2.5ms/2）在 50k/s 下需 70+ in-flight > 64 线程；
CFS 靠原地唤醒抢占实现亚毫秒 per-cycle 等待，slice cap 摊不平这个
差距——50k 档需要 kick 兜底（-3%~-9% rw 代价）或 R2+ 语义机制，
列为 v7 开放项（§7.7）。

### 根因 D（E4 的修正版）：fast-path kick 才是 rw 杀手，scx 底座本身 ≈ -6%

E4 曾把 -16.7% 记在"scx 基础路径"上。v7-rc1（R0：单一 per-CPU vtime 路径，
语义 gate 只计数、完全无 kick）的实测推翻了这个归因：

**rw256 修正后的分解**（redo 4G，V97=11320）：

| 层 | tps | 相对上一层 |
|---|---|---|
| V97（上游 9.7，CFS） | 11320 | — |
| + mysqld 埋点（C5n，CFS） | 10995 | -2.9% |
| + scx 最小 EEVDF、无 kick（**R0**） | **10334** | **-6.0%**（-8.7% vs V97） |
| + 预算制唤醒 kick 1ms/2048每CPU秒（R1） | 10025 | -3.0% |
| v6.4 fast path（含 100µs kick，语义惰性，= E4） | 9158 | **-11.4% vs R0** |
| + 语义机制全开（v6.4 原版） | 8848 | -3.4% |

R0 与 E4 的机制几乎相同，唯一实质差异是去掉了 100µs 阈值的 fast-path
kick——差值 +12.9%。**v6.4 注释中"几乎每次唤醒都抢占才是缓存高效"
（去抢占 -8.5%）的结论在当前配置下不成立**：当自适应 slice 与区间记账
正确工作后，队列深度本身就提供了及时的调度点，kick 的 IPI + slice
清零 + 强制 dispatch 成为纯开销。E2（3ms 阈值）没测出这点，因为 rw
的 IO 等待都在 ms 级、lag 照样超过 3ms，kick 率几乎没降。

连"预算制 kick"（R1，CFS 量级粒度 + 2048 IPI/s/CPU 上限）也要价
-3.0%——SCX_KICK_PREEMPT 的代价比 CFS 原地唤醒抢占检查（无 IPI、
不强制 dispatch）高得多。kick 从"默认机制"降级为"尾部兜底"，
只在无 kick 的尾部实测不合格时按预算启用。

剩余的 -6.0%（R0 vs C5n）是真正的 scx 平台差距，候选：BPF
enqueue/dispatch 每次切换的固定开销、select_cpu_dfl 对 CFS
wake_affine/wake_wide 的缺失（唤醒者与被唤醒者不同核、缓存冷）、
vtime 池对 CFS rbtree 的近似。这 -6% 是 R0 后续优化的对象。

### N2 的教训：CFS 的"交互性优待"就是通用短事务倾斜

V97 下 8 个 hogs 只拿到 2.9 核（低于公平份额），频繁休眠的事务线程
天然获得优先。这不是业务标注，是**从睡眠行为推断的通用信号**。
EEVDF 的 lag/eligibility 原生提供同样的性质（睡得久的任务 lag 为负、
更早 eligible）。v6.4 的问题是 gated 任务绕过了 per-CPU vtime 池的这
个性质（进了全局 DSQ，且被 kick/debt 机制干扰）。
**v7-rc1 实测补充了下半句：池首位置（lag 排序）本身不够——FG 任务
醒来后在池首等 running 任务让出 CPU，无 kick 时这个等待是（半）个
slice；CFS 的原地唤醒抢占把这步压到亚毫秒。** N2 实测（R0+capfix）：
mixed p99 59.99ms vs V97 16.12ms、BG 完成数 330 vs 176——公平
EEVDF 把份额给了 BG，交互倾斜必须靠唤醒抢占或 R2/R3 语义倾斜
补齐（§7.7）。

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
| fast_slice 队列深度自适应 + CPU-bound cap | 自适应 slice 是饱和粒度收敛；cap 的 E1 "无害/兜底"结论作废（当时是死代码，见根因 E），修复后的真实代价待 v7-rc1 复测 |
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
| E1 | SLICE_CAP 禁用（其余不变） | cap 贡献 rw -20% 的大部分与 N1 尾部 | **结果无效**：cap 当时是死代码（根因 E），禁用死代码当然无变化；其 N1 恶化（34.5→108ms）来源存疑，很可能是实验补丁副作用 |
| E2 | WAKE_PREEMPT 100µs→3ms + commit/urgent kick 删除 | kick 风暴贡献 rw 损失的大部 | **部分证伪**：rw256 8601 无改善；N1@50k max 117→93ms 有改善但仍 34× V97。kick 只解释尾部的一小部分 |
| E3 | gated 任务改插 per-CPU 池 + 去全局 DSQ 消费（= v7 核心原型） | 全局 DSQ 锁竞争是 rw -20% 主因 | **大部分证伪**：rw256 9128（+3.2% vs 原版，仍 -19.3% vs V97），p95 反而恶化（66.8→112.7ms）；N1 全面恶化（noise@50k p99 1.06ms、max 142ms）——语义任务失去全局池+kick 的尾部保护 |
| E4 | sem gate 永不命中：一切走 fast path（语义机制全部惰性化，mysqld 侧发布不动） | -20% 的主体在 scx 基础路径（fast path 本身 + mysqld 埋点），不在语义机制 | 部分证实但归因有误：rw256 9158 中约 11pp 实为 100µs kick 的代价（见 R0 行），语义机制本身 ~3.4% |
| C5n | 同 mysqld 二进制、one-thread-per-connection、不挂 loader（= CFS + 埋点） | 切分 mysqld 埋点与 scx 平台各自的占比 | **10995（-2.9% vs V97）**：埋点 ~3% |
| R0 | v7-rc1：单一 per-CPU vtime 路径 + 语义 gate 只计数 + **完全无 kick** | 最小 EEVDF 的真实底座代价；顺带检验 E4 归因 | **t256 10334（-8.7% vs V97，-6.0% vs C5n）、t64 10365（-5.0%）**——比 E4 高 12.9%，kick 才是 rw 主体 |
| R1 | R0 + 预算制唤醒 kick（1ms 粒度 + 2048 IPI/s/CPU） | CFS 形态的 kick 是否免费 | **t256 10025、t64 10038（-3.0% vs R0）**——预算制 kick 仍有 3% 代价 |
| V97 重测 | 同日 10:22 漂移对照 | 日内顺序对比的漂移幅度 | **t256 10871（早晨 11320，-4.0%）**——同日对比有 ±4% 漂移，R0 按同时段算 -4.9% |
| R1 激进 | gran=100µs + budget=8192 | 激进 kick 在干净记账下是否仍然昂贵 | **t256 9372（-9.3% vs R0）**——kick 代价随剂量线性，记账干净也救不了；kick 单次成本远高于 CFS 原地抢占 |
| R0 cap1ms | hog cap 2.5ms→1ms（无 kick） | 无 IPI 路线：收紧 cap 缩短每周期等待 | t256 9925（-4.0% vs R0，死 cap 期测得）；修复后 1ms cap：noise@10k avg 1.05 但 p99 反升至 6.55（方差/切换扰动），50k 仍 FATAL |
| R0fix | cpu_budget 死代码修复（3cde7bd33）后默认 2.5ms cap | 修复后 cap 的 rw 代价 + N1 收益 | **rw t256 10291（-0.4% vs 修复前，漂移噪声内）、t64 10078（-2.8%）；N1 noise@10k avg 5.20→1.43、p99 11.45→3.02、V97 为 0.26；noise@50k 仍 FATAL** |

**E1-E4 + C5n + R0/R1 小结**：v6.4 的 rw -21.8% 最终分解为——100µs
fast-path kick 约 **11pp**（E4 vs R0），mysqld 埋点 ~3pp，语义机制
~3pp，scx 平台真实底座 **~6pp**。kick 从根因 B 的"部分证伪"升级为
**确认主犯**；语义机制维持"无实测正收益"结论。

**kick 剂量-代价曲线**（rw t256，R0=10334 基线）：无 kick 10334 →
gran=1ms/2048 每 CPU 秒 10025（-3.0%）→ gran=100µs/8192 9372
（-9.3%）。SCX_KICK_PREEMPT（IPI + slice 清零 + forced dispatch）
的单次成本远高于 CFS 原地唤醒抢占（need_resched），记账修复与预算
上限都改变不了这一点。**v7 的 kick 定位从"R1 默认机制"降级为
"N1 尾部不合格时的兜底开关"**；无 kick 路线下的替代是收紧 hog
cap（2.5ms→1ms 代价 -4.0%，效果待 N1 验证）。

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
6. **战略备选：条件式调度（conditional scheduling）**。R0 实测后此
   备选的必要性大幅下降：scx 真实底座代价 ≈ -6%（vs C5n），而非 E4
   误估的 -16.7%。优先尝试的底座优化（按序）：select_cpu 补
   wake-affine（唤醒者同核放置）、base slice 缩到 CFS 量级（20ms→
   6-10ms）、dispatch 路径减负。若这些逐项尝试后仍无法压进 -3%，
   再考虑条件式形态：平时 CFS（零代价），由 loader 监测威胁指标
   （队列深度、唤醒延迟），仅在负向场景 attach scx，解除后 detach。
   sched_ext 支持运行时 attach/detach，切换代价为毫秒级全队列迁移。
7. **N1@50k 与 N2 的残余差距，粒度倒置问题**：R1 kick 的 1ms 粒度
   过滤在短睡眠高吞吐场景（N2 mixed、N1@50k）挡掉了本该发生的 kick
   （查询周期睡眠 0.2~1ms < 粒度），而降到 100µs 则 rw -9.3%。
   CFS 无此矛盾（原地抢占每事件近零成本）。scx 侧候选：
   （a）gran=100µs + budget=2048 组合（粒度放宽、预算收紧、先到先得）
   ——明日首试；（b）R2 语义 kick：仅在池首任务被 hog/BG 阻挡时才
   花这个 IPI（语义知识买回预算效率）；（c）接受为 rc1 已知边界。
   N2 实测（R0+capfix）：mixed 6127/p99 59.99/BG 330 vs V97
   9109/16.12/176——BG 拿了 1.9× V97 份额，纯公平 EEVDF 无交互
   倾斜；R1 kick 仅边际改善（6514/56.84/295）。

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
