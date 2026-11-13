# smart_sched：原生公平底座与按需关键路径联合调度

状态：**v2 设计评审稿，尚未实现**（2026-11-13，分支 `smart_sched`）。

运行代码仍是 v1，测试二进制对应 `56c465014`。v2 不构成任何新的性能承诺。

## 1. 目标不变，默认路径改变

保留 one-thread-per-connection，不引入 reactor、协程或业务优先级。目标是：

1. 干净的 sysbench `oltp_read_write`、点查和 TPC-C 相对纯净 9.7（V97）
   持平或更优；单档吞吐门槛为 -2%，不是用其他场景的收益抵扣回退。
2. N1 外部 CPU 噪声、N2 前台事务混合 CPU 长任务、N4 热点锁竞争下，
   不产生吞吐/尾延迟回退，再争取严格改善。
3. 只使用数据库自身的执行事实：实际 COMMIT/ROLLBACK、有等待者的
   latch/事务锁 holder、实际在核执行时间。不按连接、用户或 AP/TP 标签
   分配固定优先级，业务间资源分配交给中间件。
4. 无 sched_ext、loader 退出、map 不兼容或策略无效时，回到原生内核调度，
   不让数据库 correctness 依赖 BPF 调度器。

**核心变化：不再用常驻的自制调度器替换全机 EEVDF。默认使用原生
EEVDF；只有负向证据持续存在时，才短时启用全系统 sched_ext。**

这里的 EEVDF 指 Linux 当前公平调度实现；测试脚本沿用历史名称 CFS。

## 2. v1 验收证据与解释边界

16c DB + 8c loadgen，V97 与 v1 同日交错复测：

| 场景 | V97 | v1 | 结论 |
|---|---:|---:|---|
| rw64，两轮均值 | 11,610 TPS | R0 10,686；S0 11,632 | R0 -8.0%；无 loader 的 bridge 约持平 |
| TPC-C 8WH/64，两轮均值 | 151,309 TpmC | R0 154,378 | raw TpmC +2.0%，非完整合规成绩 |
| N1 noise@50k | p99/max 0.56/2.19ms | R0 FATAL；R1 3.49/14.95ms | 不通过 |
| N2 mixed | 9,425 TPS，p99 16.41ms | R0 6,134，p99 61.08ms | 吞吐 -34.9%，不通过 |
| N4 Pareto 热点，单轮 | 8,098 TPS，p99 42.61ms | R0 8,439，p99 40.37ms | 仅初步信号，语义开关未超过 R0 |

完整原始数据见
`aliyun/mysql-sched-bench/results/smart-20261113/summary.md`。

必须保留以下解释边界：

- rw 的 -8% 尚未被唯一根因解释。BPF ops 约 1.75% CPU、bridge 差值约
  0.7%，不足以单独解释它；上下文切换反而减少，也不是天然收益。
- “缺少唤醒抢占”是合理机制假设，不是完整答案。加 kick 后 rw 仍回退，
  N1 也没有恢复到 V97，说明放置、排序、slice 和调度路径需要一起审查。
- TPC-C 的局部收益不能掩盖 rw 和负向场景失败；N4 的收益来自无语义的
  R0，不能记到 commit、latch/lock holder 或 PI 头上。
- `--slice-shift=1`、`--wake-affine`、原 R1 盲 kick、local-wakeup 和
  mature-kick 都是实测负收益，不应仅换参数重试。

## 3. 为什么不在 v1 上继续堆机制

### 3.1 默认路径替换了过多内核决策

`support-files/scx/scx_db_sched.bpf.c` 接管所有普通任务，却自行近似了
CPU 放置、唤醒、slice、迁移和公平性：

- 当前实现实质是 lag-clamped vtime DSQ，并非完整 EEVDF。默认路径没有
  按 `p->scx.weight` 缩放执行时间，不能完整保留 nice/cgroup 权重和系统
  原有的调度行为。
- `select_cpu_dfl()` 返回的 `is_idle` 被丢弃，唤醒总是进入 per-CPU vtime
  DSQ；已尝试的 waker-CPU 迁移又破坏局部性，不能用单一路径覆盖所有唤醒。
- 内核的 `wakeup_preempt_scx()` 是空操作。手动 `SCX_KICK_PREEMPT` 还包含
  异步 irq-work、IPI、slice 清零和强制 dispatch，成本模型不同于原生唤醒。
- `SCX_ENQ_PREEMPT` 的直接抢占针对内建 local DSQ；内建 local DSQ 又是
  FIFO，不能直接拿来替换当前 vtime DSQ。此前 local-wakeup 实验已经证明，
  绕过公平排序并不是答案。
- 单个共享 `vtime_now` 被多个 CPU 读写但没有原子最大值协议；迁移时又把
  全局虚拟时间放入 CPU 局部队列。它需要正确性验证，不能只按 x86 上
  64-bit store 不撕裂就认为排序正确。

因此，当前 R0 不是一个足够小的变量。先补齐自制通用调度器，再在其上
增加语义，会把内核工程与数据库优化混在一起。

### 3.2 vtime/debt 记账混淆了“累计服务”与“临时排序”

`scx_bpf_dsq_insert_vtime()` 会把传入的顺序值写回 `p->scx.dsq_vtime`。
当前语义路径把 commit/holder bonus 和 debt 都加进这个值，`stopping()`
又在它上面累计运行时间。结果是：

- 临时 bonus 可能留存到窗口结束后的下一次入队；
- debt 已进入 `dsq_vtime`，同时仍保存在 `task_account.debt`，后续可能
  再次偿还；
- PI 在 waiter 入队时直接覆盖 holder floor，而不是保留最急 waiter；
  holder 第一次使用后又删除 floor，不能表示持续的多 waiter 关系；
- `depth` 没有递推，当前实现不是多级优先级继承。

这不影响“语义开关全关”的 R0 结论，但使 S2/S3/S4 无法作为正确的
borrow/repay 或 PI 来验收。

### 3.3 wait-edge 不是一个有所有权的对象

代码审查确认：

- `lock_holder` 实际是布尔标记；任意一个 waiter 结束就删除 holder，
  即使其他 waiter 仍存在。多 waiter 下倾斜会提前消失。
- latch 和事务锁共用 `blocked_by[waiter]`，后发布的边可以覆盖先发布的边。
  `latch_wait_begin()` 的返回值没有被调用方用于精确配对，`latch_wait_end()`
  则可能清除另一条边。
- `holder_tilt` 只在 enqueue 检查。holder 已经占用 CPU 时，新出现的 waiter
  不会使 holder 立即改变调度状态，因而计数为 0 不代表路径已经覆盖。
- `scx_clear_wait_edge()` 在 waiter 的 THD/tid 无法解析时可能退回调用者
  tid；THD 销毁与 pthread/tid 复用时，这不再是可靠的边身份。
- lock edge 发布在 InnoDB 锁分片临界区内执行 userspace mutex、哈希表和
  BPF syscall，可能延长数据库自身的锁关键路径。

所以当前 1 秒 TTL 和周期 sweeper 不能代替精确生命周期。TTL 只能作为
异常退出后的兜底，不能成为正常引用计数的一部分。

### 3.4 控制面与数据面职责混乱

常驻 `rg_prio`、固定 BG 低权重、语义 kick、transitive PI、多个兼容 map、
refcount gate 和 sweeper 同时存在，但多数没有净收益证据。尤其固定降低
后台线程权重不是通用无代价优化：redo/purge 等也可能处在提交关键路径上。

v2 删除这些默认假设。业务 resource-group 不进入调度器；后台线程只按
实际行为及是否阻塞关键路径处理，不预设永久等级。

## 4. v2 架构：native-first、按需介入、失败退回

```text
干净负载（默认）
  connection threads ───────────────> 原生 EEVDF
       │ 仅在真实 OS 等待时更新廉价的线程本地事实
       └──────────────┐
                      v
                控制面观测/触发
                      │ 持续排队或关键路径阻塞证据
                      v
短时联合调度窗口     全系统 sched_ext（不是 partial 优先级捷径）
  phase/edge maps ──> 公平的参考底座 + 有界关键路径加速
                      │
          证据消失、无净收益或异常
                      v
                 detach → 原生 EEVDF
```

三种运行状态足够描述第一版：

1. **NATIVE**：不接管调度。没有每命令 BPF syscall、map 扫描或 gate
   sweeper。控制面只读取已有的低频指标和真实 OS-wait 计数。
2. **GUARDED**：attach 全系统调度器，保留普通任务的公平服务；对实际
   关键路径给予有限加速。先在手工强制模式下证明该状态有收益。
3. **BACKOFF**：证据消失后经过冷却期 detach；加载失败、运行错误、map
   版本不符或探测窗口明确退化时，也退回 NATIVE，并抑制反复切换。

切换是系统级操作，不能每个 SQL 或每次 latch 等待都做。自动模式的候选
窗口、冷却期和退出条件见第 9 节；在手工 GUARDED 未证明收益前，不交付
自动切换。

### 为什么不把 partial SCHED_EXT 当成主方案

`SCX_OPS_SWITCH_PARTIAL` 可让未选择的任务留在 EEVDF，适合后续做局部
实验；但 x86 调度类链接顺序是 RT → fair → ext → idle。也就是说，
`SCHED_EXT` **低于** `SCHED_NORMAL`，不是天然的业务高优先级。仅把
holder 切到 partial sched_ext，既不能保证它压过外部 hogs，也不能保证它
压过仍在 fair 类的其他数据库线程。

因此 v2 不声称“只调度关键线程”就能解决 N1。N1 的 GUARDED 必须管理
外部 hogs 与数据库线程之间的全局竞争。cpuset 隔离是另一种主机资源
方案，不应偷换当前 N1 的测试定义。

## 5. 调度数据面：先忠实，再加速

### 5.1 通用底座

不继续从零发明通用 EEVDF。先选取一个维护中的 sched_ext 公平调度参考
实现作为 **reference**，在相同内核上比较 V97、reference 和裁剪后的自有
实现。参考实现用于校验：

- `p->scx.weight`、cgroup 权重、slice 过期和迁移记账；
- idle CPU 的 direct dispatch 与 busy CPU 的唤醒等待；
- per-CPU/NUMA 放置与负载均衡；
- `running/stopping` 的真实在核时间，而不是把 runnable 等待算作执行。

自有底座只保留参考实现中已经验证的必要部分。idle 且目标队列为空时，
可以走内建 local DSQ；busy 时仍进入公平排序，不能再以 local FIFO 绕过
所有普通任务。唤醒抢占单独验收，不因为“理论上应该更快”默认打开。

### 5.2 把 canonical vruntime 与排序键分开

每个受管任务至少有：

```text
TaskAccount {
  task_cookie;             // tid + 内核 task start_time，用于防止复用误配
  base_vruntime;           // 只累计实际服务，长期公平依据
  run_start_ns;
  borrowed_in_window;
  debt;
  effective_flags;         // 实际获得加速，而非仅发布了语义
}
```

停止运行时：

```text
scaled = actual_on_cpu_ns * reference_weight / p->scx.weight
base_vruntime += scaled

若本次实际获得加速：
    borrowed_in_window += actual_on_cpu_ns（有上限）
否则：
    在任务仍参与 CPU 竞争时，用额外 vruntime 服务偿还 debt
```

入队时只计算临时排序键：

```text
order = base_vruntime + normal_request(weight, slice)
        - bounded_critical_bonus + outstanding_debt_effect
```

canonical `base_vruntime` 不写入临时 bonus。`p->scx.dsq_vtime` 即使被
内核用作 DSQ 排序键，也必须在运行/停止边界恢复为 canonical 值，不能
直接把临时 deadline 当成累计服务继续加算。

debt 的上限和公平窗口是可审计参数，例如窗口 W 内最多借用 B，且 B/W
很小。未运行时不靠“TTL 到期”抹掉账目；睡眠任务不竞争 CPU，不需要
继续加速，醒后仍按未偿还额度降低后续服务。最终公式和 EEVDF eligibility
细节必须同 reference 对照及单元测试确认，不能沿用当前近似即宣布完成。

## 6. 数据库语义：短窗口、真实竞争、可配对

### 6.1 统一 phase，不再复制 coroutine bridge

native 线程模型下，一个 THD 执行期间固定属于一个 OS 线程，不需要
reactor 的 per-job tid 推断，也不需要复制 resource-group 队列逻辑。

保留两类状态，且分开管理：

- **Phase**：`NORMAL`、`COMMIT/ROLLBACK`、`ACTIVE_HOLDER`。用作用域
  cookie 进入/退出，不能由另一个不相干窗口清除。
- **WaitEdge**：`waiter → immediate holder`，只记录实际进入 OS 等待前的
  直接阻塞关系。holder 空闲等客户端下一语句时不获得 CPU 倾斜。

不需要让 reactor 先解析 SQL。`ha_commit_trans()` 等实际执行点已经覆盖
显式、隐式和 autocommit 提交，比在 socket 前猜 COMMIT 更准确。大型
ROLLBACK 也不能无界加速；后续应细分真正释放锁/日志资源的阶段，受
credit 上限约束。

### 6.2 WaitEdge 是可验证身份，不是布尔值

```text
WaitId = { waiter_tid, waiter_cookie, wait_seq }
WaitEdge = { WaitId, holder_tid, holder_cookie, kind, wait_epoch }
HolderState = { live_waiter_count, ... }
```

规则：

1. `wait_seq` 和 kind 使同一线程上的 lock/latch 或嵌套等待不会互相覆盖。
2. 每个 edge 只增加一次 holder 引用；end/cancel/timeout 只按同一个
   `WaitId` 幂等删除一次。两个 waiter 时，任意一个离开不影响另一个。
3. tid 必须配合内核 task `start_time` 或等价 cookie。scheduler 在
   enable/disable 时验证和清理，不能让旧 edge 倾斜复用 tid 的新线程。
4. 无法识别 holder、reader 不唯一、owner 已释放时，跳过该 edge；这是
   可观测的覆盖率损失，不得猜测 holder。
5. 配对清理是正常路径，TTL 只回收异常遗留。测试必须证明 map 最终为空，
  而不是等待 sweeper 后“看起来没问题”。

不要在 InnoDB 锁分片内执行 userspace 分配或 BPF syscall。先在原有锁
保护下取得稳定的 blocker 事实，到锁释放后、真正 OS 等待前再发布；取消、
授锁和销毁路径使用同一身份。 latch owner tid 应随 latch ownership 更新，
避免每次等待再锁一张全局 `std::thread::id → tid` 哈希表。

### 6.3 初版只做直接关键路径

按以下顺序单独验证：

1. **commit**：实际提交/回滚关键段，在 CPU 饱和或有等待者时加速。
2. **direct holder**：确实存在 waiter 的 latch/事务锁 holder。holder
   正在运行时不能“抢占自己”；其后续唤醒才使用加速，必要时给阻挡它的
   普通长任务一个有界调度点。
3. **bounded PI**：只有直接 holder 已证明有效后才考虑。链长、有效期和
   每个 waiter 的 floor 必须可追踪，不能再用最后写入者覆盖和一次删除。

所有 holder 默认没有业务高低之分；倾斜依据是“缩短等待链能释放多少
系统服务能力”。 waiter 已经离开、holder 等客户端输入或 credit 用尽时，
不继续倾斜。

删除默认的 BG 固定权重、RG map、S1–S4 全家桶开关和盲 kick。保留
`feature_mask` 仅用于消融；生产默认值必须由逐项验收决定。

## 7. 抢占是一种有成本的关键路径操作

只在同时满足下列条件时考虑 kick：

- 被唤醒的是**实际获得加速**的队首关键任务；
- 目标 CPU 当前任务已经连续在核超过粒度，且不是另一个不可打断的
  关键路径；
- 有证据证明 holder/waiter 正被该任务阻挡，而不是仅凭旧 lag；
- 每 CPU token bucket 尚有预算，近期 kick 确实换来了更早运行。

`tilted + WAKEUP + budget` 不是完整阻挡判据。还要分别记录：

`request_preempt → kick_sent → critical_running → lock_release`。

若请求发出但 critical task 没更早运行，或释放没有提前，该机制就应
关闭。不能把 IPI 数当作优化成果。holder 已经在核时优先利用自然调度点，
不对其自身 CPU 发无意义 kick。

## 8. 控制面与生命周期

控制面可以常驻，数据面默认不接管。它需要：

- 有版本和 session id 的 pinned map 会话；loader 重载后不能继续写旧 fd。
- 低频探测 reconnect，而不是 mysqld 只在启动时 `init()` 一次。
- 原子地切换到新 session；旧 fd 延迟到所有并发发布者退出后回收。
- loader 心跳失效或 sched_ext 非 enabled 时停止发布；BPF 异常卸载时由
  内核恢复原调度，数据库锁语义不依赖这些 map。
- 有明确 shutdown 的控制线程，不再使用无法 join 的永久 sweeper。

监控优先复用已有 InnoDB OS-wait/lock-wait 统计和内核 schedstat，避免
在 NATIVE 下给每次命令添加两次 clock/map syscall。真实 OS 等待处的少量
线程本地计数可以接受，但其成本必须实测。

候选触发信号是持续的可运行排队延迟、锁/latch 等待及 CPU 饱和的**组合**；
单纯高 TPS 或 CPU 100% 不是负向证据。自动触发前必须先回答：在干净的
rw64 上它会不会频繁误触发？阈值由基线分布决定，不预先编造固定毫秒数。

## 9. 实施顺序：每一步都有可撤销交付物

### D0：诊断参考系，不改变生产默认

- 测试镜像不限定发行版，优先 Debian、Ubuntu、Aliyun Linux 等镜像中的
  高版本内核。EEVDF 从 Linux 6.6 进入主线，但本方案要求实际内核同时
  具备 `CONFIG_SCHED_CLASS_EXT=y`、BTF/BPF JIT 和可用的
  `/sys/kernel/sched_ext/state`；优先 Linux 6.12+，不能仅凭发行版名称或
  用户态 scx 头文件推断内核能力。
- V97、NATIVE、GUARDED 必须在同一镜像、同一内核和相同实例规格上重新
  交错测量。记录 `uname -a`、sched_ext 配置、内核源码/头文件版本及
  DSQ/preempt 语义，不与旧内核的历史数字直接比较。
- 采集 V97/v1 的 wakeup→running、running segment、CPU 迁移、lock wait
  关键路径直方图，不用大规模 `perf record` 代替聚合数据。
- 同内核比较 reference scheduler 与 v1 R0，判断 -8% 更接近通用底座还是
  自有策略；reference 本身不达标也不能被用作成功对照。
- 输出各机制的假设与证伪条件，而不是再以单个 TPS 数字猜根因。

### D1：正确的最小 bridge

- 删除 coroutine/RG 遗留接口，建立 phase/cookie/wait-edge 所有权。
- 把锁分片内的发布移到真正 OS 等待前且内部锁已释放的位置。
- NATIVE 不发布 map，GUARDED 才发布；埋点成本与调度收益分开计量。
- 完成单元测试、真实 InnoDB 集成测试和 detach/reconnect 测试。

D1 本身不是性能优化，不能因为测试通过就打开自动调度。

### D2：最小 GUARDED 与公平底座

- 手工 attach/detach；无 commit/holder/kick。
- 比较 reference、最小自有底座和 V97；检查权重、公平性和迁移。
- 若强制 GUARDED 在干净负载持续差于 -2%，不允许用自动触发掩盖它。
  默认继续 NATIVE，并缩短底座或选择已验证的 reference，而非加机制。

### D3：逐项加入关键路径

严格按 commit → direct holder → 有界 preempt 的顺序；每一步单独 commit、
构建、加载和基准。transitive PI 不在首批交付中。

### D4：受控自动切换

只有手工 GUARDED 在目标负向场景有重复净收益后，才实现自动触发、
冷却和 backoff。记录 attach 原因、持续时间、退出原因和切换期间指标；
高频振荡视为控制面失败。

## 10. 完整验收设计

### 10.1 正确性与公平性先于性能

新增或重建测试，不复用旧二进制作为当前分支通过证据：

- `WITH_SCX_SCHEDULER=OFF/ON` 两种构建；当前源码重编 `sync0rw-t`。
- 同一 holder 两个 waiter，结束顺序正/反；同线程 lock→latch、重复等待、
  unknown owner、多个 reader、授锁/超时/死锁取消、THD 销毁。
- pthread/tid 复用、旧 WaitId 清理、loader 重载、旧 map fd 失效和异常
  detach；所有状态最终清理且无错误倾斜。
- 数学记账测试：bonus 不进入永久 vruntime，debt 只偿还一次，窗口与
  总债务有界；普通任务在长窗口内不会因反复关键窗口永久失去份额。
- 两个等权 CPU burner 的长窗口服务比、不同 nice/cgroup 权重、CPU-bound
  长任务与交互任务的混合公平性。测试时同时记录完成进度，不能只看 FG。
- loader kill/error、sched_ext watchdog 退回后，正在等待和提交的事务
  结果与 V97 一致。

### 10.2 干净负载

- sysbench rw：8、16、32、64、128、256、512、1024、2048；V97、NATIVE、
  强制 GUARDED 同日交错，至少三轮，保留每轮而不是只报最好一次。
- point select：完整开环 rate ladder，报告实际 QPS、p99、max 和
  event queue full；预热时间一致。
- TPC-C：V97/NATIVE/GUARDED 交错复测，保留 raw TpmC 和响应时间约束，
  不冒称完整合规。
- NATIVE 相对 V97 的吞吐差应落在重复测量误差内，目标额外开销 ≤0.5%；
  强制 GUARDED 每档 ≥-2%。每个后续语义特性仍须满足同一门槛。

### 10.3 负向矩阵

先手工 GUARDED 验证机制，再测自动模式的发现时间与稳定性：

| 场景 | 必须报告 | 通过原则 |
|---|---|---|
| N1：16 外部 CPU hogs + 点查 base/noise × 多档 rate | QPS、p99/max、event queue、wakeup 延迟 | 不 FATAL、不崩溃；不差于 V97，改善才可称韧性收益 |
| N2：rw64 + 8 个 BENCHMARK | solo/mixed TPS、p99、BG 完成数、history list | FG 不回退，BG 不饿死；不能只优化一个指标 |
| N4：Pareto h=0.2 热点行锁 | TPS/p99、行锁等待、死锁/回滚细分、holder edge 命中与清除 | 多轮重复改善；区分 R0 底座收益和语义收益 |
| latch 微基准 | OS-wait→release、holder off-CPU/on-CPU 分解 | 直接 holder 确实被加速，且无跨锁误清理 |
| 突发连接/短事务 | 容量、尾延迟、恢复时间、切换次数 | 不崩溃、不振荡；恢复后不残留 debt/edge |

还需测试 burst 后长公平窗口、超核数长 SQL、redo/purge 受限和客户端
慢速保持事务等反面 case。后者说明“持锁者优先”不是无条件总能执行：
holder 等下一业务请求时，操作系统没有可提前完成的工作。

### 10.4 结果判定

性能结论必须给出对应源码 SHA、loader 参数、sched_ext state、原始日志和
至少三轮同日对照。平均值不得隐藏单档失败，p95 不得代替 p99，单轮 N4
不得写成稳定性证明。未运行的新方案测试一律标为“待验证”。

## 11. 明确不做的事

- 不恢复 reactor/协程，不在数据库内增加业务优先级或 resource-group QoS。
- 不用固定 BG 降权、每命令 map 发布、盲 kick 或多层兼容 map 换取表面
  功能完整。
- 不把 `SCHED_EXT` 当成高于 CFS 的优先级，不用 local FIFO 绕过全局公平。
- 不在等待边尚有所有权错误时实现多级 PI，不用 TTL 掩盖引用计数错误。
- 不让自动控制器在未验证 GUARDED 收益前反复接管生产调度。

## 12. 预期与未决风险

v2 的主要收益首先是**风险收敛**：干净负载不再为通用调度器近似付费；
语义只出现在有证据的关键路径；无效时可以退回 V97 行为。它不是已测得
的性能提升。

仍需实测回答：

1. 一个正确的全系统参考调度器在突发窗口内能否同时压住 hogs、保持
   交互任务份额，并避免 v1 的唤醒/排序回退？
2. attach/detach 的迁移成本和控制延迟能否被窗口收益摊销？极短突发只能
   依赖原生 EEVDF，本方案不承诺零检测时间。
3. direct holder 的真实覆盖率是多少？多少 latch 因多 reader、owner
   不可知或已在核而无法受益？
4. commit/holder 加速减少的等待是否大于 map、抢占和 debt 的成本？

若 GUARDED 不能在目标负向场景重复优于 V97，应关闭对应特性，保留
NATIVE，而不是以“已使用 sched_ext”作为成功标准。
