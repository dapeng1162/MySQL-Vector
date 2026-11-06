# 韧性优先的数据库高并发调度：基于 sched_ext 的公平语义协同

**Resilience-First Scheduling for High-Concurrency Databases: Fair Semantic Co-Design with sched_ext**

> 论文形态：SIGMOD 风格研究论文（设计 + 原型初步结果）
> 代码与数据：`Docs/coroutine-sched-design.md`、`Docs/coroutine-sched-resilience-design.md`、`scripts/bench/sched/`
> 结果声明：§6 中标注【实测】的数字来自本仓库 v1–v3 原型的历史测量；标注【计划】的实验尚未执行，不构成结果声明。

---

## 摘要

通用操作系统的 CPU 调度器以线程公平份额为目标，而数据库的并发需求由事务依赖、锁临界区与提交路径等结构语义决定。两者之间的语义鸿沟在混合负载、外部干扰与突发流量下表现为尾延迟放大、优先级反转与过载崖点。既有工作（如 UFS）通过要求用户显式划分时间敏感与后台两层来缓解该问题，但在业务高频变化、AI Agent 动态生成负载的现实下，人工优先级标注既不可持续也不可行。

本文提出**韧性优先（resilience-first）**的调度目标：优化对象不是单场景峰值吞吐，而是服务级目标下的有效吞吐（goodput）、尾延迟、过载崖点位置与恢复时间。在此目标下，本文设计 EEVDF-Sem——一个以 EEVDF 公平思想为基座、通过 sched_ext 实现的单池调度器。它不要求任何业务标注，仅施加两类普适的结构语义微调：（1）**提交早行**，对 COMMIT/ROLLBACK 施加有界虚拟截止期奖励；（2）**锁持有者条件性优先级继承**，仅在等待边出现时让持有者继承最急等待者的虚拟截止期，继承深度与时间均有硬上限。配合以排队时间为信号的准入控制与突发吸收机制，系统在过载时退化而非崩溃。

基于 MySQL 9.7 的分阶段原型（前台协程 reactor + 固定执行池 + sched_ext 桥接）的初步测量表明：在外部 CPU 噪声下，语义发布使点查 p99 劣化从 7.6 倍降至 1.4–1.7 倍；热点更新负载吞吐提升 66%；代价是干净负载约 5% 的吞吐开销。完整评估计划见 §6.7。

**关键词**：数据库调度；sched_ext；优先级反转；尾延迟；过载控制；性能韧性

---

## 1 引言

数据库管理系统（DBMS）的并发控制决定了事务间的逻辑顺序，但“可运行的事务如何获得 CPU” traditionally 被交给操作系统。这一分工在专用硬件与平稳负载的时代运转良好，如今却在三方面同时失效：

**第一，负载形态变了。** AI Agent 成为主要服务调用方后，数据库访问呈现强烈的潮汐与脉冲特征：一次任务拆解扇出数十个并行子查询，超时即程序性重试，重试过载互为正反馈。此类负载由程序动态生成，既无法预测，也无法人工标注优先级。

**第二，优化的目标函数错了。** 工业界的基准竞赛历史表明，针对特定场景的调优往往对客户日常负载收益甚微甚至为负，却把复杂度转嫁给用户 [24]。当业务以周为单位调整时，任何“为某负载特调”的参数都必然过期。真正需要的不是最好情况下的峰值，而是**最坏情况下的可预期性**：突发不崩溃、越崖不雪崩、噪声下尾延迟有界。

**第三，瓶颈的位置变了。** 随着锁与缓冲池优化日趋成熟，非业务开销的重心上移至通信、上下文切换与调度协同：近期全栈剖析显示简单事务 52%–67% 的运行时消耗在消息传递上 [28]；本仓库原型亦观测到，队列为空时仍有 33% 的执行任务因 worker 被抢占或迁移而等待 100–500µs。这意味着仅在数据库内部优化已不足够，必须打通数据库语义与内核调度。

**本文的出发点**是：调度器应消费**结构语义**而非**业务标签**。有些调度事实无需任何人告知系统——COMMIT 是 2PL 锁的批量释放点；锁/latch 持有者被抢占会连带放大所有等待者的延迟；等待时长本身就是紧迫度信号。这些事实普适、可自动获得、与业务无关。

据此，本文提出 **EEVDF-Sem**，一个以 EEVDF 虚拟时间公平性为基座、经 sched_ext/eBPF 实现的调度器，并围绕它构建完整的数据库韧性栈。本文的贡献如下：

1. **韧性优先的目标形式化**（§2.3）：以 goodput 崖点、越崖行为、恢复时间与噪声下尾延迟劣化倍数为第一优化对象，并给出可证伪的验收口径。
2. **单池公平基座上的两类结构语义微调**（§4）：COMMIT/ROLLBACK 的有界虚拟截止期奖励，与锁持有者沿等待边的有界虚拟截止期继承。两者均满足条件性、临时性、有界性，且对无标注业务零配置。
3. **以排队时间为信号的数据库准入控制**（§4.4）：区别于以线程数或队列深度为信号的做法，本设计直接观测包含 OS 调度等待的排队时间，配合公平排队与重试放大防护，将崖点推远并保证越崖后退化而非崩溃。
4. **分阶段原型与初步证据**（§5–§6）：MySQL 9.7 上的协程前台 + 固定执行池 + sched_ext 原型实测显示，语义发布在外部噪声下将 p99 劣化从 7.6× 降至 1.4–1.7×，热点更新吞吐提升 66%，干净负载代价约 5%。

## 2 背景与动机

### 2.1 两层调度的语义鸿沟

EEVDF/CFS 将线程视为可互换的 CPU 消费者，按权重分配虚拟时间；数据库则掌握事务状态、锁等待图、提交路径与后台角色。信息的不对称是双向的：数据库的优先级队列无法保证被唤醒的 worker 立即获得 CPU；内核无法从一个普通 tid 推断它正持有数十个事务所等待的锁。Practical DB-OS Co-Design [27] 将此类问题归因于通用 POSIX 接口缺乏数据库定制支持；UFS [5] 进一步证明，即使配置 cgroup 权重，EEVDF 的唤醒放置启发式仍会使 CPU-bursty 事务扎堆至少数核，吞吐损失达 50%。

### 2.2 三种典型失效模式

**优先级反转放大。** 持锁者 H、等待者 W 与无关 CPU 密集任务 M 构成经典三角：M 抢占 H，W 事实上继承 H 的低优先级。在 MySQL/InnoDB 上，该问题有两个独特形态：显式事务持有者在语句间隙不占任何线程（`real_id = 0`），内核侧无法定位提权对象；线程池模型下全部 worker 阻塞于行锁时，持有者的 COMMIT 在队列尾部等待无线程可用的执行池，形成**执行池活性死锁**——它在 InnoDB 等待图上无环，死锁检测器不可见。

**过载崖点。** 超过最优并发度后，lock thrashing [26] 与上下文切换使 goodput 随负载下降。Agent 负载的程序性重试使该过程自我加速。固定大小线程池避免了线程爆炸（MySQL Thread Pool 极端情况下可达 65536 线程），却把过载转化为无界排队。

**非业务开销主导。** 本仓库微基准量化了三类开销的量级：`notify_all` 惊群造成每秒 25 万次上下文切换，吞吐仅为修复后的 1/10；每命令两次切换加 epoll 重注册的交接开销可解释廉价点查场景近八成的性能差距；外部 CPU 噪声使默认调度器下 p99 劣化 7.6 倍。

### 2.3 设计目标：性能韧性

本文将**性能韧性（performance resilience）**定义为四个可测量属性的合取：

- **R1 崖点韧性**：goodput 随 offered load 开始下降的点（崖点）尽量推远；
- **R2 越崖行为**：超过崖点后 goodput 不崩溃、无活性故障、无重试放大；
- **R3 恢复韧性**：负载回落后尾延迟在秒级内恢复至基线邻域；
- **R4 干扰韧性**：外部干扰下尾延迟劣化倍数有界。

约束条件为：**C1 免标注**（默认配置不依赖任何业务优先级输入）；**C2 公平有界**（任何微调不得造成可观测的饿死）；**C3 干净开销小**（无干扰低争用下吞吐代价 ≤5%）；**C4 可降级**（内核不支持 sched_ext 时用户态机制独立有效）。

峰值吞吐在本框架中是回归约束而非优化目标：打榜负载仅用于确认无退化，与 Snowflake 所批评的“为基准特调”划清界限 [24]。

## 3 系统概述

EEVDF-Sem 部署于 MySQL 9.7 的分层架构中（图 1，文字描述）：

1. **前台 reactor 组**：reactor 线程数可配置（默认 1），每个 reactor 一个独立 epoll 实例，连接按 fd 哈希分片到各 reactor；每连接一个 C++20 无栈协程，承载全部网络等待；**收包后在 reactor 上同步完成 parse**（纯 CPU、有界、不触碰 MDL/TABLE），输出语句类型、访问表集合与事务边界供调度决策，解析失败直接返回错误、不占 worker；optimize/execute 仍由 worker 执行。
2. **执行池**：worker 数可配置的线程池（配置后固定，默认 4× 硬件并发数），以整个 THD 为交接单元（`store_globals/restore_globals`）；高优先级队列与 VIP worker 保证排队层活性。
3. **准入层**（本设计新增）：位于 reactor 与执行池之间，按排队时间发放 credit。
4. **EEVDF-Sem 调度器**：sched_ext BPF 程序，单池虚拟时间调度，消费用户态发布的意图。
5. **意图通道**：BPF maps 承载 job 类型、等待边与继承元数据；无 sched_ext 时该通道关闭，其余各层独立运行。

设计原则可概括为一句话：**公平是默认，语义是微调，微调必有界**。

## 4 设计

### 4.1 公平基座：单池虚拟时间模型

所有任务（前台事务、后台 purge、外部进程）位于同一公平池，权重默认全等。每个任务维护虚拟运行时 `vr`，eligible 任务按虚拟截止期 `vd = vr + slice/weight` 排序，最小者先运行。因 IO 或锁阻塞让出 CPU 的任务保留其 lag（经 clamp 限制以防信用囤积），从而在唤醒时获得自然优先——这是 EEVDF 对突发交互负载友好的根源，必须完整继承。

针对 EEVDF 已知的唤醒放置缺陷 [5]，唤醒路径采用**局部性优先的直放**：优先将任务放回其上次运行的 CPU（保持缓存亲和，抑制 IPC 下降）；仅当该核被占且短期无法让出时才选择其他核并直接入 local DSQ，避免周期负载均衡纠正不及造成的 bursty 扎堆。

### 4.2 提交早行：有界截止期奖励

**依据。** 2PL 下行锁持有至事务结束，COMMIT 是唯一的批量释放点：它短、必然发生、受益者是全体等待者。提权 COMMIT 不偏好任何业务，仅缩短锁系统的关键路径。ROLLBACK 同理论证成立。

**机制。** 用户态在事务提交/回滚入口写 `job_type[tid] = COMMIT`，出口清除。调度器对标记任务施加截止期提前，提前量记入§4.3 统一的借还账户：COMMIT 极短，借入量与偿还量都很小，对长期份额的影响可忽略。

**提权对象的窄化。** 仅提权 COMMIT/ROLLBACK 语句本身，不提权“锁持有者的下一条任意语句”。后者存在博弈缝隙：应用在持锁状态下执行重型查询即可借道插队。窄化后该通道无滥用空间。

**优先语句谱系（推广）。** 优先级只授予“执行后净减少系统等待”的语句，判定三问：是否释放被等待的资源、是否有界且短、是否是活性逃生口。据此分档：

| 档位 | 语句/路径 | 释放或收益 | 提权条件 |
| --- | --- | --- | --- |
| 默认优先 | COMMIT / ROLLBACK | 全部行锁 | 总是 |
| 默认优先 | XA COMMIT / XA ROLLBACK | prepared 事务的锁（2PC 决策点） | 总是 |
| 默认优先 | KILL QUERY / KILL CONNECTION | 管理面活性逃生口 | 总是 |
| 默认优先 | 死锁 victim / 锁等待超时 victim 的回滚执行 | 行锁，解开等待环 | 由 InnoDB 标记 |
| 条件优先 | UNLOCK TABLES、RELEASE_LOCK() | 表锁、用户级 GET_LOCK | 有等待者 |
| 条件优先 | 锁持有者的下一条语句（限缩版） | 缩短持锁窗口 | 有等待者，且 parse 判定为 COMMIT/短 DML；长查询不提权 |
| 可选低强度 | 语句收尾（结果写回/清理） | MDL、worker 槽位 | cleanup 阶段可判 |

反例（明确不优先）：BEGIN（无释放）；SAVEPOINT/ROLLBACK TO SAVEPOINT（InnoDB 不释放行锁，无释放收益）；普通 SELECT/DML（仅经持有者继承获益）；FLUSH TABLES WITH READ LOCK 等获取全局锁的语句（应避免插队加剧 MDL 堆积）。THD 状态感知输入包括：语句类型（reactor parse）、autocommit/显式事务、持有锁等待计数、语句阶段、死锁/超时 victim 标记、XA prepared、空闲时长。全部优先走同一借还账户。

**边界。** 当提交路径瓶颈在 IO（同步 fsync）时，该微调无收益亦无害；应用不发出 COMMIT 时无调度可救，此属应用层活性责任。

### 4.3 锁持有者条件性优先级继承与借还记账

**依据。** latch 临界区为 ns–µs 级，锁释放路径为 ms 级；持有者被抢占的每一微秒被全部等待者继承，自旋等待者还直接浪费 CPU。UFS 的 spinlock 微实验显示，失控时该现象可致 PostgreSQL PANIC，而条件性提权后总耗时仅为无干扰基线的约 2 倍 [5]。

**机制（算法 1）。** 等待边出现时（`thd_wait_begin` 或 InnoDB latch 等待事件），用户态写 `blocked_by[waiter] = holder` 及时间戳。调度器令持有者继承其所有等待者中最小的虚拟截止期：`vd_holder = min(vd_holder, min_{w ∈ waiters} vd_w)`。等待边沿依赖链传递，继承深度硬上限 D（默认 8）。等待边消失（`wait_end`、锁释放、teardown/KILL）即撤销；每条边带时间戳，超时未刷新由清扫路径删除。

```
算法 1：有界虚拟截止期继承 + 借还记账
输入：等待边集合 E = {(w, h, ts)}，任务 vd[·]、borrowed[·]、debt[·]
常量：D（继承深度上限）、B_max（窗口借入上限）、D_max（未偿还上限）
1:  等待边出现 (w, h)：若 debt[h] < D_max，为 h 开通提权窗口
2:  窗口内每次调度 h：
3:      vd[h] ← min(vd[h], min_{(w,h)∈E} vd[w])   // 继承最急等待者
4:      沿 E 传递，深度 ≤ D                       // 嵌套等待
5:      h 实际运行 t：borrowed[h] += t             // 借入记账
6:      borrowed[h] ≥ B_max ⇒ 提权失效，回到普通公平 // 借入硬顶
7:  窗口关闭（锁释放/COMMIT 完成）：
8:      debt[h] += borrowed[h]; borrowed[h] ← 0
9:  窗口外调度 h：vd_eff[h] = vd[h] + debt[h]      // 更不紧急
10:     h 因 debt 被推迟 δ：debt[h] −= δ           // 用等待偿还
11: h 阻塞/睡眠：debt 保留（不随 lag clamp 抹除）   // 防逃债
12: h 退出：debt 核销；等待边 ts 超期由清扫删除    // 有界损失
```

**为什么必须“先借后还”而不是储蓄。** cgroup CFS 带宽控制的 burst（`cpu.cfs_burst_us`/`cpu.max.burst`，Linux 5.14+ [31]）允许任务组消耗历史周期累积的未用配额：这是**储蓄模型**，其前提是该组曾被限流且有用剩的配额。锁/latch 持有者通常持续 runnable，不存在“用剩的配额”可存，储蓄模型对它结构性失效。因此本设计采用**信用模型**：提权窗口内多消耗的 CPU 记为借入，窗口关闭后转为 debt，通过推迟后续调度偿还——**短窗口内允许多拿，周期级总量守恒**。继承决定“窗口内多急”，借还决定“窗口后怎么还”，两者正交、可独立消融。debt 独立于 vruntime 存储且不随 lag clamp 抹除，防止持有者以睡眠逃债；debt 达到上限即禁止再借，消除持续争用下的无限优先。

**与经典 PI 的关系。** 经典优先级继承在固定优先级空间中传递“级别”；本设计在虚拟时间空间中传递“紧迫度”，与 EEVDF 的 lag 语义天然兼容，且继承量随等待者自身状态动态变化——等待者越急，持有者越优先。借还账户进一步保证了这种传递不破坏长期公平：这是 PI 的活性收益与 EEVDF 的份额保证的组合。

**latch 的发布成本控制。** InnoDB latch 每秒进出百万次量级，逐次发布不可行；仅在 latch 等待发生时（sync 等待数组路径）发布等待边，与 UFS 的 wait-event 触发式上报同构，稳态零开销。

### 4.4 以排队时间为信号的准入控制

削减开销无法单独防止崖点，必须限制在制工作量。本设计的准入层采用闭环 credit 控制，与既有方案的区别在信号选择：

- **信号为排队时间而非队列深度或线程数**。原型测量表明队列深度为 0 时仍有 33% 的任务等待 100–500µs（worker 被 OS 抢占），深度信号在此类失效下完全失明。
- **过载时的行为是延迟接纳而非拒绝**。新命令在 reactor 侧排队，客户端观测到延迟增加而非超时失败，从源头削弱重试放大。
- **准入等待按连接公平轮询**，防止单个 Agent 连接的突发扇出挤占全部容量。租户级资源分配不在 DB 内实现，由中间件承担（§7）。
- **迟滞控制**：credit 乘性收缩、加性恢复，避免振荡。

### 4.5 非业务开销的配套削减

固定线程池杜绝 stall 驱动的线程爆炸；cascade 唤醒避免惊群；调度相关计数器按 CPU 分片消除 false sharing；廉价查询的完成回投批量化降低每命令固定交接税；所有意图写入仅在事件路径发生，稳态零成本。

## 5 实现

原型基于 MySQL 9.7（`coroutine-dev2` 分支），Linux ≥ 6.12。本节按四条主线展开：协程在哪里（§5.2）、线程池在哪里（§5.3）、在哪里排队（§5.4）、调度如何实现（§5.5）。标注【已有】的为 v1–v3 已实现并测量过的代码，标注【v4】的为本设计新增、尚未实现的部分。

### 5.1 组件地图

| 组件 | 代码位置 | 状态 |
| --- | --- | --- |
| 协程运行时（Task/Netpoller/Executor） | `coroutine/`（task.h、netpoller.cc、executor.cc） | 【已有】 |
| 协程连接处理器 | `sql/conn_handler/coroutine_connection_handler.cc` | 【已有】 |
| THD 跨线程移交 | `sql/sql_class.cc`（`store_globals/restore_globals`） | 【已有，上游机制复用】 |
| 意图桥接（THD 等待图、job 发布） | `sql/sched_ext_bridge.h/cc` | 【已有】 |
| COMMIT 注入点 | `sql/handler.cc`（`ha_commit_trans` 入口/出口） | 【已有】 |
| BPF 调度器与加载器 | `support-files/scx/scx_db_sched.bpf.c`、`scx_db_sched_main.c` | 【已有 v3；v4 重写为单池 EEVDF-Sem】 |
| 准入层（queue-time credit） | reactor 与 executor 之间（新增模块） | 【v4】 |
| 借还账户（borrowed/debt） | BPF per-task 状态 map + 调度器记账路径 | 【v4】 |

### 5.2 协程在哪里：只协程化网络等待，不协程化执行

**协程的边界**：前台协程承担网络 IO 与 parse，optimize/execute 保持在线程上。parse 是纯 CPU、有界（语句长度限制）、不打开 TABLE、不触碰 MDL（表打开与 MDL 获取发生在 optimize 阶段），因此可在 reactor 上同步执行而不阻塞事件循环；多 reactor 分片使该部分 CPU 可水平扩展。THD 上下文仅在同步段内持有：`store_globals → parse → restore_globals`，不跨 `co_await`。optimize/execute 协程化需要完整保存 THD/TLS/锁/异常语义，代价与风险远超收益，不做。

**连接生命周期**【已有框架 + v4 parse 前移】（`coroutine_connection_handler.cc`）：每个连接一个 C++20 无栈协程帧（约几百字节），主循环为 `co_await wait_readable(fd)` → 同步 `parse`（失败直接回错、不占 worker）→ `submit_thd_job(do_command)` → `co_await CommandDone`。多语句包、存储过程与 parse 异常路径回退到 worker 内完整处理。认证同样以 job 形式提交执行池（`thd_prepare_connection`）。等待 socket 可读的连接**不占任何执行线程**——这是与 one-thread-per-connection 的本质区别。

**Netpoller**【已有】（`coroutine/src/netpoller.cc`）：单 epoll 实例同时驱动监听 fd 与全部连接 fd，ET + EPOLLONESHOT（避免惊群与重复事件），eventfd 用于跨线程唤醒（执行完成回投），内置定时器堆与 fd 兴趣表，单批事件上限 1024。KILL 与 fd 所有权采用单点关闭纪律：fd 只在 reactor 线程的 teardown 关闭；其他线程的 `vio_shutdown()` 经 pre-close 钩子转为带 token 校验的强制取消，挂起在 `wait_readable` 的协程被唤醒后走正常 teardown——fd 关闭前不复用，消除误 cancel/误 close 新连接的串扰。

**CommandDone 同步**【已有】：双阶段协议（`cmd_mu` mutex + `cmd_done` atomic + `cmd_waiting` handle）。执行线程完成 job 后先置 `cmd_done` 再经 eventfd 通知 reactor；协程侧 `await_ready` 快路径在作业已完成时直接返回不挂起，慢路径注册 handle 挂起。该协议保证“完成信号不丢失、协程不永久悬挂”，是每命令两次线程切换的固定成本所在。

**THD 移交**【已有】：交接单元是整个 THD 对象而非执行计划——AccessPath 内嵌大量指向 THD `main_mem_root` 的指针，不可序列化。worker 接管时执行 `store_globals()` + `thd_set_thread_stack()`，释放时 `restore_globals()`。这是 MySQL 上游已有的跨线程迁移机制，本架构只是复用。

### 5.3 线程池在哪里：固定执行池 + HP/VIP 活性通道

执行池【已有】（`coroutine/src/executor.cc`）为 pthread 池，**worker 数可配置**（默认 `max(4 × hardware_concurrency, 16)`），**VIP worker 数可配置**（默认 1，worker 0..num_vip-1 只消费 HP 队列），创建时 `sched_setaffinity` 绑核并执行 `my_thread_init/end`。默认系数 4 用于吸收 `do_command` 内的行锁等待与同步 IO 原地阻塞——**阻塞留给线程，不挂起归还**（v1 的明确取舍；执行侧挂起列为远期工作）。固定池从根本上消除了 MySQL Thread Pool 插件 stall 驱动的线程爆炸（其上限可达 65536 线程）。

唤醒策略为 **cascade notify**：`submit()` 时 `notify_one`；worker 取任务时若队列剩余 >1 则再 `notify_one`。纯 `notify_one` 会让单 worker 独占突发队列并在阻塞 IO 后死锁（实测 32 连接 0 TPS）；`notify_all` 造成每秒 25 万次上下文切换（实测 311 TPS）；cascade 修复后 3176 TPS。

**HP/VIP 通道**【已有 v2】：reactor 提交前查 `is_lock_holder(thd)`（THD 级等待图，等待计数 >0 即命中），命中则提交 HP 队列；0 号 worker 为 VIP 专用 worker，只消费 HP 队列。即使普通 worker 全部阻塞在行锁上，持锁者的 COMMIT 仍有线程执行——执行池活性死锁在排队层被结构性消除。该机制纯用户态、始终编译，不依赖 sched_ext。

### 5.4 在哪里排队：五个队列的分工

调度问题被分解为五个语义不同的排队点，每点有明确的负责人：

| # | 队列 | 实现位置 | 等待内容 | 负责机制 |
| --- | --- | --- | --- | --- |
| Q1 | 网络等待 | reactor 协程（`wait_readable`，无队列实体） | 客户端命令到达 | epoll；不占线程 |
| Q2 | 准入队列【v4】 | reactor 与 executor 之间的 credit 闸门 | 过载时的延迟接纳 | queue-time credit + 按连接公平轮询 |
| Q3 | 执行队列 | executor 共享 Normal 队列 + HP 队列 | 已接纳命令等 worker | cascade notify；HP/VIP 保活性 |
| Q4 | 锁/latch 等待 | InnoDB 等待数组 / THD 等待图 | 行锁、latch | CATS（锁授予顺序）；等待边发布给 Q5 |
| Q5 | OS 运行队列 | EEVDF-Sem 的 DSQ | runnable 任务等 CPU | 单池 vd 排序 + 语义微调 + 借还 |

该分解解释了两个关键设计取舍：Q2 与 Q3 分离是因为“限流”与“保活性”是不同决策——准入层只决定进不进，HP 通道只保证进入后关键命令不被堵；Q4 与 Q5 必须打通是因为 InnoDB 的锁调度（CATS）决定谁下一个拿锁，却无法决定持锁者现在有没有 CPU——等待边是两个队列之间唯一需要传递的信息。

### 5.5 调度怎么实现：EEVDF-Sem 的 BPF 实现

调度器为 sched_ext struct_ops BPF 程序【v4，替代 v3 的三层 DSQ 实现】，由用户态加载器在运行时载入，不修改内核源码。

**回调分工**：

- `enqueue`（任务变 runnable）：从 per-task map 读取 `vr/debt/job_type`，计算 `vd = vr + slice/weight + debt − bonus(job_type)`；若处于提权窗口则再执行 vd 继承（取等待者最小 vd，深度 ≤8）。普通任务入共享 DSQ（按 vd 排序）；唤醒直放场景下直接入目标 CPU 的 local DSQ
- `dispatch`（CPU 空闲取任务）：从共享 DSQ 取 vd 最小者派发；被推迟的任务按推迟时长冲减 debt（偿还路径）
- `select_cpu`（唤醒选核）：局部性优先——首选上次运行的核（缓存亲和）；仅当该核被占且短期无法让出时改选他核。**已知的实现陷阱**：此回调直插 `SCX_DSQ_LOCAL` 会跳过 `enqueue`，微调与记账逻辑必须在该快路径上有等价实现或显式禁用，否则提权静默失效
- `running/stopping`：实际 CPU 消耗记账——vruntime 正常推进；提权窗口内的运行时间累加 `borrowed`，达 `B_max` 关闭窗口

**per-task 状态**【v4】（BPF map，key = tid）：`vr`、`debt`、`borrowed`、`job_type`、提权窗口时间戳。等待边单独存于 `blocked_by` map（key = waiter tid，value = holder tid + ts）。debt 独立于 vr，**不经过 lag clamp**——阻塞/睡眠不清债；任务退出时 map 条目由 teardown 路径删除（`forget_thd`），残余 debt 核销。

**发布纪律**【已有，v2/v3 工程教训固化】：执行 job 开始/结束（`thread_job_begin/end`）解决显式事务 holder 语句间隙 `real_id = 0` 的 tid 归属；所有 BPF map 写入必须在释放等待图叶子锁（`s_wait_mu`）之后进行——v2 曾因锁内发布导致自死锁，首次行锁等待即全服挂死；KILL/回滚/teardown 的清理路径是正确性测试必查项。

**性能预算**：意图写入仅发生在事件路径（job 边界、锁等待、COMMIT 进出），稳态无写；对标 UFS 的 hinting 开销实测 ≤1%，本设计 v3 已测得干净负载约 5% 总开销（含 BPF 回调），v4 预算不高于该水平。

### 5.6 可观测性与降级

状态变量【已有 + v4 扩展】：`Coroutine_handoff_queue_depth`、`submit_to_run` 七桶直方图（区分 DB 排队与 OS 调度丢失）、`hp_jobs_submitted/executed`、`admission_deferred`【v4】、借还账户快照（borrowed/debt 总量）【v4】、BPF loader `--dump` 内核侧 map 快照。

降级矩阵【已有原则，v4 继承】：无 sched_ext 内核时 BPF 半边整体关闭，Q1/Q2/Q3/Q4 的全部用户态机制（协程等待、准入、HP/VIP、等待图）独立有效，Q5 退化为原生 EEVDF；`WITH_SCX_SCHEDULER=OFF` 时构建期不链接任何 BPF 代码。

## 6 实验评估

### 6.1 研究问题

- **RQ1（干扰韧性）**：外部 CPU 干扰下，语义发布能否约束尾延迟劣化？
- **RQ2（锁活性）**：等待者数超过执行线程数时，持有者 COMMIT 是否仍被调度执行？
- **RQ3（代价与公平）**：干净负载的吞吐代价是多少？持续高优流下后台与普通任务是否前进？
- **RQ4（崖点与恢复，v4 目标）**：准入控制能否推远崖点、保证越崖不退化并缩短恢复时间？
- **RQ5（突发吸收，v4 目标）**：Agent 式阶跃与潮汐负载下，系统是否免重试放大？

### 6.2 实验设置

【实测】v1–v3 数据来自两类环境：（a）本地 aarch64 10 核、Linux 6.18.15、NVMe；（b）阿里云 ecs.c9i.4xlarge 16 vCPU，数据库与压测节点分离，sysbench 1.0.20，32 表 × 10 万行，open-loop 点查与封闭环读写混合。对照配置：C1 = one-thread-per-connection（OS-only 基线）、C2 = 协程执行池无 SCX、C4 = 协程 + sched_ext 语义发布。每配置 ≥2 轮取中位数，噪声注入于数据库节点。

### 6.3 测量方法学

**开放环与封闭环分工。** 封闭环（固定客户端线程数，完成一个再发下一个）适合可重复地比较吞吐，但存在 coordinated omission：服务变慢时客户端自动少发请求，尾部被“测没”[32][33]。因此**过载、突发、尾延迟类实验一律采用开放环**（按预定到达过程发送，速率不随响应变慢而降低），封闭环仅用于正向回归基线。

**指标口径。** 分列 offered rate、完成率、**goodput**（提交且不计入 abort/重试的有效吞吐）、abort/retry 计数；延迟报告 p50/p95/p99/p999/max 与 SLO 违约率，不用均值替代尾部。每配置 ≥2 轮取中位数，max 离群必须复测确认。

**观测分解。** 结果（goodput）、排队（handoff 深度、submit→run 直方图、OS run queue）、依赖（row lock waits/time、deadlocks、HP submitted/executed、map 条目）、资源（上下文切换、迁移、IPC、RSS/FD）。submit→run 混合用户态排队与 OS 调度等待，不单独归因于任何一层。

**对照与消融。** C0 = 原生 Thread Pool、C1 = one-thread-per-connection、C2 = 协程执行池无 SCX、C4 = EEVDF-Sem。逐项消融：COMMIT 奖励开/关、vd 继承开/关、借还开/关、准入开/关。噪声注入必须在数据库节点；每配置独立输出目录；远程状态采集失败不得静默归零。

### 6.4 正向基准（传统负载，仅作回归约束）

| 基准 | 配置 | 目的与判据 |
| --- | --- | --- |
| sysbench `oltp_read_write` | 封闭环 32/64/128/256 线程伸缩 | 锁争用下吞吐与 p95 不退化；干净开销 ≤5%（C3） |
| sysbench `oltp_point_select` | 封闭环 32 线程 + open-loop rate 扫描 | 分离每命令交接税；报告与 OTPC 的差距构成 |
| TPC-C（BenchBase） | 多仓库，封闭环吞吐 + open-loop 尾延迟；交互式 vs 存储过程两种模型 | 与文献可比的通用 OLTP 参照；分离通信开销 [28] |
| CH-benCHmark（TPC-C + TPC-H 并发）[34] | TP 与 AP 并发执行 | HTAP 混合下 TP goodput 与 AP 完成时间双侧不退化 |

历史基线【实测，v1】：oltp_read_write 32 线程 3176 TPS（128 线程无执行线程耗竭），oltp_point_select 67,320 TPS（p95 0.68ms）。这些数字仅作回归锚点，不构成 v4 结果。

### 6.5 负向稳定性矩阵（主验收）

| # | 场景 | 方法 | 指标 | 通过判据 | 依据 |
| --- | --- | --- | --- | --- | --- |
| N1 | 外部 CPU 干扰（B7） | DB 节点 stress-ng ×16，open-loop 点查 | p99/p999/max、劣化倍数 | 劣化 ≤2× | Caladan、UFS |
| N2 | 混合负载（B1） | BG 会话 ×8，minmax/5050；CH-benCHmark | 前台 p99、BG 进度、purge history | 前台改善且 BG 不饿死 | UFS |
| N3 | 优先级反转与锁活性（B2） | holder parked + waiter > worker + 锁链 + KILL/ROLLBACK | HP submitted/executed、COMMIT submit→run、map 清空 | 无 hang、无 map 泄漏 | UFS spinlock 微实验 |
| N4 | 热点 skew 扫描 | uniform → zipfian θ 递增；小表/单仓 TPC-C | goodput/p99、row lock waits、deadlocks | 无吞吐崩塌、无活性故障 | DBx1000、CATS |
| N5 | 执行池耗竭（C1） | 长事务多语句阻塞风暴，N > worker | 活性、队列深度、恢复时间 | HP 通道活性，无 0 TPS | 本仓库 v1 限制 |
| N6 | 死锁雪崩（C3） | 反向顺序更新 2–4 行 + KILL/timeout/retry | deadlocks、goodput、重试放大 | victim 回滚快、无重试风暴 | — |
| N7 | Agent 突发与潮汐 | open-loop 方波（1s 内 0→4× 崖点）+ 程序性重试 | 准入收缩、恢复时间、重试放大系数 | 不崩溃、秒级恢复 | Breakwater、TailBench |
| N8 | 过载膝点（C5） | offered rate 阶梯越过崖点 | 崖点位置、越崖 goodput 保持率 | 崖点 ≥ 基线 1.2×、goodput 不降 | Breakwater |
| N9 | 高连接与 churn（C4） | 空闲 1k/5k/20k 连接 + 高频 connect/disconnect/KILL | RSS/FD/协程计数回落、认证排队 | 无泄漏、正常查询 p99 稳定 | — |
| N10 | 争用 soak（C6） | 30–60min 热点负载 | RSS 斜率、map/图节点趋势、TPS/p99 漂移 | 无降解、无泄漏 | — |
| N11 | 借还公平对抗 | 持续 latch 争用；提权后立即睡眠；分钟级份额测量 | 份额偏差、D_max 到顶行为、debt 残留 | 偏差 <1%、到顶回落公平、无逃债 | cgroup burst 思想 [31] |
| N12 | 快路径审计 | idle CPU 唤醒直插 SCX_DSQ_LOCAL vs enqueue 路径对照 | 提权是否生效、记账完整性 | 两路径语义一致 | 仓库已知陷阱 |

### 6.6 初步结果【实测，v1–v3 原型】

**RQ1：外部噪声下的尾延迟（B7）。** 数据库节点注入 16 个 CPU 抢占者（模拟“OS 层来了 DB 不认识的负载”），open-loop 10k rate 点查：

| 配置 | 基线 p99 | +噪声 p99 | 劣化 |
| --- | --- | --- | --- |
| C2（EEVDF） | 0.395 ms | 3.02 ms | **7.6×** |
| C4（语义发布） | 0.44 ms | 0.60–0.74 ms | 1.4–1.7× |

噪声期绝对 p99 相差 4–5 倍。诚实记录：C4 的 max（约 22 ms）并不优于 C2（18 ms）——tier 保护作用于分位数而非极端离群；该结果仅证明 CPU 噪声，不能外推至 IO/内存带宽干扰。

**RQ1 补充：锁持有者提权（v1 云端）。** 热点 UPDATE 负载开启锁持有者 slice/vtime 保护后，TPS 提升 **66%**、p95 延迟下降 45%（18.5k → 30.8k TPS）。同时暴露 v1 的 tid 解析缺陷：显式事务持有者在语句间隙无 OS 线程，13.8k 次等待边发布失败——该缺陷直接催生了 v2 的执行期发布机制（§4.4 的 in-flight 语义）。

**RQ2：锁活性（B2）。** 显式事务 `BEGIN; UPDATE; SLEEP(4); COMMIT` 叠加竞争者，各配置等待者均约 4007 ms（≈4 秒预设锁等待 + 提交）——HP/VIP 通道已使持有者 COMMIT 活性不再是差异来源。该结果不能推广为任意锁图的安全性证明。

**RQ3：代价与公平（B5、B1）。** 正常负载下 SCX 语义发布带来约 **5%** 吞吐开销（BPF 回调与 map 维护）；4 核 256 线程过饱和时吞吐反超 **20%**，但 p99 更高——公平与尾延迟存在真实权衡。混合负载中后台 tier 使前台 p99 中位数从 87.6 ms 降至 75.8 ms（−13%）。120 秒 soak 无内存/FD 泄漏，TPS 无降解。

**微基准对照。** `notify_all` 惊群（250K 上下文切换/秒）下吞吐 311 TPS，cascade 唤醒修复后 3176 TPS；50ms 轮询实现仅 3.68 TPS，epoll 驱动修复后恢复。这些数字支撑 §2.2 对非业务开销量级的论断。

### 6.7 验收标准与覆盖状态【计划，未执行】

以下实验在实现冻结指标后执行，当前不构成结果：

1. **Agent 突发（RQ5，核心新增场景）**：open-loop 方波负载（1 秒内 0 → 4× 崖点速率阶跃），叠加程序性重试模型。度量准入收缩曲线、租户间排队公平、恢复时间（R3）与重试放大系数。
2. **崖点扫描（RQ4）**：offered rate 阶梯爬升越过崖点，报告崖点位置（R1）、越崖后 goodput 保持率（R2）、无 hang 验证。验收口径：崖点 ≥ EEVDF 基线的 1.2×。
3. **提交早行的消融（RQ1/RQ3）**：`B_commit ∈ {0, 0.5, 1, 2} slice` 扫描，区分收益来源；`innodb_flush_log_at_trx_commit=1` 下验证 IO 瓶颈时无负效应。
4. **继承参数扫描（RQ2/RQ3）**：深度上限 D 与清扫超时的敏感性；构造提权泄漏对抗场景（KILL/teardown/回滚）验证清理路径。
5. **借还记账的公平回归（RQ3，新增）**：分钟级长窗测量被提权任务的实际 CPU 份额偏差（目标 <1%）；`B_max/D_max ∈ {1,2,4} slice` 扫描；两个对抗场景——（a）持续 latch 争用：借债达到 D_max 后必须回落普通公平，验证无无限优先、无饿死；（b）sleep 逃债：提权后立即阻塞/睡眠，验证 debt 不被 lag clamp 抹除、唤醒后仍须偿还。COMMIT 路径做借入量与提前量的联合消融。
6. **负向矩阵回归**：C1 执行线程耗竭、C2 热点 skew、C3 死锁雪崩、C4 连接 churn、C5 过载膝点、C6 争用 soak，全部从待办提升为验收门槛。
7. **公平下限**：持续高优流下监控 purge history 长度与普通会话最大连续等待，确认保底窗口生效。

### 6.8 执行规范

噪声必须注入数据库节点（早期误注压测节点导致实验无效）；每配置独立输出目录；≥2 轮中位数并检查 max 离群（单次 217 ms 离群经复测排除）；远程状态采集失败不得静默归零；封闭环与开放环数字不混用。

## 7 讨论

**为什么不做自动负载分类？** IconqSched [29]、LSched [22] 证明了预测式调度的潜力，但其前提是负载可标注或可预测。本设计刻意将智能限制在结构语义内：COMMIT 与锁等待边是确定事实，无模型误差、无训练成本、无回退负担。

**为什么不做库内业务优先级？** resource group、租户权重等机制把高频变化的业务语义固化进最难变更的组件，且只能控制线程级 nice/绑核，无法覆盖锁、缓冲池、IO 与内存带宽，既不完整也不可持续。本设计将业务间资源分配整体上移中间件：泳道隔离、租户配额、限流与重试退避由最了解租户与 SLO 的一层执行；数据库只对自身效率负责——结构语义微调、全局 queue-time 准入（业务无关的自我保护）与按连接公平排队，并向中间件暴露 queue-time、goodput、准入延迟等闭环信号。需要强隔离的租户，推荐泳道级独立实例而非库内权重。

**与 PreemptDB 的分工。** PreemptDB [13] 在数据库层用用户态中断实现事务级抢占，适合需要事务内精细抢占的场景；本设计在 OS 层工作，任务阻塞即让出 CPU、可同核并发多个高优任务，且不依赖特定硬件特性。两者解决的是不同粒度的抢占问题，可组合。

**局限。** 本设计不解决 IO 瓶颈、应用不发 COMMIT、NUMA 分区与 AP 内部算子级并行；sched_ext 无稳定 ABI 承诺，需锁定内核版本。reactor 支持可配置多分片（默认 1）以消除极高 QPS 下的单点。

## 8 相关工作

**数据库调度与优先级。** Carey 等 [8] 早已指出 DBMS 优先级需跨 CPU、磁盘与缓冲协同；CATS [25] 以阻塞面加权决定锁授予顺序，与本设计的 CPU 侧继承互补；R-SMF [9] 与 DRFT [10] 分别探索事务排序空间与多租户事务公平；Polaris [23]、Natto [20] 在 OCC/分布式语境下实现事务优先级。PreemptDB [13] 提供事务级用户态抢占。与上述工作不同，本文聚焦免标注的结构语义与内核 CPU 分配。

**OS 调度与干扰治理。** IX [4]、Shinjuku [17]、Caladan [11]、ZygOS 等量化并治理微秒级调度开销与干扰；Breakwater [6] 以排队时间驱动 credit 准入，本文将其信号机制适配到数据库执行池。EEVDF 的唤醒放置缺陷在 [5] 及内核社区多次报告中均有记载。

**DB–OS 协同。** Practical DB-OS Co-Design [27] 采用特权内核 bypass；Tigger [7] 与 BPF-DB [26b] 用 eBPF 下沉数据库逻辑；MosaicDB [14] 以协程化事务隐藏 OS 调度延迟。UFS [5] 与本文最为接近：本文保留其意图通道与条件性提权，去除其人工 TS/BG 分层，并将基座替换为单池 EEVDF 虚拟时间模型，新增过载准入与突发韧性目标。

**性能剖析。** Looking Glass 系列 [12, 28]、DBx1000 [26]、Shore-MT [18]、微架构分析 [19] 共同构成本文开销归因的证据链：瓶颈从锁与缓冲池上移至通信、内存层次与调度协同。

## 9 结论

本文主张将数据库高并发调度的目标从峰值吞吐转向性能韧性，并给出一条无需业务标注的技术路线：以 EEVDF 公平虚拟时间为基座，用两类普适结构语义（提交早行、锁持有者条件性继承）做有界微调，配合排队时间准入控制应对 Agent 时代的突发与过载。分阶段原型的实测证据表明，语义发布在外部干扰下将尾延迟劣化从 7.6 倍降至 1.4–1.7 倍，代价约为 5% 干净吞吐。完整韧性评估（崖点、恢复时间、突发吸收）是下一步工作。

## 参考文献

[4] Belay et al. IX: A Protected Dataplane Operating System. OSDI 2014.
[5] Bilodeau-Savaria, Scherzinger. Unfair by design: eBPF-based scheduling of mixed database workloads (UFS). arXiv:2605.02377, 2026.
[6] Cho et al. Overload Control for µs-scale RPCs with Breakwater. OSDI 2020.
[7] Butrovich et al. Tigger: A Database Proxy That Bounces with User-Bypass. PVLDB 16(11), 2023.
[8] Carey, Jauhari, Livny. Priority in DBMS Resource Scheduling. VLDB 1989.
[9] Cheng et al. Towards Optimal Transaction Scheduling (R-SMF). PVLDB 17(11), 2024.
[10] Cheng et al. Fair Transaction Processing for Multi-Tenant Databases (DRFT). PVLDB 18(8), 2025.
[11] Fried et al. Caladan: Mitigating Interference at Microsecond Timescales. OSDI 2020.
[12] Harizopoulos et al. OLTP Through the Looking Glass, and What We Found There. SIGMOD 2008.
[13] Huang et al. Low-Latency Transaction Scheduling via Userspace Interrupts (PreemptDB). SIGMOD 2025.
[14] Huang et al. The Art of Latency Hiding in Modern Database Engines (MosaicDB). PVLDB 17(3), 2023.
[17] Kaffes et al. Shinjuku: Preemptive Scheduling for µsecond-scale Tail Latency. NSDI 2019.
[18] Johnson et al. Shore-MT: A Scalable Storage Manager for the Multicore Era. EDBT 2009.
[19] Sirin et al. Micro-architectural Analysis of In-memory OLTP. SIGMOD 2016.
[20] Yang, Yan, Wong. Natto: Providing Distributed Transaction Prioritization. SIGMOD 2022.
[22] Sabek, Ukyab, Kraska. LSched: A Workload-Aware Learned Query Scheduler. SIGMOD 2022.
[23] Ye et al. Polaris: Enabling Transaction Priority in Optimistic Concurrency Control. SIGMOD 2023.
[24] Snowflake. Industry Benchmarks and Competing with Integrity. https://www.snowflake.com/en/blog/industry-benchmarks-and-competing-with-integrity/
[25] Tian et al. Contention-Aware Lock Scheduling for Transactional Databases (CATS). PVLDB 11(5), 2018.
[26] Yu et al. Staring into the Abyss: An Evaluation of Concurrency Control with One Thousand Cores. PVLDB 8(3), 2014.
[26b] Butrovich et al. BPF-DB: A Kernel-Embedded Transactional DBMS for eBPF Applications. SIGMOD 2025.
[27] Zhou et al. Practical DB-OS Co-Design with Privileged Kernel Bypass. SIGMOD 2025.
[28] Zhou et al. OLTP Through the Looking Glass 16 Years Later: Communication is the New Bottleneck. CIDR 2025.
[29] Wu et al. Improving DBMS Scheduling Decisions with Accurate Performance Prediction on Concurrent Queries (IconqSched). PVLDB 18(11), 2025.
[30] Wagner, Kohn, Neumann. Self-Tuning Query Scheduling for Analytical Workloads. SIGMOD 2021.
[31] Corbet, J. The Burstable CFS Bandwidth Controller. LWN.net, 2021. https://lwn.net/Articles/844976/；Linux 内核文档：CFS Bandwidth Control（`cpu.cfs_burst_us` / `cpu.max.burst`，Linux 5.14+）。
[32] Kasture, Sanchez. TailBench: A Benchmark Suite and Evaluation Methodology for Latency-Critical Applications. IISWC 2016.
[33] Fruth et al. Tell-Tale Tail Latencies: Pitfalls and Perils in Database Benchmarking. arXiv:2107.11607, 2021.
[34] Cole et al. The Mixed Workload CH-benCHmark. DBTest 2011.
