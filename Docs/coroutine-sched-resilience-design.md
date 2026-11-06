# 面向高并发与突发流量的数据库性能韧性方案

> 副标题：基于 sched_ext 的公平语义协同调度（EEVDF-Sem）
> 状态：设计草案（v4，衔接 `coroutine-sched-design.md` 的 v1–v3）
> 立场声明：本方案不以任何基准打榜成绩为优化目标。

---

## 一、设计立场：性能韧性优先于峰值性能

### 1.1 反对打榜，不等于反对性能

Snowflake 在《Industry Benchmarks and Competing with Integrity》中的批评值得全文重读：打榜时代的行业惯例是“添加配置旋钮、特殊设置、针对特定场景的优化，把基准成绩抬高百分之零点几”——这些改动**对客户日常负载几乎没有正面影响，甚至为负**，同时把复杂度转嫁给用户，把研发精力从真正重要的问题上引开。

对当前国内业界复现这一模式的现象，本方案的态度是：

- 单场景峰值 TPS 不构成设计目标；**SLO 约束下的 goodput、尾延迟、过载后的恢复时间**才构成目标
- 业务调整频率很高，任何“为某类负载特调”的参数都会在下一次业务变化时失效甚至反噬。因此**调度策略必须业务无关、免标注、免调参**——不是“调得好”，而是“不需要调”
- 但性能本身仍然要尽量高：韧性不是保守，是把优化的靶心从“最好情况多好”移到“最坏情况多可控”

### 1.2 AI Agent 时代的负载假设

Agent 成为服务的主要调用方后，数据库访问模式发生三个变化：

1. **潮汐与突发**：Agent 的并行扇出（一次任务拆解出几十个子查询）、重试风暴、定时批任务叠加，使流量呈阶跃和脉冲形态，而不是平滑爬坡
2. **无人类的耐心**：客户端是程序，超时即重试，过载会被重试放大为正反馈（abort → retry → 更深过载）
3. **业务语义不可标注**：Agent 动态生成 SQL，无法要求业务方给每个会话打“高优/低优”标签

由此推出核心要求：**突然来的流量不许把系统打过崖点（cliff），坏场景下仍要有可预期的行为**。崖点的定义不是“吞吐不再增长”，而是 goodput 随 offered load 下降、尾延迟失控、出现活性故障的那一点。韧性 = 把崖点推远 + 越过崖点后不崩 + 回落后快速恢复。

### 1.3 优化对象：削减非业务开销，而非提升单点吞吐

高并发下的能力损失主要是非业务开销（详见文献：CIDR'25《OLTP Through the Looking Glass 16 Years Later》——简单事务 52–67% 的运行时耗在消息传递上）：

| 开销 | 本仓库已量化的证据 | 本方案的对应机制 |
| --- | --- | --- |
| 上下文切换 | `notify_all` 惊群 250K 次/秒 → 311 TPS（修复后 3176） | 固定线程池 + cascade 唤醒（已有）；调度器侧减少无效迁移 |
| 调度丢失 | 队列为空时 33% job 仍等 100–500µs（worker 被抢占/迁移） | 唤醒直放 CPU + 局部性保持（§4.1） |
| 锁决策反转 | 行锁风暴下 stall 兜底线程爆炸；显式事务 holder 无 tid | COMMIT 早行（§4.2）+ latch holder 条件性保护（§4.3） |
| false sharing / IPC 下降 | 共享计数器、全局 map 热点 | 分片计数器、per-CPU 状态、减少全局锁范围（§4.6） |
| 外部干扰 | B7：EEVDF 下 p99 劣化 7.6 倍 | 公平基座上的有界语义微调（§4 全章） |

---

## 二、总体架构

```
        Agent/应用（突发、不可标注、重试敏感）
              │
              ▼
┌─────────────────────────────────────────────────────┐
│ ① 准入层（新增）：queue-time credit + 背压            │
│    - 依据排队时间而非线程数做 admission               │
│    - 过载时延迟接纳、公平排队，不拒绝、不崩溃           │
├─────────────────────────────────────────────────────┤
│ ② 前台 reactor 组（线程数可配置，fd 分片）：协程 epoll  │
│    - 连接等待不占线程；收包后同步完成 parse              │
│    - 输出语句类型/表集合/事务边界供调度；optimize 留 worker│
├─────────────────────────────────────────────────────┤
│ ③ 执行池（已有 v1/v2）：worker 数可配置 + HP/VIP       │
│    - 排队层活性：holder 的 COMMIT 不被普通队列堵死      │
│    - in-flight 任务类型发布（job begin/end）           │
├─────────────────────────────────────────────────────┤
│ ④ EEVDF-Sem 调度器（本方案核心，sched_ext BPF）        │
│    - 单池公平基座：vruntime / 虚拟截止期（EEVDF 思想）  │
│    - 语义微调 A：COMMIT/ROLLBACK 截止期提前（有界）     │
│    - 语义微调 B：latch/锁持有者条件性优先级继承          │
│    - 防饿死：aging + clamp + 保底，无 TS/BG 人工分层    │
├─────────────────────────────────────────────────────┤
│ ⑤ 意图通道（已有 v2/v3 的扩展）：BPF maps              │
│    job_type[tid]、blocked_by[waiter→holder]、          │
│    holder_credit[tid]（继承深度/时间戳，用于有界清理）   │
└─────────────────────────────────────────────────────┘
```

与 v3 的关键差异：**取消 TS/Normal/BG 三层人工分层作为默认形态**；业务资源分配上移中间件（§4.7）。

reactor 与 worker 均为**可配置多实例**：reactor 线程数可配置（默认 1，每 reactor 独立 epoll 实例，连接按 fd 哈希分片，消除单 reactor 瓶颈）；worker 数与 VIP worker 数可配置（默认 `max(4×硬件并发数,16)` / 1）。

---

## 三、设计原则

1. **公平是默认，语义是微调**：所有任务在同一个公平池里按 EEVDF 思想获得 lag 补偿；语义微调只改变“ eligible 任务里谁先跑”和“唤醒后落在哪个 CPU”，不改变长期份额的量级
2. **只调度可知的语义**：调度器只消费三类事实——这是 COMMIT/ROLLBACK、谁持有被等待的锁、谁已经等了很久。凡是需要业务知识才能判断的（TP vs AP、租户等级），一律不作为默认策略的输入
3. **微调必须条件性、临时性、有界**：没有等待者不保护持有者；锁释放即撤销；单次提权的时间与继承深度有硬上限
4. **过载时退化而不是崩溃**：超过容量时排队时间线性增长，但 goodput 不下降、无活性故障、无重试放大；回落后在秒级内恢复基线延迟
5. **负向场景是验收标准**：打榜负载只作回归参照；验收以噪声、锁风暴、突发、热点、长事务等负向矩阵为准（§六）
6. **业务资源分配上移中间件**：数据库只对自身总体吞吐与延迟负责，业务间的泳道、配额、限流由中间件实现；库内不引入 resource group 式业务优先级（§4.7）

---

## 四、核心机制

### 4.1 公平基座：EEVDF-Sem 的单池 vruntime 模型

以 EEVDF 的设计思想为基座（实现形态是 sched_ext BPF 程序，不改内核源码）：

- **虚拟截止期**：eligible 任务按 `vd = vruntime + slice / weight` 排序，vd 最小者先跑。weight 默认全等——没有人为高低优
- **lag 补偿保留**：因 IO/锁阻塞让出 CPU 的任务保留其 lag（受 clamp 限制，防信用囤积），醒来后自然优先——EEVDF 对 bursty 交互负载友好的根本原因，必须继承
- **唤醒直放 + 局部性**：借鉴 UFS 的 direct-to-CPU 思路解决 EEVDF 已知的唤醒放置缺陷（bursty 任务扎堆）：唤醒时选“能让它最快跑起来”的核并直接入 local DSQ，但**优先上一次运行的核**（cache 亲和）；只有该核被占且短期内无法让出时才迁移。这是针对 IPC 下降（迁移导致缓存失效）的微调
- **借鉴而非照搬 UFS**：UFS 的 TS kick preempt 只用于下文两类有界微调场景，不作为常态机制

### 4.2 语义微调 A：COMMIT/ROLLBACK 早行（commit deadline bonus）

**依据**：2PL 下锁持有到事务结束，COMMIT 是唯一的批量释放点，短、必然发生、受益者是所有 waiter。这是不需要业务标注、几乎无条件成立的结构性优化（前文已论证）。

**机制**：

- 用户态在 `ha_commit_trans()`/`ha_rollback_trans()` 入口写 `job_type[tid] = COMMIT`（已有 v1 注入点，复用），出口清除
- 调度器对该任务施加**有界截止期奖励**：`vd -= COMMIT_BONUS`（常数，量级为一个普通 slice），而不是无限 slice 或 RT 抢占。效果：COMMIT 在 eligible 集合里排最前，但跑完即消失，长期份额不变
- **窄化提权对象**：只提权 COMMIT/ROLLBACK 语句本身，不提权“锁持有者的下一条任意语句”——防止应用持锁跑重查询借道插队（博弈面）

**优先语句谱系**：优先级只授予“执行后净减少系统等待”的语句。默认优先：COMMIT/ROLLBACK、XA COMMIT/ROLLBACK（2PC 决策点）、KILL QUERY/CONNECTION（管理面活性逃生口）、死锁 victim 与锁等待超时 victim 的回滚执行。条件优先（有等待者时，走借还账户）：UNLOCK TABLES / RELEASE_LOCK()；锁持有者的下一条语句的限缩版——parse 判定为 COMMIT/短 DML 才提权，长查询不提权（窄化条款的精确化，parse 前移使其可执行）。可选低强度：语句收尾（结果写回/清理，释放 MDL 与 worker 槽位）。反例：BEGIN 无释放；SAVEPOINT/ROLLBACK TO SAVEPOINT 在 InnoDB 不释放行锁，无收益；普通 SELECT/DML 仅经持有者继承获益；FTWRL 等获取全局锁的语句应避免插队。THD 状态感知输入：语句类型（parse）、autocommit/显式事务、锁等待计数、语句阶段、victim 标记、XA prepared、空闲时长。

**边界（诚实声明）**：

- 瓶颈在 IO（`innodb_flush_log_at_trx_commit=1` 的 fsync）时该微调无收益也无害——CPU 层优化不承诺解决 IO 等待
- 应用不发 COMMIT 时无调度可救，这是应用层的活性责任

### 4.3 语义微调 B：锁/latch 持有者的条件性优先级继承

**依据**：临界区短（latch ns~µs、锁释放路径 ms 级）、等待者众、持有者被抢占时全体 waiter 连带受损、自旋者直接烧 CPU。UFS 的 spinlock 微实验表明失控时可致 PostgreSQL PANIC，条件性提权后 ≈2× 无干扰基线。

**机制**：

- **触发**：仅在等待边出现时启动（用户态 `thd_wait_begin`/InnoDB latch 等待路径写 `blocked_by[waiter_tid] = holder_tid`）。无等待 = 无保护 = 零开销
- **继承形式**：holder 继承 waiter 的调度紧迫度（取 waiter 的最小 vd，即“最急的那个等待者有多急”），而非固定提到最高级。等待者越急，holder 越优先——这是 PI 思想与 EEVDF 虚拟时间的自然结合
- **传递与上限**：沿等待边传递（A 等 B、B 等 C 时 C 也获益），继承深度硬上限（如 8），每边带时间戳，超时未刷新即清除
- **退出**：锁释放（`wait_end`）即撤销；连接 teardown/KILL 走 `forget_thd` 清理，杜绝提权泄漏
- **latch 覆盖策略**：InnoDB latch 进出频率太高，不逐次发布；只在 latch 等待发生（sync 数组等待事件）时发布等待边，与 UFS 在 PG wait-event 路径上报同构

**借还记账（borrow-and-repay）：提权不是免费的，下个周期还回来**

借鉴 cgroup CFS 带宽控制的 burst 思想（`cpu.cfs_burst_us` / `cpu.max.burst`，Linux 5.14+），但模型必须改造：cgroup burst 是**储蓄模型**——只有历史上被 throttle 且有用剩 quota 的组才能累积 burst 额度；而锁/latch 持有者通常持续 runnable，没有“用剩的 quota”可存，储蓄模型对它无效。因此采用**信用模型：先借后还**。

- **账户**：每任务两个计数——`borrowed`（当前提权窗口已借入的 CPU 时间）、`debt`（窗口关闭后的待偿还量）。上限：`B_max`（单窗口借入上限，约 1 个 slice）、`D_max`（未偿还上限，约 2–4 个 slice）
- **借入**：提权窗口内实际消耗的 CPU 时间记为 `borrowed`；借满 `B_max` 后提权自动失效、回到普通公平——**窗口内多拿有硬顶**
- **偿还**：窗口关闭（锁释放/COMMIT 完成）时 `debt += borrowed`。此后该任务的虚拟截止期按 `vd_eff = vd + debt` 计算（更晚 → 更不紧急）；每当它因 debt 被推迟（本可被选中却被跳过），按推迟时长冲减 debt。**上个周期多拿的，下个周期用等待还回来**，长期份额不变
- **防逃债**：debt 独立于 vruntime 存储，**不随阻塞/睡眠被 lag clamp 抹除**——持有者不能靠 sleep 逃债；任务退出时残余 debt 核销（有界损失）
- **防博弈**：`debt ≥ D_max` 时禁止再借，直到偿还到阈值以下；借入只能由等待边/COMMIT 事件开通，普通运行永远无法借
- **与 vd 继承的分工**：继承决定“窗口内多急”，借还记账决定“窗口后怎么还”——正交，互可独立消融
- **COMMIT 复用同一账户**：`COMMIT_BONUS` 的提前量等价于借入，COMMIT 极短，debt 极小且快速还清

**边界**：嵌套 latch 须尊重 InnoDB latch 序；提权 slice 有界，临界区意外变长（持 latch 做 IO）时退化为普通公平任务，不长期霸占；debt 机制保证的是**周期级公平**，不承诺逐秒公平——这正是设计意图（短窗口换取临界区加速，长窗口总量守恒）。

### 4.4 in-flight 语义发布：调度只消费“知道是什么”的任务

v2/v3 已证明的原则推广为一条架构约束：**执行侧必须把“这个 tid 当前在干什么”以 job 粒度发布给内核，否则内核无从调度**。

| 语义 | 产生点 | 消费者 |
| --- | --- | --- |
| COMMIT/ROLLBACK 进入/退出 | `handler.cc` 提交钩子 | 微调 A |
| 行锁/latch 等待边 | `thd_wait_begin/end`、sync 等待路径 | 微调 B |
| 执行 job 开始/结束 | executor `thread_job_begin/end` | 解决显式事务 holder 语句间隙 `real_id=0` 的 tid 归属 |
| 语句结构（类型/表集合/事务边界） | reactor parse（纯 CPU、有界、不碰 MDL） | 准入分类、热点 hint、HP 判定 |
| 排队时长（aging） | executor 队列时间戳 | 准入层 + 防饿死 |

明确不做的事：不在 reactor 做 optimize（表打开/MDL/可能阻塞，留 worker）；多语句包/存储过程/parse 异常回退 worker；不发布 THD 指针/执行计划进内核；不要求业务标注。

### 4.5 过载韧性：以排队时间为信号的准入控制（新增）

单纯削开销无法防止崖点，必须有 admission。采用 Breakwater 式闭环，但适配数据库：

- **信号**：executor 的 queue-time（不是队列深度——深度为 0 时 worker 仍可能被抢占，本仓库直方图已证明）
- **动作**：queue-time 超过阈值时，准入层降低发放速率（credit 收缩），新命令在 reactor 侧排队而非涌入执行池；客户端表现为延迟增加而非超时失败
- **公平排队**：准入等待**按连接**做公平轮询，防止单个 Agent 连接的突发扇出挤占全部容量（这是结构问题，不是业务问题）。**租户级配额、泳道、限流不在 DB 内实现，一律上移中间件**（§4.7）
- **迟滞**：credit 收缩快、恢复慢（乘性减、加性增），防振荡
- **重试放大防护**：过载期间对同一连接的重复请求去重提示（协议允许时），状态变量暴露 `admission_deferred` 计数供客户端退避

### 4.7 与中间件的分层：业务资源分配上移

本方案明确不在数据库内做业务间的资源分配与优先级（resource group、租户权重等）。理由：业务语义高频变化，不应固化进最难变更的组件；且 DB 内机制只能控制线程级 nice/绑核，管不到锁、buffer pool、IO 与内存带宽，既不完整也不可持续。

分层职责：

| 层 | 职责 | 机制 |
| --- | --- | --- |
| 中间件 | 谁该来多少：泳道隔离、租户配额、限流、降级、重试退避 | 泳道/独立实例、令牌桶、熔断、SLO 路由 |
| 数据库 | 来了以后始终高效：结构语义微调 + 全局自我保护 | COMMIT 早行、latch 继承、全局 queue-time 准入、按连接公平 |
| 数据库 → 中间件 | 闭环信号出口 | queue-time、goodput、admission_deferred、锁等待计数 |

两个边界必须留在 DB 内：**全局准入**（中间件的延迟/错误信号滞后，无法替代 DB 自我保护）与**结构优先级**（COMMIT/latch/purge 中间件不可见）。resource group 仅为上游兼容保留，本方案不使用、不推荐。

### 4.6 非业务开销的配套削减

- **惊群治理**：沿用 cascade notify；唤醒直放 CPU 时避免无差别 kick（减少 IPI 风暴）
- **false sharing**：调度相关的全局计数器（HP 计数、map 更新计数）按 CPU 分片聚合；等待图的分桶锁沿用叶子锁纪律（`s_wait_mu` 教训：BPF map 写入必须在释放锁之后）
- **交接开销**：廉价短查询允许在提交侧合并完成回投（批量化），减少每命令 2 次切换 + epoll 重注册的固定税（v1 实测该税对点查占 79% 差距）
- **不引入新开销换取调度智能**：所有 hint 只在事件路径写，稳态零成本（对标 UFS hinting ≤1% 的实测）

---

## 五、与 UFS 的关系与差异

| 维度 | UFS | 本方案（EEVDF-Sem） |
| --- | --- | --- |
| 分层 | TS/BG 两层，业务用 SQL `SET task_tier` 打标 | **默认单层公平池，无业务打标**；业务资源分配上移中间件 |
| 基座 | 两层 + 层内 cgroup 权重比例 | EEVDF vruntime/虚拟截止期 + lag 补偿 |
| 普适优化 | 不含（TS 优先是业务策略） | COMMIT 早行、锁/latch holder 条件性 PI，自动生效 |
| 唤醒路径 | TS direct-to-CPU + kick | 局部性优先的唤醒直放，仅在微调场景 kick |
| 锁 hint | wait-event → map，临时提权 BG holder | 同构，但改为 vd 继承 + 深度/时间有界 |
| 过载治理 | 不涉及 | queue-time 准入 + 公平排队 + 重试防护 |
| 适用假设 | 需要人能区分 TS/BG | Agent 生成负载、业务高频变化、无人可标 |

保留 UFS 最有价值的两件东西：**eBPF map 意图通道**和**等待边驱动的条件性提权**；放弃它最不适用的东西：**要求用户手工分层**。

---

## 六、验证方案：负向场景是主验收，打榜负载只做回归

### 6.1 韧性指标（先于实现冻结）

| 指标 | 定义 | 验收口径 |
| --- | --- | --- |
| 崖点位置 | goodput 随 offered rate 开始下降的点 | open-loop 阶梯加压，崖点 ≥ 基线 EEVDF 的 1.2× |
| 越崖行为 | 超过崖点后 goodput 是否保持、有无 hang/线程爆炸 | goodput 不降、无活性故障 |
| 恢复时间 | 负载回落后 p99 回到基线 1.2× 内的时间 | 秒级 |
| 噪声韧性 | B7 式 DB 节点外部 CPU 噪声下 p99 劣化倍数 | ≤2×（v3 实测 1.4–1.7×，保持） |
| 锁活性 | holder parked + waiter > worker 时 COMMIT 完成 | HP 提交且执行，无 hang |
| 公平下限 | 持续高优流下 purge history 不无限增长、无会话饿死 | 保底窗口生效 |
| 干净开销 | 无噪声低争用下吞吐/延迟代价 | ≤5%（v3 水平） |

### 6.2 场景矩阵

1. **Agent 突发（新增，核心场景）**：open-loop 方波负载（0 → 4× 崖点速率，1s 内阶跃），观察准入收缩、排队公平、恢复时间；叠加客户端重试模型，验证无重试放大
2. **B7 外部噪声**、**B1 混合负载**、**B2 优先级反转**（沿用 v3 矩阵作回归）
3. **C1 执行线程耗竭**、**C2 热点 skew 扫描**、**C3 死锁雪崩**、**C4 连接 churn**、**C5 过载膝点**、**C6 争用 soak**（沿用 TESTPLAN 待补项，本方案把它们提升为验收门槛而非待办）
4. **CH-benCHmark**：只作为“无业务打标时 HTAP 不退化”的回归参照，不作为优化目标
5. **打榜负载（sysbench/TPC-C 峰值）**：仅用于确认无回归，不用于宣传
6. **借还公平对抗（新增）**：分钟级长窗测量被提权任务 CPU 份额偏差（<1%）；持续 latch 争用验证 debt 到顶后回落公平；提权后立即睡眠验证 debt 不被 lag clamp 抹除

### 6.3 方法学纪律

沿用仓库已有教训：噪声打在 DB 节点；每配置独立输出目录；≥2 轮取 median 并检查 max 离群；远程状态采集失败不得静默归零；closed-loop 数字不与 open-loop 混用。

---

## 七、落地阶段

| 阶段 | 内容 | 依赖 | 验收 |
| --- | --- | --- | --- |
| v4a | COMMIT 截止期奖励（EEVDF-Sem 最小实现：单池 vruntime + vd bonus） | 已有 commit 注入点 | B2 + B7 回归、干净开销 ≤5% |
| v4b | 等待边 vd 继承（含深度/时间上限、清理路径） | v2 等待图复用 | C1/C3 锁风暴活性 |
| v4c | queue-time 准入 + 公平排队 | 新增准入层 | Agent 突发场景、崖点后移、恢复时间 |
| v4d | 唤醒局部性直放、计数器分片、批量化交接 | 调度器迭代 | IPC/迁移计数、点查交接税下降 |
| v4e | reactor 多分片（线程数可配置，默认 1，fd 哈希分片）+ worker/VIP 数可配置 | conn_handler 参数化 | 高 QPS 下 reactor 不成单点；改配置不改代码 |
| 可选 | 中间件对接：queue-time/goodput/admission 指标出口（泳道、限流由中间件实现） | §4.7 | 业务 QoS 不上 DB 内核；resource group 仅上游兼容保留，不使用 |

降级：无 sched_ext 内核时，①②③⑤ 的用户态部分（HP/VIP、准入、公平排队、cascade）全部独立有效；BPF 半边整体关闭，退化为 EEVDF + 用户态韧性层。

---

## 八、风险与边界

1. **sched_ext idle CPU 快路径**：`select_cpu` 直插 `SCX_DSQ_LOCAL` 会跳过 `ops.enqueue()`，微调逻辑可能失效——v3 已标记为待审计项，v4a 必须先做专门对照
2. **提权泄漏**：所有 map 条目带时间戳并定期清扫；KILL/teardown 路径的清理是正确性测试的必查项
3. **借还账户的会计正确性**：debt 独立于 vruntime，禁止被 lag clamp 抹除（sleep 逃债）；debt ≥ D_max 禁止再借；任务退出核销须有界。会计错误会把“临时提权”退化为“永久优待”或“永久惩罚”，是正确性测试的重点
4. **准入误伤**：queue-time 信号混有 OS 调度等待，阈值须保守、带迟滞；提供完全关闭开关
4. **ABI/内核依赖**：sched_ext API 无稳定 ABI 承诺，锁定内核版本并保留降级路径
5. **本方案不解决**：IO 瓶颈、应用不发 COMMIT、NUMA 分区、AP 内部并行（morsel 级）——这些在各自的层解决，不混入调度层承诺

---

## 参考

- Snowflake. *Industry Benchmarks and Competing with Integrity*. https://www.snowflake.com/en/blog/industry-benchmarks-and-competing-with-integrity/
- Bilodeau-Savaria, Scherzinger. *Unfair by design: eBPF-based scheduling of mixed database workloads*（UFS）. arXiv:2605.02377
- Zhou et al. *OLTP Through the Looking Glass 16 Years Later: Communication is the New Bottleneck*. CIDR 2025
- Zhou et al. *Practical DB-OS Co-Design with Privileged Kernel Bypass*. SIGMOD 2025
- Huang et al. *Low-Latency Transaction Scheduling via Userspace Interrupts*（PreemptDB）. SIGMOD 2025
- Huang et al. *The Art of Latency Hiding in Modern Database Engines*（MosaicDB）. PVLDB 17(3), 2023
- Cho et al. *Overload Control for µs-scale RPCs with Breakwater*. OSDI 2020
- Yu et al. *Staring into the Abyss*. PVLDB 8(3), 2014
- 本仓库：`Docs/coroutine-sched-design.md`（v1–v3 架构与实测）、`Docs/coroutine-sched-research.html`（文献地图）、`scripts/bench/sched/TESTPLAN.md`（负向场景矩阵）
