# coroutine / sched_ext 协同调度：工作交接

> 更新时间：2026-11-11（§5 设计复审，全文归纳简化）。本文是后续 agent 的入口，
> 记录真实代码状态，不把设计目标当作已验证结果。各轮详细日志在
> `/root/src/aliyun/mysql-sched-bench/results/<轮次>/`，本文只留结论与数字。

## 1. 目标（2026-11-11 用户重新定义的验收标准）

- **主目标：TPC-C 与 sysbench OLTP 两个场景下，性能与 9.7 上游分支
  （one-thread-per-connection，下称 V97）基本持平。**
- **同时保持高稳定性**：锁车队、commit 风暴、突发流量、scx 加载失败/被卸载等
  坏场景不崩，尾延迟低。
- 不追求单点 benchmark 打榜；不要求用户给连接手工标注高低优先级；业务间资源
  隔离/限流/泳道属于中间件，不属于数据库内核。

历史设计论述：`Docs/coroutine-sched-design.md`（既有实现与测试方法）、
`Docs/coroutine-sched-resilience-design.md`（v4 方案）、
`Docs/coroutine-sched-resilience-paper.md` / `.html`（论文式文档与 SVG 图）。
**注意：这些文档描述的是 reactor+worker 主线；§5 复审后主线方向已调整。**

## 2. 当前代码状态（分支 `coroutine-dev2`，HEAD `0695f15fa`，均已 push github）

| 组件 | 位置 | 状态 |
|---|---|---|
| scx_db_sched BPF 调度器 | `support-files/scx/scx_db_sched.bpf.c` + loader | 已实现并在云端验证加载/卸载；v5 两池化（无语义任务走 per-CPU FIFO，语义任务走全局 EEVDF DSQ）、有界 vd bonus、PI 深度≤8、TTL+sweeper 防泄漏 |
| sched_ext 桥接 | `sql/sched_ext_bridge.{h,cc}` | tid 注册、lock-holder/TS 发布（gate 引用计数）、latch 等待边镜像 |
| commit 标记 | `sql/handler.cc` 的 `commit_begin/end`（ha_commit_trans 等） | **与连接模型无关**，经典线程模型下同样生效 |
| InnoDB latch 插桩 | `storage/innobase/sync/sync0rw.cc`（4 条等待路径）、`include/ib0mutex.h`、`lock0wait.cc` | mutex+rw-lock 等待边；pass≠0 传递锁与多 reader wait-ex 不发布边（能力边界） |
| coroutine runtime | `coroutine/`、`sql/conn_handler/coroutine_connection_handler.cc` | reactor（epoll+协程前端，fd 分片）+ executor pool（默认 4×CPU worker） |
| D3 reactor parse | 同上 + `sql/sql_parse.cc`（`parse_com_query`/`execute_parsed_command` 两阶段） | 默认 OFF；仅明文未压缩完整 COM_QUERY |
| D5 语句提权 + HP 队列 | `sql/stmt_priority.h` + HP/VIP worker | KILL/victim-rollback/UNLOCK TABLES/RELEASE_LOCK 稀有提权；实测 30s 仅 68 job，几乎闲置 |
| D5b reactor 内联执行 | `coroutine_reactor_exec`（默认 OFF）+ 500µs 预算 + 1s 冷却 | text SELECT 内联；**存在 ~10%/~68% 两种内联相位的性能双峰** |
| PS 内联（8e14d449f） | `can_inline_stmt_execute`/`inline_stmt_execute`，peek 14 字节 + `stmt_map` 查 `Prepared_statement::m_lex` 惰性分类 | COM_STMT_EXECUTE 安全 SELECT 内联；只受 `coroutine_reactor_exec` 门控 |
| admission 准入控制 | `coroutine_admission_queue_time_us` 默认 2000µs，0 关闭 | queue-time EWMA 信用收缩/恢复，下界=池大小；**ON/OFF 收益从未 A/B 证明** |

关键提交（倒序）：`0695f15fa` 资源释放记录 → `a30d6f149`/`ba06ba8ac` 文档 →
`8e14d449f` PS 内联 → `7cc194212` v8 运行中 TS 免 kick → `2a903c485` v7 交接降本 →
`17b1f9420` v6 gate 泄漏修复 → `30260af4b` v5 两池化 → `b10d64916` admission 下界
修复 → `7bffa3131` 协程帧 thread_stack 修复 → `78c7e21f1` admission 无锁化 →
`a06ce9c90` D5b 内联 → `02694b9b3` netpoller 常驻 interest。

## 3. 构建与测试（现行命令）

```bash
cmake --build build --target mysqld mysqltest -j4
cmake --build build_coro --target sql_main -j4
# MTR（默认模式；coroutine_ps_exec 会 skip）
./build/mysql-test/mtr --parallel=1 coroutine_reactor_exec coroutine_stmt_prio \
  coroutine_latch_status coroutine_ps_exec
# MTR（PS 协议模式，coroutine_ps_exec 在此模式下才有真实断言）
./build/mysql-test/mtr --parallel=1 --ps-protocol coroutine_reactor_exec coroutine_ps_exec
# gunit（build/runtime_output_directory/ 下直接执行）
executor-t(13) netpoller-t(8) task-t(5) stmt_priority-t(9)  # 共 35 个，全过为准
```

注意：
- `coroutine_stmt_prio` 在 `--ps-protocol` 下失败是**既有行为**（stash 基线复现
  确认）：D5 分类依赖 COM_QUERY 的 reactor parse，PS 命令不经过该路径。
- mysqltest 的 `enable_ps_protocol` 指令在不加 CLI `--ps-protocol` 时是**空操作**
  （只把开关设回 CLI 标志值）；测试内条件判断用 `$PS_PROTOCOL`。
- MTR 选项文件必须命名 `mysql-test/t/<test>-master.opt`（不读裸 `.opt`）。
- 本机容器环境 MY-015655 警告会使所有 MTR 失败，测试内用
  `call mtr.add_suppression("Server ignores the discovered container restrictions")`。
- mysqltest 变量只在 `eval` 命令中插值，含变量的 SELECT 写 `eval SELECT ...`；
  但**计数器绝对值不要写进 result**（多测试共享 server 实例会累积），用
  `let $d = \`SELECT $a - $b\`; --echo ...` 只输出差值。
- coroutine 模式不能用 `--daemonize`（runtime 线程先于 fork 创建）。
- 已知未修（预先存在）：启动失败 unireg_abort 时 `destroy()`→`ExecutorPool::join()`
  可能段错误；正常 SHUTDOWN/kill TERM 不触发。

## 4. 历史测量归纳（云资源全部已释放，详见各轮 summary）

** redo 配置是轮次间最大的混淆变量**：c4perf 及之前用 100MB（checkpoint 追不上、
吞吐限速），c4opt 起统一 4G。**只能同轮内对比，不要跨轮比绝对值。**
对比口径：V97=pristine 9.7 线程模型；C2=coroutine 无 scx；C4=coroutine+scx。

### 4.1 perf2 轮（2026-11-09，提交 8953c8145..78c7e21f1）
- netpoller 常驻 interest（syscall 减半）、D5b 内联、admission 无锁化落地。
- 文本点查追平反超：306k QPS vs C1text 292k，p99 持平；89% 内联。
- **关键发现**：sysbench 默认走 PS 协议（COM_STMT_EXECUTE），当时 reactor 路径
  只认 COM_QUERY，标准 sysbench 负载下内联从未触发 → 后来才有 §4.6 的 psinline。
- r=1+parse/exec 严重回退（单 reactor 串行）：parse/exec 必须配 ≥4 reactor。
- 结果：`results/perf-20261106/summary-perf2.md`。

### 4.2 vs97 轮（2026-11-09，32 线程、100MB redo、内核 7.0）
- 文本点查 C2 302k–319k vs V97 291k–304k（+5%）；PS 点查 C2 -37%（无 PS 内联）。
- rw：C2 +22%~+45%（注意：小连接数+小 redo 配置，不可外推到 rw256）。
- **scx（C4）当时有点查开销**：PS -39%/文本 -16% vs C2 → 促成 v5 两池化。
- SCX 构建配方：节点 clone sched-ext/scx 用 `scheds/include` + 预生成
  `vmlinux-v7.3-rc5-*.h`（`-DSCX_VMLINUX_H=`）；本机 6.6 内核树头不可用。
- 结果：`results/vs97-20261109/summary-vs97.md`。

### 4.3 paper64 轮（2026-11-09，论文 §6.4 三基准，文本协议）
- rw 伸缩：C2/C4 为 V97 的 85–96%；t128/t256 p95 反超或持平。
- 点查阶梯至 300k 未触膝点；C4 300k 档 p95 3.49ms（v4 调度器成本，v5 后修复）。
- TPC-C：C2 -0.6% 打平，C4 -4.8%。
- 两个缺陷修复（独立 commit）：admission 下界自饿死（t256 崩到 978 TPS）、
  协程帧地址误设 thread_stack（假报 error 1436）。
- 结果：`results/paper64-20261109/summary-paper64.md`。

### 4.4 c4perf 轮（2026-11-10，100MB redo）
- v5 两池化 + v6 gate 泄漏修复后：**C4≈C2（7167 vs 7237），调度器开销归零**。
- **残差 -8% 是 C2↔V97 协程模型差距，且 C2 在 t256 留 ~9% CPU 空闲。**
- 每事务指令数：V97 5.36M / C2 5.88M（+10%）/ C4v6 6.32M（+18%）。
- **p99：C4 215.4ms vs V97 397.4ms（-46%）——语义调度收益的主证据。**
- TPC-C：V97 2247.7 / C2 2179.2 / C4-v6 2137.9（-4.9%，库增长对末位 C4 略不利）。
- RES-IPI 16.7k/s（V97 仅 4/s）；迁移 C4=2×C2。
- 部署坑：`rsync -a` 保留旧 mtime 会让 make 跳过重建（bridge 静默没编进去），
  同步后 touch 或 `--no-times`。
- 结果：`results/c4perf-20261110/summary.md`。

### 4.5 c4opt 轮（2026-11-10，4G redo 统一起点）
- 保留 v7（指令/事务 -9.1%）与 v8（运行中 TS 免 kick）两个提交。
- **最终 rw256：V97 11846.82/54.83ms；C4 10342.88/74.46ms（-12.65%）**；
  PS rw256 -18.57%；TPC-C goodput -17.17%（与 4.4 轮 -4.9% 矛盾，库状态漂移，
  需干净库重测）。
- **否决清单（勿重开）**：worker 数扫描（64 最优，128/48/32 均更差）、100ms 内联
  冷却（-13%）、dispatch 空池 `nr_queued` 预检（433→635ns）、worker 硬绑核
  （迁移降但吞吐 -15%）、DML/COMMIT 上 reactor 内联（行锁/redo 阻塞风险）。
- 结果：`results/c4opt-20261110/summary.md`。

### 4.6 psinline 轮（2026-11-10，提交 8e14d449f）
- PS 点查 300k：C4-old **复现性失败（event queue full）**→ C4-new 300002/0.26ms
  追平 V97（299916/0.20ms）；200k 档 p99 从 0.45ms 降到 0.21ms。
- PS rw256（60s 同轮）：10280.71→10660.97 TPS（+3.7%）、p99 -5.3%；对 V97 仍
  -18.9%。text rw256 无回归（同轮 10366 vs 10334）。
- **内联预算扫描（重要负结果）**：500µs→7.7% 内联/p99 87.6ms；2ms→63%/121ms；
  100ms→64%/161.5ms。饱和下把多数读搬进 8 个 reactor 使 reactor 成为瓶颈——
  **继续提高内联占比是错误方向**。
- 结果：`results/psinline-20261110/summary.md`（部分原始日志被覆盖，目录内有
  校验过的重构记录）。

### 4.7 云资源与备份现状（2026-11-11）
- **所有实例已释放**（11-10 11:38Z 手动 DeleteInstance 两台；更早轮次实例的
  auto-release 兜底时间均已过）。不要假设任何云端环境还在。
- 二进制备份（SHA 已核对）：`/root/src/aliyun/mysql-sched-bench/binaries/`
  `mysqld.psinline[.zst]`（4d78baf9，= 8e14d449f 的 x86_64 云端构建）、
  `scx_db_sched[.zst]`（97c5dad7）。旧二进制可从 ba06ba8ac 重建。
- 本机是 **aarch64，云机是 x86_64**：本地 mysqld 不能直接上传运行；源文件 rsync
  到云端增量构建（2 个 TU + 链接约 5 分钟），或用备份二进制覆盖。
- 环境重建手册：`/root/src/aliyun/mysql-sched-bench/ENVIRONMENT.md`（购买 →
  bootstrap → run_remote.sh → 基准全流程）。

## 5. 设计复审（2026-11-11）：为什么没持平、怎么办

### 5.1 核心结论

1. **三轮实测证明价值的是 scx 语义调度（p99 -46%），coroutine 线程池本身的
   价值从未被证明**——所有测试 ≤256 连接，正是池模型没有优势的区间。
2. **rw256/TPC-C 差距的根因是每命令两次跨线程交接的延迟税**：closed-loop 下
   吞吐 = 连接数 / 事务延迟，交接拉长每条命令延迟，CPU 有空闲也掉吞吐
   （C2 在 t256 有 ~9% CPU 空闲是直接证据）。加 worker、调预算都修不了。
3. **达到持平最省事的路径：经典 one-thread-per-connection + scx_db_sched
   （下称 C5）**，从结构上消掉交接税，保留已验证的 commit/lock/latch 收益。
   桥接几乎全部可复用，改动只有几十行。

### 5.2 当前设计的四个根本问题

1. **closed-loop OLTP 是延迟受限，不是 CPU 受限**。V97 每条命令一次内核唤醒；
   C4 每命令多出约 5~8 次 syscall（MSG_PEEK、FIONREAD、condvar/futex、eventfd
   post、epoll_ctl 重臂）+ 1 次额外线程唤醒，每条命令多 ~10-20µs，事务延迟
   等比放大，吞吐下降且 CPU 空闲。
2. **reactor 内联执行是不稳定的补偿器**：消除交接但带来性能双峰（同二进制同
   配置 10366 vs 9394 TPS）、reactor 阻塞风险（内联语句等 MDL 时该 reactor 上
   ~32 连接全部陪葬）、双协议双路径的复杂度。
3. **两个调度器各管一段**：用户态 FIFO+HP/VIP+admission 与 scx 互相看不见。
   HP 队列 30s 仅 68 job、VIP worker 长期闲置；admission 有振荡史且 ON/OFF
   收益从未证明，却在饱和时 30s defer 30 万次。
4. **scx 侧仍有固定成本**：每命令桥接 syscall、16.7k/s RES-IPI（V97 为 4/s）、
   迁移 2×C2。只要 reactor+pool 在，这些消不干净。

### 5.3 组件处置

| 组件 | 处置 | 理由 |
|---|---|---|
| scx_db_sched + 桥接 + commit/latch 钩子 | **保留，核心资产** | C4≈C2 开销归零；p99 -46%；与连接模型无关 |
| coroutine 池（含 D3/D5/D5b/PS 内联） | **降级为可选前端** | 只剩交接税，无独立收益证据 |
| HP 队列 + VIP worker | **删除** | 30s 68 job，语义加速由 scx 承担 |
| admission control | **默认 OFF** | 坏场景收益未证明，有振荡史 |
| fd registry/KILL 处理 | pool 保留则保留 | 有测试覆盖 |

### 5.4 推荐主线 C5：经典线程模型 + scx

```
连接 = 内核线程（9.7 原生，零交接税）
  ├─ do_command 前后：sched_ext::thread_job_begin/end（新增，~10 行）
  ├─ 连接线程启动/退出：thread_start/thread_end（新增，几行）
  ├─ handler.cc commit_begin/end ── 已有，直接生效
  └─ sync0rw latch 等待边 ───────── 已有，直接生效
                ↓
        scx_db_sched（唯一调度器）：commit/锁持有/latch 加速 + EEVDF 公平
```

- **性能持平是构造性保证**（执行路径=V97 + 已验证≈0 的 BPF 成本）。
- **p99 收益大概率可移植**（commit VIP/lock-holder/latch 边都是 per-thread
  标记）——但这是**待验证假设，不是既成事实**。
- **回退天然干净**：scx 卸载/加载失败 = 退回原生 EEVDF = V97 行为。
- pool 唯一可能不可替代的场景是超高连接数/突发洪水（线程内存、惊群），
  **从未测过**；要么补测（2k~10k 连接突发到达）证明价值，要么承认现状。
- 若坚持 pool 主线，唯一正确的方向是 cheap-hop（worker 侧直接 epoll_ctl 重臂
  去掉 eventfd post、无锁提交队列去全局 mutex、删 HP/VIP、admission 默认关），
  工作量大且只能逼近持平，**不推荐作为主攻方向**。
- D3/D5b/PS 内联随 pool 降级**不是返工浪费**：桥接、BPF、latch 插桩、测试方法
  全部复用；"池模型无独立收益"只有测过才知道。

### 5.5 下一步验证顺序（每步都有独立信息价值）

1. **零改动控制**：现有 C4 二进制加 `--thread-handling=one-thread-per-connection`
   跑 rw256/TPC-C vs V97。隔离 pool 之外的改动（D6 插桩等）是否有成本。
2. **C5 最小挂钩**（几十行）+ scx loader：rw256（text/PS）、TPC-C、点查
   ratescan。验证吞吐持平 + p99 收益是否移植。
3. **配对 perf**：V97 vs C5 的 instructions/txn、ctx、migrations、IPI——确认
   交接税消失、量化 scx 真实残差。
4. **坏场景回归**：scx 中途 kill/detach、锁车队（热点行+长事务）、commit 风暴、
   open-loop 洪峰；pool 专属场景（高连接数）单独补测。
5. TPC-C 用**每配置干净库或交错顺序**重测，消除 §4.4（-4.9%）与 §4.5（-17.2%）
   的矛盾。

### 5.6 坏场景稳定性现状映射

| 坏场景 | 现有机制 | 证据状态 |
|---|---|---|
| 锁车队/热点行 | lock-holder boost + latch 边 | 已证（rw256 p99 -46%） |
| commit 阻塞 | commit VIP | 已证（TPC-C 分项 p99 改善） |
| scx 失效/卸载 | gate 引用计数 + TTL + sweeper | v6 修复后已验证回退 |
| open-loop 高压 | 点查 300k 达标 | 已证（V97 本身也达标，非池独有收益） |
| 突发洪峰/过载膝点 | admission + 池上限 | **未做 ON/OFF 对照，收益未证** |
| 病态语句 | 内联预算+冷却（仅 pool） | 有 MTR；但双相位说明仍不可控 |
| 超高连接数 | pool 多路复用 | **从未测过（所有测试 ≤256 连接）** |

## 6. 其他已知边界与操作注意事项

- D3 让 slow-query 起点和 `thread_running` 覆盖 reactor→worker 排队时间，是有意的
  语义变化。
- 完整大 SQL 可能让 reactor parse 较久；不要静默取消现有回退。
- 业务 priority/resource-group 硬映射已移除是有意设计，不要恢复成永久分层。
- `Docs/preemptdb-sigmod2025.pdf` 和 `/root/src/preemptdb` 是参考资料。
- mysql CLI `--force -e` 在报错后会发 COM_QUIT；驱动错误风暴场景改用 mysqltest。
- 云 benchmark 遵守根目录 `AGENTS.md`：结果先拉回并校验，实例测试后立即释放；
  Spot 实例构建产物及时备份（本地 `binaries/` 模式）。
- perf-stat time-enabled 是多核求和，归一化窗口 = 值/核数；别用 bpftool 窗口去除。
