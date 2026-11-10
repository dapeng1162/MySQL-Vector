# coroutine / sched_ext 协同调度：工作交接

> 更新时间：2026-11-10（§4.7 paper64、§4.8 c4perf、§4.9 c4opt）。本文是后续 agent 的入口，记录真实代码状态，不把设计目标当作已验证结果。

## 1. 工作背景与目标

目标不是为单一 benchmark 冲峰值，而是在高并发、锁等待、外部 CPU 干扰和 AI Agent 突发流量下保持性能韧性：

- 以公平调度为基座，优先完成 COMMIT/ROLLBACK，帮助 latch holder 尽快退出临界区。
- 用有界奖励和 borrowed/debt 借还账户避免永久 VIP、饥饿和“睡醒后债务清零”。
- reactor 负责连接、可配置多分片 epoll、完整报文预检和可选的 SQL parse；worker 负责执行。
- worker/VIP 数量可配置；queue-time 准入控制避免执行池被突发流量打穿。
- 业务间资源隔离、限流和泳道属于中间件，不要求用户给数据库连接手工标注高低优先级。

详细设计与 SIGMOD 风格论述见：

- `Docs/coroutine-sched-design.md`：既有实现、历史数据和测试方法。
- `Docs/coroutine-sched-resilience-design.md`：v4 方案。
- `Docs/coroutine-sched-resilience-paper.md` / `.html`：论文式文档和 SVG 图。

## 2. 本次提交包含的代码

### A. sched_ext BPF（`support-files/scx/scx_db_sched.bpf.c`）

已写代码：A1/A2、B1/B2/B3、C1/C2/C3。

- `select_cpu` 不再通过 `SCX_DSQ_LOCAL` 绕过统一 enqueue。
- 三层硬优先级 DSQ 改为单池 virtual deadline 模型；COMMIT 和 TS 使用有界 vd bonus。
- 唤醒 lag 有 100ms clamp；holder slice、bonus、borrowed/debt 均有界。
- PI 深度上限为 8，holder boost 带时间戳清扫，任务退出时清理借还账户。
- `rg_prio` map 仅为桥接 ABI 保留，调度逻辑不再使用它。

### B. sched_ext 桥接与 InnoDB

已写代码：C4 和后台线程标注。

- `sql/sched_ext_bridge.{h,cc}`：latch 等待边、std::thread::id 到 tid 的注册。
- `storage/innobase/include/ib0mutex.h`：mutex spin 后进入等待前上报 holder，等待结束清除。
- 目前只覆盖 mutex 路径，rw-lock 等待边尚未插桩。

### C. coroutine runtime

已写代码：D1/D2/D4，并补了一个取消路径的 credit 泄漏。

- `coroutine_executor_threads`：默认 0，auto 为 `max(4 * hardware_concurrency, 16)`；显式配置不再乘 4。
- `coroutine_executor_vip_workers`：默认 1。
- `coroutine_reactor_threads`：默认 1，可配 1–64；连接按 `fd % reactor_count` 固定分片。
- `coroutine_admission_queue_time_us`：默认 2000，0 关闭；EWMA 超阈值乘性减 credits，低于 1/4 阈值加性恢复。
- `Coroutine_admission_deferred` 等状态变量位于 `sql/mysqld.cc`。
- 命令循环先检查 `NET.remain_in_buf` 和 VIO read buffer：认证/read-ahead 已读入用户态的命令直接提交，避免内核 fd 无数据时在 epoll 上死等。

### D. D3：reactor parse 前移

代码位置：

- `sql/conn_handler/coroutine_connection_handler.cc`：`can_parse_on_reactor()`、`parse_on_reactor()` 和命令循环。
- `sql/sql_parse.cc` / `.h`：`do_command_impl()`、`parse_com_query()`、`execute_parsed_command()`；`dispatch_command()` 和 `dispatch_sql_command()` 支持 parse-only / already-parsed 两阶段。

行为：

- 新开关 `coroutine_reactor_parse`，**默认 OFF**，可动态开启。
- 只对明文、未压缩、socket 中完整到达、非多 packet 的 `COM_QUERY` 启用；TLS、压缩、半包、其他命令回退原 worker `do_command()`。
- 用 `MSG_PEEK|MSG_DONTWAIT` 读取 4 字节 header + command byte，再用 `FIONREAD` 检查完整 packet，不在 reactor 等待网络。
- reactor 完成收包、命令初始化、AST parse；`Parser_state` 和 `COM_DATA` 保存在连接协程帧中，worker 继续 rewrite/execute/多语句/secondary-engine/PSI 收尾。
- 首条语句长度截断延迟到 worker，避免多语句被截断；worker 不重复初始化命令、不重复 parse。
- `dispatch_command` 用独立的 `parse_only_state`（reactor 输出 AST）和 `pre_parsed`（worker 续执行）表示两种模式，禁止复用同一参数；parse-only 真正完成时显式设置 `parse_completed=true`，提前 `done` 保持 false。不能用 PSI/digest 指针推断 ready（PSI 可关闭）。
- `Coroutine_reactor_parsed` 记录成功移交给 worker 的 reactor parse 数量。

**D3 已完成构建和真实客户端冒烟；默认 OFF，仍属实验特性，不等同于生产成熟度或高并发验收。**

## 3. 验证状态

- 最终 `cmake --build . --target mysqld -j6` 通过，日志 `/tmp/mysqld-build-d3f.log`；`component_reference_cache`、`mysqladmin` 目标也已构建。
- `task-t` 5/5、`netpoller-t` 6/6、`executor-t` 10/10 已通过。
- `coroutine_reactor_parse=OFF` 基线 SELECT 正常、counter 为 0；切为 ON 后 SELECT 正常且 counter 增长，最终冒烟计数为 73。
- DDL/DML、显式 COMMIT/ROLLBACK、连续多语句结果正确；回滚行未残留。
- 同一连接语法错误后 `SELECT 42` 成功；TLS、压缩协议和 COM_PING 回退正常；12 个顺序新连接均成功。
- 最终实例通过 SQL `SHUTDOWN` 干净退出，最后一轮无新增崩溃日志。历史调试日志中曾有模式混淆导致的空 AST 崩溃，已通过分离 `parse_only_state`/`pre_parsed` 修复。
- BPF：编译链已在本机完整验证（见 P1 BPF 节），加载验证需专门环境（本机内核 6.6 无 SCHED_CLASS_EXT、无 BPF JIT）；高并发、公平性和性能数字未在本轮复测。

标准构建：

```bash
cd /root/src/mysql-server/build
cmake --build . --target mysqld -j6
```

冒烟数据目录 `/tmp/coro_d3/data` 已用 `--initialize-insecure` 初始化。启动示例：

```bash
cd /root/src/mysql-server
build/runtime_output_directory/mysqld --no-defaults \
  --datadir=/tmp/coro_d3/data --user=root \
  --socket=/tmp/coro_d3/mysqld.sock --port=33306 \
  --thread-handling=coroutine-epoll \
  --coroutine-reactor-threads=2 --coroutine-executor-threads=8 \
  --coroutine-reactor-parse=ON \
  --skip-log-bin --innodb-buffer-pool-size=256M \
  --log-error=/tmp/coro_d3/error.log --pid-file=/tmp/coro_d3/mysqld.pid
```

用 shell 后台运行，不要加 `--daemonize`：当前 `Connection_handler_manager::init()` 先创建 runtime 线程，daemonize 后 fork 会使子进程丢失这些线程。只构建 mysqld 后若提示缺少 `component_reference_cache.so`，执行 `cmake --build . --target component_reference_cache -j6`。

至少验证：

1. `SELECT 1`、DDL/DML、`START TRANSACTION; ...; COMMIT/ROLLBACK`。
2. 语法错误后同一连接继续执行；`SELECT 1; SELECT 2` 多语句结果完整。
3. `mysqladmin ping` 等非 COM_QUERY 回退；TLS/压缩连接回退。
4. `SHOW GLOBAL STATUS LIKE 'Coroutine%'`：`reactor_parsed` 增长、reactor/worker 配置符合预期。
5. `coroutine_reactor_parse=OFF` 时 counter 不增长；切换 ON 后只影响后续命令。
6. 检查 error log 中 assert、PSI/digest 跨线程异常和崩溃。

## 4. 剩余工作（建议顺序）

### P0：收尾验证（已完成）

- 完整编译通过；真实 mysql 客户端事务、多语句、`mysqladmin ping` 回退、`SHOW GLOBAL STATUS LIKE 'Coroutine%'` 均验证通过。
- gunit：`task-t`、`netpoller-t`、`executor-t` 全部通过。

### P1：D5 优先语句谱系（已实现、已验证）

- 分类器：`sql/stmt_priority.h`（header-only 纯函数，`stmt_priority::classify()`，Reason ∈ {Kill, VictimRollback, UnlockTables, ReleaseLock}），只依赖 `my_command.h`/`my_sqlcommand.h`，无 THD 依赖。
- 接入：`sql/conn_handler/coroutine_connection_handler.cc` —— reactor parse 后调用分类器；命中则走 HP 队列（VIP worker 优先消费），并有原子计数器。
  - KILL QUERY/CONNECTION 提权；死锁/超时 victim 的下一条 COMMIT/ROLLBACK/XA 提权（victim 语义 = 仅下一条语句，任何中间成功语句清除标志）；UNLOCK TABLES（需 `thd->open_tables != nullptr`）提权；SELECT list 顶层含 RELEASE_LOCK/RELEASE_ALL_LOCKS 时提权（子查询/WHERE 中不检测，已文档化）。
  - 明确不提权 `ROLLBACK TO SAVEPOINT`（不一定释放全部行锁），有负例测试。
- 可观测：`SHOW GLOBAL STATUS` 新增 `Coroutine_stmt_prio_kill/_victim_rollback/_unlock_tables/_release_lock`。
- 测试（全部通过）：
  - gunit：`unittest/gunit/coroutine/stmt_priority-t.cc`（9 个用例）。
  - MTR：`mysql-test/t/coroutine_stmt_prio.test`（`main.coroutine_stmt_prio`，含 `--record` 录制与不带 `--record` 重放均通过）。
- 性能测试（脚本已写好、未实测，留待专门环境）：`scripts/bench/sched/run_b8_stmt_prio.sh`（victim 风暴 + KILL 风暴 + sysbench 护栏三阶段），见 `scripts/bench/sched/TESTPLAN.md` 的 B8 行。
- 已定论、勿重开：用户态 HP 队列是严格优先级，只有“稀有且全局有益”的语句可用；COMMIT 泛化提权会破坏有界公平，不做。

### P1：D6 rw-lock latch 等待边插桩（已实现、已验证）

- Bridge 重构（`sql/sched_ext_bridge.h/.cc`）：latch 等待边从“仅 SCX 构建可用”移到**始终编译**的用户态半区——`latch_wait_begin(owner, Latch_kind)` 维护 `std::thread::id` 键控的 waiter→holder 镜像 + 每族（Mutex/RwLock）waits/edges 计数器；`HAVE_SCX_SCHEDULER` 构建下再由 `scx_publish_latch_edge/scx_clear_latch_edge` 写 tid 键控 BPF map（语义与原实现一致）。`ib0mutex.h` 调用点同步改为带 `Latch_kind::Mutex`。
- 插桩点（`storage/innobase/sync/sync0rw.cc`，全部 4 个 `sync_array_wait_event` 等待路径）：
  - S 等待（`rw_lock_s_lock_spin`）、X 等待（`rw_lock_x_lock_func`）、SX 等待（`rw_lock_sx_lock_func`）：holder = `recursive` 标志保护下的 `writer_thread`（pass≠0 的异步 IO 传递锁无法可靠解析，不发布边，只计数）。
  - wait-ex（`rw_lock_x_lock_wait_func`，等 reader 排空）：`lock_word == threshold-1` 即恰好一个 reader 时，用 `reader_thread.recover_if_single()` 解析该 reader；多 reader 无单点 holder，不发布边。
  - 已知竞态（已文档化）：holder 读取天然 TOCTOU，边只活到 `latch_wait_end()`，误解析的 id 通常不在注册表中被安全跳过。
- 可观测：`SHOW GLOBAL STATUS` 新增 `Coroutine_latch_wait_mutex/_mutex_edge/_rw/_rw_edge`（任何线程模式可用，值掩码录制）。
- 测试（全部通过）：
  - gunit：`unittest/gunit/innodb/sync0rw-t.cc` 新增 `rw_lock_latch_wait_edges`——5 把锁覆盖 X 阻塞 X / 单 reader wait-ex / 双 reader 无边 / X 阻塞 S / X 阻塞 SX，验证镜像边、计数增量、解锁后镜像清空。注意 `sync_check_init(N)` 的 N 要 ≥ 并发等待者数（默认 4 槽，5 个等待者会触发 sync0arr 断言）。
  - MTR：`mysql-test/t/coroutine_latch_status.test`（`main.coroutine_latch_status`，验证 4 个状态变量管道；录制与重放均通过）。
  - 回归：`task-t/netpoller-t/executor-t/stmt_priority-t`（5+6+10+9）与 `main.coroutine_stmt_prio` 均通过。
- 性能测试（脚本已写好、未实测）：`scripts/bench/sched/run_b9_latch_edges.sh`（uniform vs 单表热点 + 插桩开销护栏 + PFS rwlock 一致性对照），见 TESTPLAN B9 行。
- 本地功能冒烟（2026-11-06，coroutine-epoll + reactor-parse，非 SCX 构建）：32 线程 pareto update_index、8 表 × 1 万行、30s —— `Coroutine_latch_wait_rw` 6→1583、`_rw_edge` 6→1126、`_mutex` 74→2248、`_mutex_edge` 68→618，edge ≤ waits 不变式成立，负载正常（5.5k TPS、p99 40ms）。
- 未验证：`HAVE_SCX_SCHEDULER` 构建下 `scx_publish_latch_edge` 路径的**运行**（编译已通过，见下方 BPF 节；本机运行内核无 sched_ext，只能在专门环境验证）。

### P1：BPF 编译与加载验证（编译已全通，加载待专门环境）

- **已修复的编译问题**：
  - `sql/sched_ext_bridge.cc`：`scx_publish_latch_edge/scx_clear_latch_edge/latch_owner_tid` 原定义在 `namespace sched_ext` 直接命名空间，与匿名命名空间声明不匹配（"declared static but never defined"，链接必失败）；已移入 SCX 半区匿名命名空间块。用 `build/compile_commands.json` 提取的命令加 `-DHAVE_SCX_SCHEDULER` 单独编译验证通过，默认构建 `cmake --build . --target sql_main mysqld -j6` 通过。
  - `scx_db_sched.bpf.c`：`scx_bpf_dsq_move_to_local(EEVDF_DSQ)` 需补第二个 enq_flags 参数（当前 compat 宏签名）。
- **BPF 侧编译链（本机已通，零警告，产物 938KB）**：
  - vmlinux.h：scx repo 预生成版，如 arm64 用
    `https://ghfast.top/https://raw.githubusercontent.com/sched-ext/scx/main/scheds/vmlinux/arch/arm64/vmlinux-v7.3-rc5-g6ebdea0d0fb8.h`
    （4.8MB；`scheds/vmlinux/vmlinux.h` 和 `arch/arm64/vmlinux.h` 只是指针文件，不要用）。
  - BPF 侧头用 **scx main 分支** `scheds/include/scx/*.h` + `scheds/include/bpf_arena_common.bpf.h`（注意后者在 scheds/include/ 下，不在 scx/ 下）。**不能用本机 `/root/src/kernel/tools/sched_ext/include`（6.6 补丁集）**：缺 `scx_bpf_dsq_move_to_local` 两参兼容宏。
  - 编译：`clang -g -D__TARGET_ARCH_arm64 -mlittle-endian -I<headers> -I<vmlinux-dir> -Wall -Wno-compare-distinct-pointer-types -O2 -mcpu=v3 -target bpf -c ...`（clang18 在 `/opt/openEuler/llvm-toolset-18/root/usr/bin/clang`）。
  - skeleton：`bpftool gen object` 三步 + diff + `gen skeleton ... name scx_db_sched`。
- **loader 双路径**（`scx_db_sched_main.c`）：本机 bpftool 7.2 < 7.4，旧 skeleton 无 `skel->struct_ops` 段——`#ifdef SCX_SKEL_HAS_STRUCT_OPS` 走 `SCX_OPS_OPEN/SCX_OPS_LOAD` 宏，`#else` 走 `scx_db_sched__open()` + 手动 `rodata->uei_dump_len = UEI_DUMP_DFL_LEN; RESIZE_ARRAY(skel, data, uei_dump, ...)` + `scx_db_sched__load()`（内核文档明确 hotplug_seq=0 跳过热插拔检测；两种布局 `SCX_OPS_ATTACH` 都走 `maps.__ops_name`）。CMake 已加 bpftool 版本探测，≥7.4 自动定义 `SCX_SKEL_HAS_STRUCT_OPS=1`。
  - loader 用户态头用 **torvalds/linux v6.12 的 tools/sched_ext/include/scx**（v6.12 的 `SCX_OPS_LOAD/ATTACH` 兼容旧 skeleton，且其 `user_exit_info.h` 要求 -I 路径里有 vmlinux.h）。已知无害警告：v6.12 `common.h` 的 `RESIZE_ARRAY` 有 `-Wdiscarded-qualifiers`（main 分支头已修）。
  - loader 已本机编译链接通过，`--help` 正常，运行时正确报 "sched_ext is not available"（符合预期）。
- **剩余（专门环境，如云端 kernel 7.0.0-31 Ubuntu）**：
  - 实际加载：`bpftool` 7.2 环境跑 legacy 路径；≥7.4 环境跑 struct_ops 路径（新路径分支本机无法验证，代码与 upstream 宏一致，风险低）。
  - 正式 CMake 构建：`WITH_SCX_SCHEDULER=ON` + 指向一套新头（BPF/loader 同源即可），CMakeLists 的版本探测与 WARNING 已就位（standalone configure 冒烟通过）。
  - 加载后跑 B5 自死锁回归（SCX 构建下首次行锁等待）与 B8/B9/C 系列实测。
- 已否决路线（勿重试）：`pahole -J` 离线转 BTF（6.6 补丁集树 DWARF 不完整）；全量 vmlinux.h include 进 loader（glibc 类型冲突）；repo 内找新版 bpftool（最高 7.2）。
- GitHub 直连不通时使用 `https://ghfast.top/` 前缀代理；只支持 raw.githubusercontent.com，api.github.com 与 github.com 页面不行。

### P2：文档与完整测试矩阵

- 将 v4 design/paper/html 中 D1–D4、D3 的状态从“设计”改成真实的“已实现且已验证”或“实现实验性、默认关闭”；不要只改一份。
- 负向测试脚本已全部写好（`scripts/bench/sched/`，语法检查通过、未实测）：B8（D5 语句提权风暴）、B9（D6 latch 边）、C1（执行线程耗竭）、C2（热点争用扫描）、C3（死锁雪崩）、C4（高连接 + churn）、C5（过载膝点爬坡）、C6（争用 soak + 泄漏检查）；D6 计数器已做本地功能冒烟。实测按 TESTPLAN 落地计划在专门环境执行。
- 跑 TPCC、sysbench OLTP，以及文档 N1–N12 负向测试：锁风暴、外部干扰、突发、重试、悬崖点、恢复、fairness/debt 等。
- 历史数字只能引用 `Docs/coroutine-sched-design.md` 中已有实测；v4 新机制没有新 benchmark 前不得写性能提升结论。

## 4.5 perf2 轮（2026-11-09，已提交 8953c8145..78c7e21f1，已 push github/coroutine-dev2）

C2 vs C1 点查差距的归因（每查询 2 次跨线程交接 + ~4 次 epoll_ctl + 2 次 g_adm_mu 全局锁）
落地为三个独立 commit，本地验证全过：

1. `02694b9b3` netpoller 常驻 interest：m_fds 条目连接生命周期内常驻，事件到达只清
   waiter，重臂走 EPOLL_CTL_MOD（syscall 减半），取消结果经 bool*（指向挂起协程帧）
   传递，去掉每次等待的 shared_ptr 堆分配。gunit netpoller-t 8/8（新增 2 个：条目
   常驻 + cancel 后重等）。
2. `a06ce9c90` reactor 内联执行（D5b）：`coroutine_reactor_exec`（动态，默认 OFF）+
   `coroutine_reactor_exec_budget_us`（默认 500µs，超预算该连接冷却 1s 回 executor
   路径）。分类器 `parsed_select_is_reactor_safe`：SQLCOM_SELECT、表锁 ∈ {TL_IGNORE,
   TL_READ_DEFAULT, TL_READ}（**注意：parse 期普通 SELECT 是 TL_READ_DEFAULT，不是
   TL_READ**——首版写成 TL_READ 导致表查询全被拒绝，mtr 录制结果全 0 才发现；
   FOR UPDATE 在 contextualization 期改写为 TL_WRITE 被排除）、顶层 select list 排除
   get_lock/sleep/benchmark/release_lock/release_all_locks。计数器
   `Coroutine_reactor_exec/_slow`。mtr `coroutine_reactor_exec`（精确 delta 断言）。
   **admission credit 在 parse 前已取，内联后必须 admission_job_done() 归还**。
3. `78c7e21f1` admission 无锁化：outstanding/credits 原子化，await_ready 纯 CAS，
   EWMA/credits 重算每线程 1/8 采样锁一次。

**关键发现（本轮最重要的结论）**：sysbench 1.0 oltp_* 默认走**预处理语句协议**
（COM_STMT_EXECUTE=0x16），`can_parse_on_reactor` 只接受 COM_QUERY，因此 reactor
parse/exec 在标准 sysbench 负载下从未触发（2790 万命令只 parse 了 25.8 万、exec 3 次；
单线程 3.9 万查询同样只有 3 次）。strace 证实 PEEK 每查询都成功返回 5 字节、
FIONREAD 一次未到——失败点在 command 字节判定。验证用 `--db-ps-mode=disable` 强制
文本协议（run_perf.sh 已加 PS_MODE 变量）。

**后续 P1（重要）**：reactor 路径支持 COM_STMT_EXECUTE——按 com_data 里的 stmt_id 查
prepared statement 的 LEX 做分类，执行仍需复用 execute_parsed_command 语义（二进制
协议参数绑定在 sql_prepare.cc 深处，需要把 execute/read-param 与 dispatch 解耦）。
生产驱动（JDBC prepared、代理、多数 ORM）以 PS 为主，不支持它则内联优化对真实
负载无效。

已知问题（预先存在，与上述 commit 无关，未修）：启动错误导致 unireg_abort 时
`Coroutine_connection_handler::destroy()` → `ExecutorPool::join()` 在 pthread_join 内
段错误（正常 SHUTDOWN/kill TERM 不触发；本地用 `--user=root` 缺失或残留 socket lock
可复现，栈见 results/perf-20261106 各 FATAL log）。建议在 destroy() 对 join 失败做
防御或排查 worker 线程早期退出路径。

perf2 环境：16c c9i.4xlarge（db, i-0jl0bdgs2kmerj7729ul）+ 8c c9i.2xlarge（loadgen,
i-0jlhposud13890bffudm）双 spot，alinux4；结果目录 `results/perf-20261106/`
（C1/C2v3r1/C2v3r8 为 PS 协议，C1text/C2v3r1text/C2v3r8text 为文本协议）。
教训：alinux4 loadgen 的 sysbench 必须链接 mysql-devel 的 libmysqlclient（mariadb
强制 TLS 报 2026；mysql_config 冲突需先 dnf remove mariadb-devel）。

perf2 最终结果（详见 results/perf-20261106/summary-perf2.md）：

- **点查（文本协议 + r=8 + parse + 内联 exec）追平并超过 one-thread**：
  306k QPS vs C1text 292k，p99 0.14–0.16ms 持平。perf1 的 158–201k 差距消除。
  计数器：100% 查询 reactor parse，89% 内联执行，slow 0.005%。
- r=1 + parse/exec 是回退（53–58k，单 reactor 串行 ~17µs/查询）——
  `coroutine_reactor_parse=ON` 必须配 ≥4 reactor，属配置约束。
- rw（文本协议 r=8）2768–3354 TPS，比 PS 协议 C2v3r8（2136–2160）提升 30–55%，
  但仍落后 C1text（4505–5344）25–40%：DML 全部下放 executor，submit→run
  avg 12µs→101µs。**后续方向：DML reactor 判定放宽（单行 PK UPDATE 内联）
  或交接路径削减**。
- PS 协议下三项优化不触发亦无回退（与 perf1 持平）。

## 4.6 vs97 轮（2026-11-09，pristine 9.7 vs coroutine-dev2+SCX，资源保留中）

用户指定的对比：pristine 9.7（008e09c28，one-thread-per-connection，即 9.7 默认
模型，无内置线程池）vs 当前 reactor+worker（coroutine-dev2 9491fe905，
reactor×8 + parse + inline exec）± scx_db_sched。双 mysqld（/opt/mysql-sched-97
与 /opt/mysql-sched-scx）共用同一 datadir 轮流启动。Ubuntu 26.04 内核
7.0.0-38（sched_ext 可用），16c db + 8c loadgen 双 spot，sysbench 32 线程
60s×2，PS/文本协议各一遍。报告：
`scripts/bench/sched/results/vs97-20261109/report.html`（gen_vs97_report.py），
原始数据同目录 `<config><proto>/`，另有 aliyun 侧
`results/vs97-20261109/summary-vs97.md`。

核心结果（60s×2 均值口径见报告；区间为两轮原始值）：

- **文本协议点查：C2 302k–319k vs V97 291k–304k，追平并反超**（+5%），p99
  持平 0.16–0.20ms；100% reactor parse、87% 内联执行。perf2 结论在内核 7.0
  复现且更优。
- **rw：coroutine 模型全面占优**。PS 下 C2 5163–6404 vs V97 4236–4405 TPS
  （+22%~+45%），p99 98–105ms → 27–72ms；文本协议同样领先。**与 perf2
  （alinux4/6.6）C2 rw 落后的结论相反——内核版本是关键变量**，值得后续
  归因（7.0 wakeup/EEVDF 对 64-executor 更友好？）。
- **PS 协议点查：C2 202k–209k vs V97 330k（-37%）**：COM_STMT_EXECUTE 绕过
  reactor 路径（exec=3 证实），交接成本裸露。P1（reactor 支持 PS）再次确认为
  最高价值项。
- **sched_ext（C4）：点查 PS -39%（209k→127k）/ 文本 -16%（319k→268k）**，
  rw 吞吐与 C2 持平但 PS 下 p99 更稳（25–35ms）。submit→run：C4ps 41µs vs
  C2ps 16µs；文本下均 ~7–8µs（inline exec 绕开交接）。**方向：scx 收益在
  争用/锁等待场景，纯吞吐场景需更轻 fast path 或按负载开关。**

SCX 构建坑（Ubuntu 26.04 内核 7.0.0-38，详见 mysql-sched-bench
ENVIRONMENT.md §3 与本轮 summary §3）：本地 6.6 内核树头缺两参
`scx_bpf_dsq_move_to_local` compat 宏不能用；节点 BTF 生成的 vmlinux.h 也
不行（缺 `struct scx_cmask`）；正确配方 = 节点 clone sched-ext/scx 用
`scheds/include` + 仓库预生成 `scheds/vmlinux/arch/x86/vmlinux-v7.3-rc5-*.h`
（`-DSCX_VMLINUX_H=`）。加载/卸载冒烟干净（enabled、3 map、无害
"member not found" 告警）。

**资源保留中（用户后续还有测试，勿释放）**：db i-0jl15tmmc3uy6tdcfxl6
（16c，10.79.14.104/8.160.179.206）、loadgen i-0jl01sye88k16xz4qspi（8c，
10.79.14.105/8.160.113.197），双 spot，auto-release 兜底 2026-11-09T15:30Z
（跨点需先推迟）。state：
`/root/src/aliyun/state/mysql-sched-vs97-20261109.json`。工作站公网 IP 在
124.70.231.8 与 36.28.77.68 间摆动，两个 /32 均已入安全组
sg-0jlis1x48amzqvro5ciz。

## 4.7 paper64 轮（2026-11-09，论文 §6.4 三基准全量，资源仍保留中）

论文 §6.4（HTAP 行已删）的三项基准全量完成，对比 V97 / C2 / C4，全部文本
协议（`--db-ps-mode=disable`，reactor parse/exec 全程生效）。脚本
`aliyun/mysql-sched-bench/run_paper64.sh`（rwscaling/ratescan）与
`run_paper64_tpcc.sh`（TPC-C load/run）。结果：
`aliyun/mysql-sched-bench/results/paper64-20261109/summary-paper64.md`，
论文 §6.4 已写入 v4 实测数字。

核心结果：

- **rw 伸缩**：C2/C4 吞吐为 V97 的 85–96%，但 t128 时 p95 反超（49.2 vs
  61.1ms），t256 时 C4 p95 持平（84.5ms）。
- **点查速率阶梯（open-loop 25k–300k）**：三配置全部精确线性跟踪至 300k/s
  未触膝点；300k 档 p95：C2 0.25 < V97 0.30 < C4 3.49ms（scx 在无锁点查上
  只见成本，符合消融预期）。
- **TPC-C**（BenchBase 32 仓库 64 terminals RC 180s）：C2 2214 vs V97 2228
  req/s（-0.6% 打平），p99 352.6 vs 371.5ms 略优；C4 2121（-4.8%，与 §6.6
  的 scx ~5% 开销结论一致）。三配置 abort 率相近、无 SQL 错误。

本轮暴露并修复两个缺陷（用户要求：测试中遇到问题直接修复、每个修复独立
commit 写清背景）：

1. **准入收缩下界自饿死**（`b10d64916`，已 push）：t256 下 C2 从 6748 崩至
   978 TPS（admission_deferred 1330 万次）。收缩下界为 0，长事务阻塞 credit
   回收时反复收缩到远低于池容量。修复：下界 = executor 池大小；新增
   `Coroutine_admission_credits` 状态变量。修复后 t256=6672，与关准入对照
   （6748）差 <1.2%。
2. **协程帧地址误设 thread_stack**（`7bffa3131`，已 push）：内联执行块在无栈
   协程体内以 `&thd`（堆协程帧）设 `thread_stack`，check_stack_overrun 得到
   ASLR 垃圾差值，t256 假报 error 1436（"95MB 栈用量"）。潜伏 bug：每次启动
   是否触发看布局运气，反向布局还会漏检真实溢出。修复：删除该调用，依赖同
   迭代 parse_on_reactor 在 reactor OS 栈上的正确设置（内联块只深数帧）。

两个修复均通过本地 gunit（netpoller-t 8 / executor-t 13 / task-t 5 /
stmt_priority-t 9）与 mtr（coroutine_reactor_exec / coroutine_stmt_prio /
coroutine_latch_status），云端增量重建后复测通过。BenchBase v2023 在 loadgen
/opt/benchbase-run 部署完成（maven 构建 + tpcc_mysql.xml 配置）。

**资源仍保留（用户后续还有测试，勿释放）**：同 4.6 双节点；auto-release 兜底
2026-11-09T15:30Z（=23:30 CST），若跨点需先推迟。当前 db 节点无 mysqld、
loadgen 无 java 残留，处于干净空闲态。

## 4.8 c4perf 轮（2026-11-10，C4 vs V97 持平攻关，资源保留中）

目标：把 C4（coroutine + scx 语义调度）对 V97（pristine 9.7 线程模型）
的吞吐差距追到持平。起点 rw256：V97 7829 / C2 7237 / C4(v4) 6692 TPS
（-14.5%）。结果与过程数据：
`aliyun/mysql-sched-bench/results/c4perf-20261110/summary.md`。

perf 归因（30s 稳态，v4）：BPF 直接成本 14.07 CPU-s = 16 核 2.84%
（dispatch 859ns×9.5M 为大头，全局 vtime DSQ 自旋锁+rbtree），
cpu-migrations 73.6k/s = C2 的 2.5×（全局池无亲和性）。三项叠加解释
C2→C4 的 -7.5%。

两个修复轮次（均独立 commit 已 push github/coroutine-dev2）：

1. **v5 两池化**（`30260af4b`）：无语义状态任务 → per-CPU FIFO DSQ
   （零 vtime 计算、仅本 CPU 锁）；仅携带语义状态（commit VIP、锁持有、
   PI waiter、BG 层）的任务进全局 EEVDF DSQ；dispatch 先 EEVDF 后本地
   FIFO 再轮询窃取（工作守恒）；TS 层插队本 CPU FIFO 头+kick。gate
   （sem_any）为引用计数，先加后发布/先清后删。注意：per-CPU DSQ 创建
   必须 node=-1；不能用 SCX_DSQ_LOCAL（6.6+ balance_one 只在 builtin
   local 空时才调 ops.dispatch，会饿死语义池）。
2. **v6 泄漏修复**（`17b1f9420`）：lock_holder 计数用跨系统调用
   check-then-act RMW 维护，并发 clear 丢更新 → 永久残留 → gate 引用
   计数泄漏 → 数分钟内全部 worker 被 gate → 退化回慢路径（TPC-C 半程
   fastpath 只剩 30%）。修复：值改 {count=1, ts_ns} 布尔语义（幂等发布
   {1,now}、clear 直接删、无 RMW），BPF 侧 HOLDER_TTL_NS=1s 过期不认，
   sweeper 2s GC。验证：t64 60s 后空闲 4s，lock_holder/blocked_by/
   commit_vip 全 0，sem_any=15=task_tier（仅启动线程钉住）。

最终结果（同日同集群）：

- **rw256（60s sysbench oltp_read_write）**：V97 7829 / C2 7237 /
  C4-v4 6692 / C4-v5 7239 / **C4-v6 7167、7125（两轮）**。
  调度器开销归零（C4 ≈ C2）；残差 -8% 是 C2↔V97 协程模型差距
  （C2 在 t256 留 ~9% CPU 空闲），属另一工作项。
- **ratescan（open-loop 点查 25k→300k，64 线程）**：v5 起 300k 档
  p95 0.28ms（v4 曾 3.49ms 爆炸；C2 0.25 / V97 0.30），尾延迟修复确认。
- **TPC-C（干净库 32w/64t/180s，顺序 V97→C2→C4）**：V97 2247.7 /
  C2 2179.2（-3.0%）/ C4-v6 2137.9（-4.9% vs V97，**-1.9% vs C2**）；
  NewOrder p99 三者 702-738ms 打平。TPC-C 跨 run 库增长对最后的 C4
  略不利，C4↔C2 的 -1.9% 更接近调度器真实残差。
- **p99 专项（rw256 t256 60s，--percentile=99）**：V97 397.4 /
  C2 272.3 / **C4-v6 215.4ms（比 V97 低 46%）**，语义调度（commit VIP
  +锁持有加速）在尾延迟上兑现了设计目标。TPC-C 按类型：OrderStatus
  p99 C4 最优（-39% vs V97），StockLevel -29%，NewOrder 打平，
  Payment +30% 偏弱（短更新事务 worker 排队尾，待查）。
- **perf 多角度（30s 稳态归一化）**：每事务指令数 V97 5.36M /
  C2 5.88M / C4v6 6.32M（+18% 是 C4↔V97 差距的主构成：reactor 解析/
  交接/桥接 syscall）；ctx 220k→159k/s；迁移 C4v6 59.9k/s 仍 2× C2；
  RES-IPI 16.7k/s（调度器 kick 引入，V97 仅 4/s）；分支误表率
  V97 1.42% 反而最高。优化方向按收益排序见 summary.md §9
  （指令总量 > kick 收敛 > 全局池亲和 > dispatch 空池跳过 >
  Payment 短事务尾部）。注意：perf-stat time-enabled 是 16 核求和，
  归一化窗口 = 值/16 = 30s，别用 bpftool 的 69s 窗口去除 perf-stat。

部署坑（已写入 `mysql-sched-bench/ENVIRONMENT.md` §9）：`rsync -a`
保留旧 mtime → make 跳过重建（bridge 没编进去，sem_any 全空，语义静默
绕过）；同步后 touch 或 `--no-times`，验证 `strings mysqld | grep sem_any`。

**资源保留中（用户后续还有测试，勿释放）**：双节点同 4.6/4.7
（db `i-0jlde5e4b4n8ea717zz0`，loadgen `i-0jl9k261boqz655doivn`），
auto-release 兜底 2026-11-10T12:40:40Z。

## 4.9 c4opt 轮（2026-11-10，逐项优化与负结果，资源保留中）

完整原始日志与逐轮分析：
`aliyun/mysql-sched-bench/results/c4opt-20261110/summary.md`。
本轮只保留两个代码改动，均已独立提交并 push `github/coroutine-dev2`：

1. **v7 命令交接降本**（`2a903c485`）：命令状态放入 ConnectionState，
   executor lambda 只捕获 shared_ptr 以落入 std::function 内联存储；worker
   复用 reactor 的 lock-holder 判定，inline SELECT 不再做无效锁图查询；
   命令完成标志复位不再多拿一次 mutex。100MB redo 下同日下午 perf：
   每事务指令 6.172M → 5.608M（-9.1%），迁移 61.6k/s → 54.9k/s。
2. **v8 运行中 TS 任务免 kick**（`7cc194212`）：task_running 为真时不再
   发 SCX_KICK_PREEMPT，stats[6]/[7] 记录跳过/发送。关闭内联对照：
   v7 9793.68 / v8 9897.57 TPS，p99 75.82 → 73.13ms；默认内联复测
   10416.37 TPS、p99 69.29ms。首轮默认内联曾出现 9163.78 TPS /
   49.21ms 的高内联相位，不能当作稳定收益，故用关闭内联和第二轮复核。

逐项试验后**明确否决**的方向：

- worker 扫描（4G redo、rw256）：v7 默认 64 worker = 10236.68 TPS；
  128 = 9832.95 / p99 102.97ms（CPU 99.5%、loadavg≈89）；48 = 9232.06；
  32 = 9385.90 / p99 53.85ms。128 的 submit→run 均值虽从 551µs 降至
  349µs，吞吐反而下降；低排队不等于全局高性能，默认池大小不改。
- 100ms 内联冷却：9036.65 TPS（比 v8 低约 13%）、p99 63.32ms，
  inline 比例 69.5%，执行并发不足。恢复 1s 冷却，未提交。
- dispatch 空全局池 `nr_queued` 预检：在同样约 10% inline 相位，
  dispatch 433 → 635ns/call，rhashtable/count 查询比失败 consume 更贵，
  未提交；没有把该预检扩展到窃取路径。
- worker 硬绑核：迁移 29.3k/s → 12.2k/s，但吞吐 8828.34 TPS、
  p99 118.92ms。跨核工作守恒比迁移数更重要，未提交。DML/COMMIT 也不
  放到 reactor 内联，避免行锁/redo 的无界阻塞破坏韧性。

**测试环境的重要修正**：原配置 `innodb_redo_log_capacity=100M`，错误日志
持续 MY-014084 checkpoint 追不上，导致吞吐限速和轮次漂移。最终对照全部
统一 4G（V97 与 C4 同样参数），不与 100MB 历史数据混算。loadgen 在
rw256 稳态仍有约 78% CPU 空闲，不是客户端瓶颈。`ENVIRONMENT.md` 已补
COMMON_ARGS、结果小文件回收、zombie 关闭等待和开放环预热经验。

最终版本 rw256（60s、4G redo、p99）：V97 11846.82 TPS / 54.83ms；
C4(v7+v8) 10342.88 TPS / 74.46ms，C4 吞吐 -12.65%、p99 +35.8%。
即本轮降低了指令与 IPI 成本，但**没有宣称对 V97 吞吐持平**。

其他最终场景：

- 点查开放环（64 线程，每配置先预热 30s，再测 30s）：200k 目标下
  V97/C4 达成 199946.86/199944.92，p99 0.18/0.24ms；300k 下达成
  299938.97/299991.88，p99 0.23/0.32ms。C4 两档都达到目标速率，
  p99 绝对差为 0.06/0.09ms。
- 默认 PS 协议 rw256（30s，统一 max_prepared_stmt_count=100000）：
  V97 12351.80 TPS / 78.60ms，C4 10055.39 TPS / 90.78ms。
  C4 reactor_parsed 仅 302559、reactor_exec=3，确认覆盖 COM_STMT_EXECUTE
  的 worker 路径，未借用文本内联优化。第一次 16382 语句上限失败和点查
  冷启动 event queue full 均已排除，不计作性能数据。
- TPC-C（32 仓库、64 terminal、RC、180s、4G redo）：V97 goodput
  7872.18/s、整体 p99 40.61ms；C4 6521.00/s（-17.17%）、45.20ms
  （+11.3%）。NewOrder/Payment p99 分别 +24.6%/+20.0%，StockLevel
  p99 -12.0%。双方均无 Unexpected SQL Errors；只使用 tpcc-final 中
  15-48-17 / 15-52-07 的新结果，旧 rsync 文件不参与计算。

验证：最终本地 mysqld/mysqltest、build_coro sql_main、BPF 编译通过；
coroutine_reactor_exec / coroutine_stmt_prio / coroutine_latch_status 及
shutdown_report 全 pass；ExecutorPool 13 + Reactor 8 + Task 5 +
StmtPriority 9 共 35 个 gunit 全 pass。云端二进制与 v7/v8 保存件 SHA
一致，本地与云端源文件 SHA 一致。

**资源保留中（用户后续还有测试，勿释放）**：实例同 §4.8，auto-release
兜底 2026-11-10T12:40:40Z。

## 4.10 psinline 轮（2026-11-10，COM_STMT_EXECUTE reactor 内联，提交 8e14d449f，已 push github/coroutine-dev2）

背景：§4.9 收尾时 PS rw256 比 V97 低 18.57%，而 text 点查靠 reactor
内联追平；根因是 `can_parse_on_reactor` 只识别 COM_QUERY，二进制协议的
COM_STMT_EXECUTE 全部走 executor 交接，协议间不对称。

实现（`sql/conn_handler/coroutine_connection_handler.cc`）：
- D5b 分类器重构为按 LEX 判定（`select_lex_is_reactor_safe`），同一套
  分类既用于 reactor 刚 parse 的 COM_QUERY，也用于 prepared statement
  保留的解析树（`Prepared_statement::m_lex`，惰性求值，不改
  sql_prepare）。
- 新增 `can_inline_stmt_execute`：MSG_PEEK 14 字节（header+cmd+stmt_id+
  flags+iteration_count），要求 COM_STMT_EXECUTE、iteration_count==1、
  无 CURSOR_TYPE_READ_ONLY、FIONREAD 确认整包已在内核缓冲（内联
  my_net_read 不会阻塞 reactor）、stmt_map 按 id 找到且分类为安全。
- 新增 `inline_stmt_execute`：普通函数（真实 OS 栈），设
  thd_set_thread_stack/store_globals 后直接跑 `do_command(thd)`——与
  Unparsed worker job 完全相同的代码，只是跑在 reactor 上。
- run() 循环在 text 内联块后新增 PS 内联块，复用 500µs 预算 + 1s 冷却、
  admission 还款、计数器（`Coroutine_reactor_exec`）。
- 只受 `coroutine_reactor_exec` 门控（不需要 reactor_parse）；默认 OFF，
  sysvar 描述已更新。

测试：新增 `coroutine_ps_exec` MTR（需 `--ps-protocol` 运行，默认运行
skip）：prepared 普通 SELECT 内联 +3、SLEEP/FOR UPDATE/DML 不内联 +1、
parse=OFF 时 prepared SELECT 仍内联（恰好 +2，证明二进制路径独立于
text parse）、动态开关生效。注意：**mysqltest 的 `enable_ps_protocol`
指令在不加 CLI `--ps-protocol` 时是空操作**（它只是把开关设回 CLI 标志
值）；`coroutine_stmt_prio` 在 `--ps-protocol` 下失败是**既有行为**
（已用 stash 基线复现确认）：D5 优先级分类依赖 COM_QUERY 的 reactor
parse，PS 命令不经过该路径，非本次回归。回归：4 个 coroutine MTR 双模
式全过、35 个 gunit 全过、build/build_coro 均编译通过。

云端 A/B（同轮、二进制按 SHA 校验，C4-old=8cc28c00 v7+v8，C4-new=
4d78baf9 本提交远端构建；本地是 aarch64 不能直接上传 x86 云机，源文件
rsync 后云端增量构建）：
- PS 点查 open-loop（64 线程、30s、p99）：200k 档 V97 199944/0.16ms、
  C4-old 199923/0.45ms、C4-new 200026/0.21ms；300k 档 V97 299916/
  0.20ms、**C4-old 复现性 FAIL（event queue full，30s/60s 预热均失败）**、
  C4-new 300002/0.26ms。预热闭环节奏下 C4-new 达 370k QPS。
- PS rw256（60s 同轮）：V97 13150.06/53.85ms；C4-old 10280.71/71.83ms；
  C4-new 10660.97/68.05ms（**+3.7% TPS、p99 -5.3%**）；对 V97 差距仍
  -18.9%——瓶颈在写/提交路径排队（submit→run 588µs），非读交接。
- text rw256 回归（60s 同轮）：C4-old 10366.07/73.13ms（内联 10.3%）、
  C4-new 10334.51/74.46ms（9.6%）——无回归；另一轮 9394.86/63% 内联为
  已知双相位现象（旧二进制同日也出现 9163/68%）。
- 内联预算扫描（PS rw256 30s）：500µs→7.7% 内联/p99 87.56ms；1ms→20%/
  90.78；2ms→63%/121.08；5ms→63%/110.66；10ms→63%/132.49；100ms→64%/
  161.51ms（TPS 9154）。饱和下把多数读搬进 8 个 reactor 反而使 reactor
  成为瓶颈；默认 500µs+冷却自适应限制在快语句尾部，是正确默认。

产物：`/root/src/aliyun/mysql-sched-bench/results/psinline-20261110/`
（summary.md + 各日志；300k 旧二进制失败日志在 loader 上被覆盖，目录内
为重构记录）。云端保留 `mysqld.pre_psinline`/`mysqld.psinline` 供后续
A/B。实例同 §4.8 保留，auto-release 兜底 12:40:40Z。

下一步候选（未做）：写/提交路径的排队差距（-18.9%）才是 rw256 主矛盾；
预算扫描表明继续提高内联占比方向错误。

## 5. 已知边界与注意事项

- D3 让 slow-query 起点和 `thread_running` 覆盖 reactor→worker 排队时间，这是有意的语义变化，需在测试中确认。
- 当前 runtime 初始化早于 daemonize fork，不能用 `--daemonize` 运行 coroutine 模式；后续若需要支持，应将 runtime 启动移到 fork 之后，而不是只隐藏问题。
- 完整大 SQL 也可能让 reactor parse 较久；后续可评估报文/SQL 长度上限和 parse 时间预算，不要静默取消现有回退。
- rw-lock latch 等待边已由 D6 覆盖（见上）；pass≠0 传递锁与多 reader wait-ex 不发布边是能力边界而非缺陷；业务 priority/resource-group 硬映射已移除是有意设计，不要恢复成永久分层。
- `Docs/preemptdb-sigmod2025.pdf` 和 `/root/src/preemptdb` 是参考资料；PreemptDB zip 克隆没有 git 历史。
- MTR 测试的选项文件必须命名为 `mysql-test/t/<test>-master.opt`（此版本不读裸 `.opt`）；从仓库根目录用 `./build/mysql-test/mtr [ --record ] <test>` 运行。
- 本机容器环境下 MY-015655（"Server ignores the discovered container restrictions"）警告会使所有 MTR 测试失败（含 `main.1st`），是环境级问题；测试内用 `call mtr.add_suppression(...)` 处理。
- mysqltest 变量只在 `eval` 命令中插值，含变量的 SELECT 必须写成 `eval SELECT ...`；mysql CLI `--force -e` 在报错后会发 COM_QUIT，驱动错误风暴场景应改用 mysqltest。
- 云 benchmark 遵守根目录 `AGENTS.md`：结果先拉回并校验，实例测试后立即释放；Spot 实例构建产物及时备份。
