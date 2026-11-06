# coroutine / sched_ext 协同调度：工作交接

> 更新时间：2026-11-06。本文是后续 agent 的入口，记录真实代码状态，不把设计目标当作已验证结果。

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
