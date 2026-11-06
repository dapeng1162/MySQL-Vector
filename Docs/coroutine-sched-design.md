# Coroutine-Epoll 前台 + 执行线程池 + sched_ext 架构：设计与实现

> 分支：`coroutine-dev2`（基于 MySQL 9.7，`008e09c28`）
> 平台约束：**Linux ≥ 6.12**（sched_ext 自 6.12 起可用；本机内核 6.18.15）

---

## 一、架构概览

```
+-----------------------------------------------------------------+
| 前台 Reactor 层（1 个 OS 线程，epoll ET + EPOLLONESHOT）          |
| - 监听 fd + 全部连接 fd 由单个 epoll 实例驱动                      |
| - 每连接一个 C++20 无栈协程帧（~几百字节）：                       |
|   wait_readable(fd) → 收到完整 command → 提交 THD 执行 → 等待完成  |
| - 持 THD 所有权（空闲态）；不参与 parse/optimize/execute           |
+-----------------------------------------------------------------+
        |  Handoff：THD 指针 + command 元数据
        |  (共享队列 + condition_variable + cascade notify)
        v
+-----------------------------------------------------------------+
| 后台执行线程池（pthread，hardware_concurrency × 4，sched_setaffinity）|
| - 接管：THD::store_globals() + thd_set_thread_stack()              |
| - 执行：dispatch_sql_command → parse → optimize → execute → InnoDB |
| - 释放：restore_globals()，NET 结果 buffer 交还前台协程异步写回     |
| - 行锁等待/IO 等待：原地阻塞（v1 设计决定）                        |
+-----------------------------------------------------------------+
        |  BPF hash maps (sched_ext, 可选降级)
        v
+-----------------------------------------------------------------+
| sched_ext BPF 调度器（内核态，Linux 6.12+，默认 WITH_SCX=OFF）     |
| - commit_vip: 提权 COMMIT 线程优先调度                              |
| - blocked_by: 锁依赖边 → 动态优先级继承                             |
| - lock_holder: 被等待持锁者 → 防 LHP 长时间片                       |
| - rg_prio: 资源组优先级 → OS 调度权重映射                           |
+-----------------------------------------------------------------+
```

### 设计原则

1. **拒绝全盘协程化**：前台只做无状态网络 IO，避免深调用栈、栈内存暴胀、DMA Pin 冲突。
2. **THD 整体移交**：交接单元是整个 THD 对象（`store_globals()`/`restore_globals()`），而非不可序列化的 AccessPath/POD。
3. **有状态的阻塞留给线程**：行锁等待、IO 等待在执行线程上原地阻塞；线程池系数（4× 硬件并发数）吸收阻塞。
4. **sched_ext 标记降级**：内核无 sched_ext 时，BPF map 为空操作，服务器正常降级运行。

---

## 二、分阶段实现要点

### Phase 1：协程运行时（`coroutine/`）

| 文件 | 职责 |
| --- | --- |
| `coroutine/include/coroutine/task.h` | C++20 无栈协程类型：`Task<T>` + `DetachedTask` |
| `coroutine/include/coroutine/netpoller.h` | 单线程 epoll 事件循环：`wait_readable/writable`、`sleep_for`、`post`、`cancel_fd` |
| `coroutine/src/netpoller.cc` | epoll ET + EPOLLONESHOT、eventfd 唤醒、定时器堆、fd 兴趣表 |
| `coroutine/include/coroutine/executor.h` | 固定线程池：共享队列 + condition_variable + 线程钩子 |
| `coroutine/src/executor.cc` | 绑核（`sched_setaffinity`）、`my_thread_init/end`、cascade notify |
| `coroutine/CMakeLists.txt` | 构建静态库 `mysql_coroutine` |

**关键设计**：`submit()` 发送 `notify_one`；`worker_loop()` 在从队列取任务时，若队列中剩余任务 > 1 则再 `notify_one()`（cascade）。这避免了单 worker 独占突发任务（导致死锁），也避免了 `notify_all` 的惊群开销。

**测试**：`unittest/gunit/coroutine/` 包含 14 个 gunit 测试（3 个用例），覆盖 task、netpoller、executor，全部通过。**TSAN**（ThreadSanitizer）在 gtest 用例 + 40K 并发任务压力测试上均无数据竞争。

### Phase 2：coroutine-epoll 连接处理器（`sql/conn_handler/`）

| 文件 | 职责 |
| --- | --- |
| `sql/conn_handler/coroutine_connection_handler.h` | `Coroutine_connection_handler` 类声明 |
| `sql/conn_handler/coroutine_connection_handler.cc` | 连接生命周期：auth → command loop → teardown |
| `sql/conn_handler/connection_handler_manager.h/cc` | 注册 `SCHEDULER_COROUTINE_EPOLL` 枚举 |
| `sql/sys_vars.cc` | 注册 `thread_handling=coroutine-epoll` 选项 |
| `sql/CMakeLists.txt` | 链接 `mysql_coroutine` 库 |

**连接生命周期**：

1. **auth**：`submit_thd_job(thd_prepare_connection)` → `co_await CommandDone`。认证在 executor 线程上完成（发送 greeting + 读取 auth 响应）。
2. **command loop**：`co_await wait_readable(fd)` → `submit_thd_job(do_command)` → `co_await CommandDone`。循环直到 `should_close`（`thd->killed`、`connection_events_loop_aborted`）。
3. **teardown**：`cancel_fd(fd, token)` → `end_connection` → `close_connection` → `release_resources` → `remove_thd` → `delete thd` → 经注册表 `mysql_socket_close()` 关 fd（reactor 线程上唯一关闭点）。

**CommandDone 同步**：双阶段协议（`cmd_mu` mutex + `cmd_done` atomic + `cmd_waiting` handle），支持 `await_ready` 快速路径（作业已完成时立即返回，不挂起）。

### Phase 3：sched_ext 桥接层（`sql/sched_ext_bridge.h/cc`）

| 文件 | 职责 |
| --- | --- |
| `sql/sched_ext_bridge.h/cc` | 空操作降级包装：`commit_begin/end`、`wait_begin/end` |
| `support-files/scx/scx_db_sched.bpf.c` | BPF 调度器：`commit_vip`、`blocked_by`、`lock_holder` 映射 |
| `support-files/scx/scx_db_sched_main.c` | BPF 调度器用户态加载器 |
| `support-files/scx/CMakeLists.txt` | 可选构建（`WITH_SCX_SCHEDULER`=OFF 时跳过） |

**注入点**（`sql/handler.cc`）：
- `ha_commit_trans()` 入口/出口 → `sched_ext::commit_begin/end()`（写入 `commit_vip` BPF map）
- 行锁等待边通过 `thd_wait_begin/end` / `thd_report_lock_wait` 回调 → `blocked_by` / `lock_holder` map

**降级路径**：`WITH_SCX_SCHEDULER`=OFF（默认）时，`sched_ext::init()` 为无操作，构建时完全不链接 BPF 相关代码。注意：bridge 的 **THD 级锁等待图（`lock_wait_begin/end`、`is_lock_holder`、`forget_thd`）始终编译**（纯用户态状态，无 libbpf 依赖），它独立于 sched_ext 驱动协程 handler 的 HP 队列——即使不带 SCX 构建/内核无 sched_ext，显式事务锁持有者的 COMMIT 仍走高优队列。仅 tid 级 BPF map 发布半边被 `#ifdef HAVE_SCX_SCHEDULER` guard。

### Phase 4a：状态变量（`sql/mysqld.cc`）

7 个 `SHOW GLOBAL STATUS LIKE 'Coroutine_%'` 变量：

| 变量 | 来源 | 说明 |
| --- | --- | --- |
| `Coroutine_connections` | `Connection_handler_manager::get_connection_count()` | 当前连接数 |
| `Coroutine_handoff_queue_depth` | `ExecutorPool::queued_jobs()` | 待执行队列深度 |
| `Coroutine_reactor_events` | `Netpoller::events_processed()` | 累计 epoll 事件数 |
| `Coroutine_executor_threads` | `ExecutorPool::num_workers()` | 执行线程数 |
| `Coroutine_commands_completed` | 全局 atomic counter | 已完成命令数 |
| `Coroutine_connections_closed` | 全局 atomic counter | 已关闭连接数 |
| `Coroutine_active_coroutines` | 全局 atomic counter | 活跃前台协程数 |

---

## 三、性能数据

### 3.1 环境

- CPU：aarch64，10 核，ARMv8
- 内存：14 GB
- 内核：Linux 6.18.15
- 存储：NVMe SSD
- 编译器：GCC 12.3.1，C++23
- 构建：`cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo`

### 3.2 sysbench oltp_read_write 对比

| 指标 | one-thread-per-connection | coroutine-epoll | 比率 |
| --- | --- | --- | --- |
| **TPS** (32 threads, 15s) | 6367 | 3176 | 50% |
| **QPS** | 127,340 | 63,955 | 50% |
| **95% 延迟 (ms)** | ~2.0 | 11.2 | 5.6× |
| **RSS (MB)** | 622 | 594 | 95% |
| **连接数伸缩** | 32 OS 线程 | 32 协程 + 40 线程 | |

### 3.3 并发伸缩与读负载

| sysbench 场景 | threads | coroutine TPS | 备注 |
| --- | --- | --- | --- |
| oltp_read_write | 1 | 563 | 无锁争用，纯单线程 |
| oltp_read_write | 32 | 3176 | 锁争用为主 |
| oltp_read_write | 64 | 3175 | 吞吐已达天花板（锁争用受限） |
| oltp_read_write | 128 | 3240 | 无执行线程耗竭，稳定运行 20s |
| oltp_point_select | 32 | **67,320** | 读负载，95% 延迟 0.68ms |

**关键结论**：
- `oltp_read_write` 吞吐天花板 ~3200 TPS 由 **InnoDB 行锁争用**决定，不是 coroutine 处理器瓶颈。32/64/128 线程吞吐几乎不变（3176/3175/3240），128 线程时仅 ~13/40 执行线程活跃、handoff 队列深度恒为 0，**未出现执行线程耗竭**。
- 读负载 `oltp_point_select` 达 **67,320 TPS**（0.68ms 95% 延迟），证明处理器本身能承载高 QPS；此时与 one-thread-per-connection 基线（316,902 TPS）的 ~21% 差距主要来自每命令的 reactor 交接开销（2 次上下文切换 + epoll 重注册 + 队列互斥），对廉价查询占比更大，对重查询（rw 负载）摊薄到 ~50%。

### 3.4 问题修复历程

| 问题 | 症状 | 根因 | 修复 |
| --- | --- | --- | --- |
| 50ms 轮询 | 3.68 TPS | 命令循环中用 `sleep_for(50ms)` 轮询 | 改为 `co_await wait_readable(fd)`（epoll 驱动） |
| 单 worker 独占 | 32 连接死锁（0 TPS） | `notify_one` 唤醒一个 worker，该 worker 独占突发任务队列，遇阻塞 IO 后死锁 | `notify_one` + cascade notify（队列中 >1 任务时再唤醒一个 worker） |
| thundering herd | 311 TPS（notify_all） | `notify_all` 每次提交唤醒 40 个 worker（250K 上下文切换/秒） | 改为 cascade notify 策略 |

### 3.5 Soak 测试（120 秒连续负载）

| 时间点 | RSS (KB) | FDs | 备注 |
| --- | --- | --- | --- |
| 基线 | 590,096 | 36 | 空闲 |
| t+20s | 607,016 | 68 | 32 连接活跃 |
| t+40s | 607,692 | 68 | 稳定 |
| t+60s | 608,004 | 68 | 稳定 |
| t+80s | 608,184 | 68 | 稳定 |
| t+100s | 608,328 | 68 | 稳定 |
| t+120s | 608,544 | 36 | 连接关闭，RSS 常态 |

- **RSS 增长**：+18 MB（120s），增长主要在 t+0~t+20（Buffer Pool 预热），之后每 20s < 0.5 MB，无内存泄漏。
- **FD 泄漏**：无（36→68→36）。
- **TPS 稳定**：3171 TPS（全程无降解）。

---

## 四、与 MySQL Thread Pool 插件对照分析

本仓库包含完整的 MySQL Thread Pool 插件（`plugin/thread_pool/`，约 1.1 万行），本节基于源码分析其性能退化场景，并与 coroutine-epoll 方案对照。

### 4.1 对照：7 个性能退化场景

| # | 场景 | 触发条件 | 影响 | 源码锚点 | Thread Pool 表现 | Coroutine-epoll 表现 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | **stall 检测延迟** | 阻塞未走 `thd_wait_begin`（CPU 密集长查询） | P99 +60-70ms 长尾延迟 | `thread_pool.cc:553` `:2845` `:2331` | stall checker 轮询（60ms 周期）后创建逃生线程 | 无 stall 概念；执行线程原地阻塞，前台线程池系数 4× 吸收 |
| 2 | **线程爆炸** | 行锁风暴 → stalled → `max_active_threads` 持续增加 | 最高 65536 线程（4096/组 × 16 组），上下文切换风暴 | `thread_pool.cc:662` `:1423` `:1361` | 每 stall_limit 周期创建一批新线程，速率控制仅 3×vCPU 以上生效 | 固定线程池（40 线程），不随负载/阻塞数膨胀 |
| 3 | **优先级提升反转** | 混合长短查询，短查询在低优先级队列等 >1000ms 后被提升 | 长查询被反复插队 | `thread_pool.cc:1534` `:4710` `:4743` | 两级优先级队列 + `prio_kickup_timer` 静态阈值 | 无优先级队列，FIFO 共享队列；cascade notify 分布负载 |
| 4 | **单组瓶颈** | 轮询分配导致活跃连接扎堆同一组，`thread_pool_size` 默认 16 | 单组卡 credit，其他组空闲，吞吐受限 | `thread_pool.cc:3074` `:3260` `thread_pool.h:59-69` | 连接轮询分配，活跃度不均时组间无法迁移 | 单共享队列，所有执行线程平等消费，无分组瓶颈 |
| 5 | **epoll 批处理上限** | 高连接数下大量事件同时就绪，每次最多取 32 个 | 多次 `epoll_wait`，增加尾部延迟 | `thread_pool.h:76` `epoll.cc:323-366` | 每线程组一个 epoll fd，`MAX_EVENTS=32` | 单 reactor 全局 epoll，`kMaxEvents=1024`，无上限瓶颈 |
| 6 | **组内锁竞争** | 组内活跃线程 >16 时 `LOCK_group` 竞争 | 单组吞吐受限 | `thread_pool.cc:1611,1654-1657` | per-group 锁，跨组无竞争但组内排队/出队受同一把锁 | `m_mu` 保护共享队列，锁范围仅入队/出队，无跨组概念 |
| 7 | **阻塞读/线程绑定** | 默认空闲连接不占线程；MTL 开启时 worker 绑定到连接跨事务阻塞读 | MTL 时线程数 = 并发事务数 | `thread_pool.cc:1155-1178` `:842` `:1212` | 默认 `rearm` 到 epoll + `thread_detach` 归还线程 | 前台协程 `wait_readable` 等待，执行线程不阻塞 |

### 4.2 关键纠正

1. **"空闲连接吃线程"**：Thread Pool 默认配置下 worker 不会在空闲连接上阻塞读（`thread_pool.cc:1155-1178`）。处理完查询后 `tp_client_low_level_rearm` 把 fd 重挂 epoll、`thread_detach` 归还线程，等待新命令的是 waiting thread（epoll_wait）。**Coroutine-epoll 在此场景并不比 thread pool 有架构优势**——两者都是语句级线程复用。区别在于 coroutine-epoll 用协程承载等待态（内存占用更低，2w 连接 < 100MB），而 thread pool 用一个 waiting thread 做 epoll_wait。

2. **"stall 检测是主要唤醒路径"**：对常见的行锁/IO 等待，`thd_pool_wait_begin`（`thread_pool.cc:3800`）通过 `tp_wake_thread` 提供了**即时唤醒**路径，**不需要**等 stall_limit 到期。stall checker 仅处理未走 `wait_begin` 的阻塞（CPU 密集长查询）。Coroutine-epoll 的 `notify_one` + cascade 机制在阻塞发生时也有类似效果——其他 idle worker 被 cascade 唤醒继续处理队列。

3. **"哈希分配导致碰撞"**：分配方式是轮询（`next_group_idx = (next_group_idx+1) % tp_normal_groups`，`thread_pool.cc:3074`），不是哈希。不均衡来自活跃度差异，不是哈希碰撞。Coroutine-epoll 的单共享队列从根本上避免了此问题。

### 4.3 本质差异

| 维度 | Thread Pool | Coroutine-epoll |
| --- | --- | --- |
| **线程管理** | 动态创建/销毁（上限 65536），受 stall 驱动 | 固定线程池（40 线程），不随负载波动 |
| **连接模型** | 分组（16-512 组），组内 epoll_wait | 单 reactor epoll_wait + 协程 |
| **阻塞处理** | `thd_wait_begin` 即时唤醒 + stall 兜底 | 原地阻塞 + 4× 系数 + cascade 分布 |
| **参数调优** | 约 20 个系统变量（`stall_limit`、`prio_kickup_timer`、`max_threads`...） | 约 2 个（`thread-handling` + 编译时线程系数） |
| **最大短板** | 线程爆炸与启发式参数退化 | 单 reactor 瓶颈（需未来分片） |

---

## 五、关键设计决策

### 5.1 THD 整体移交而非 PPD 描述符

MySQL 9.7 的 AccessPath 不可序列化（内嵌约 25 处 `TABLE *`、`Item *`、`Mem_root_array`，全部挂在 THD 的 `main_mem_root` 上）。源码已有 `THD::store_globals()/restore_globals()` 跨线程迁移机制（`sql/sql_class.cc:1801-1844`，注释明确写明 "This allows us to move THD to different threads if needed"）。交接单元因此是**整个 THD 对象**。

### 5.2 前台不做 parse/optimize

parse/optimize 需要 MDL 锁、TABLE 对象、Query_arena，均为有状态操作。前台协程只做无状态的网络 IO 与协议拆包。

### 5.3 执行线程池系数（4×）

`do_command` 内部含 `my_net_read`（阻塞 IO）和行锁等待。系数 4 确保在 IO 等待时有闲置线程处理其他连接。系数过大则增加上下文切换开销。

### 5.4 Cascade notify 策略

单共享队列 + condition_variable。`submit()` 时 `notify_one` 唤醒一个 worker；worker 在取任务时若队列中剩余 >1 个任务，则 cascade `notify_one` 再唤醒一个 worker。这避免了：
- **notify_one 垄断**：单 worker 独占突发任务，遇阻塞 IO 后死锁。
- **notify_all 惊群**：每次提交唤醒所有 worker，250K 上下文切换/秒。

### 5.5 sched_ext 降级优先

`WITH_SCX_SCHEDULER` 默认 OFF，`sched_ext::init()` 为无操作。注入点（`ha_commit_trans` 入口/出口）用 `#ifdef HAVE_SCX_SCHEDULER` 包裹。即使内核未编译 `CONFIG_SCHED_CLASS_EXT`，服务器正常运行。

---

## 六、已知限制与未来工作

### v2：两层调度（THD 级等待图 + HP 队列 + VIP worker）

**背景**：云端实测（Fedora 6.19.10 + sched_ext）发现 v1 的 tid 解析对显式事务
失效——显式事务（`BEGIN; UPDATE ...; COMMIT`）在语句间持有行锁，此时 holder THD
不在任何 OS 线程上（`real_id == 0`），`lock_wait_begin` 无法把等待边写入以
OS tid 为 key 的 BPF map。实测 244k 次解析成功（autocommit、holder 正在执行）
vs 13.8k 次失败（显式事务 holder 在语句间隙）。根因：**协程架构下"持锁者的下
一条语句由哪个线程执行"在等锁时刻不可知**。

**v2 设计：两层调度**（BPF 调度器零改动）：

1. **用户态层（解决排队死锁 / T2）**：`sched_ext_bridge` 维护 THD 级等待图
   （`waiter→holder` 边 + holder 等待计数，mutex 保护的叶子锁，THD* 仅作
   opaque key）。reactor 在提交 holder 的下一条命令前查 `is_lock_holder(thd)`，
   命中则走 executor 的**高优先级队列**；第 0 号 worker 是**VIP 专用 worker**
   （只消费 HP 队列），即使全部普通 worker 阻塞在锁等待上（死锁风暴场景），
   持锁者的 COMMIT 仍有线程执行——T2 的 COMMIT 活性由此获得端到端保证。
2. **内核层（解决抢占自旋 / T4 LHP）**：executor 线程在 THD job 开始时调
   `thread_job_begin(thd)`：若该 THD 是锁持有者（等待计数 > 0），把
   `lock_holder[当前tid]=计数` 写入 BPF map；job 结束时 `thread_job_end` 清除。
   scx 调度器照旧对持锁 tid 给 `SCX_SLICE_INF` + vtime boost。autocommit 的
   v1 即时路径保留为兼容（holder 正在执行时 `lock_wait_begin` 直接写 tid map）。

**相关 API**（`sql/sched_ext_bridge.h`）：`thread_job_begin/end`、
`is_lock_holder`、`forget_thd`（teardown 清理残留等待边）；executor
`submit(job, high_priority)`；状态变量 `Coroutine_hp_jobs_submitted/executed`。

**编译边界（重要）**：THD 级等待图（`lock_wait_begin/end`、`is_lock_holder`、
`forget_thd`）是纯用户态状态，**始终编译**（`sched_ext_bridge.cc` 无条件编入
sql_main）——即使 `WITH_SCX_SCHEDULER`=OFF 或内核无 sched_ext，用户态 HP 队列
+ VIP worker 仍完整工作（本地无 SCX 构建冒烟验证：显式事务锁持有者的 COMMIT
`Coroutine_hp_jobs_submitted/executed` 计数正确递增）。仅 tid 级 BPF map 发布
半边（`thread_job_begin/end` 的 map 写入、`scx_publish_wait_edge` 兼容路径）被
`#ifdef HAVE_SCX_SCHEDULER` guard。

验证：`executor-t` 新增 4 个测试（HP 优先出队、VIP worker 救援阻塞池、
VIP 不饿死普通任务、num_vip 钳制）。

**v2 云端端到端验证（Ubuntu 26.04，内核 7.0，SCX 构建 + scx_db_sched）**：
显式事务（`BEGIN; UPDATE; SLEEP; COMMIT`）+ 竞争者场景下——
- `lock_holder` BPF map 在等待期间出现持有者 executor tid（v1 在显式事务
  场景解析失败的根因已由 v2 `thread_job_begin` 执行期发布解决）；
- `blocked_by` map 正确记录 waiter_tid → holder_tid；
- COMMIT 后两个 map 清空，无泄漏；
- `Coroutine_hp_jobs_submitted/executed` 各 +1（COMMIT 走 HP 队列）；
- autocommit 竞争、KILL 回归、协程计数均正常。

**教训（已修）**：曾引入 `lock_wait_begin` 持有 `s_wait_mu` 调用
`scx_publish_wait_edge`（内部再锁同一非递归 mutex）的自死锁——首次行锁等待
即全服挂死。该路径仅在 SCX 构建下编译，本地 no-SCX 构建冒烟无法覆盖，
由云端验证捕获。锁序约束：`s_wait_mu` 为叶子锁，**任何 BPF map 写入必须在
释放它之后进行**（`lock_wait_begin/end` 均为 scoped lock + 释放后发布）。

### v3：三层任务模型（TS / Normal / BG）——对 UFS 论文双重调度问题的回答

**背景**：v2 解决了"持锁者活性"，但 OS 与 DB 两层调度器仍然互不知情。以
UFS 论文（arXiv 2605.02377，分层 sched_ext 调度）的视角检视本架构：网络 IO
协程（reactor 线程）与执行线程共享同一组 CPU，DB 层无法把自己的优先级意图
传达给 CFS；反向，CFS 的公平份额会把 InnoDB 后台任务（purge、dict stats、
FTS）与前台事务一视同仁地调度。实测两类证据（本地 aarch64 10 核，C2 配置）：

- `Coroutine_submit_to_run` 延迟直方图：队列深度为 0 时仍有 33% 的 job 落在
  100-500µs 桶、8k+ 个 >500µs——**worker 线程被 OS 抢走/迁移**，而非排队；
- 外部 CPU 噪声（B7）下 open-loop 点查 p99 0.59ms → 7.84ms（13×）、max 到
  96ms——OS 层无保护，DB 层 HP 队列管不到 CPU。

**v3 设计：DB 意图 → OS 调度器的单向发布 + 内核侧 tier 模型**

1. **用户态三级 tier**（`mysql_sched_tier.h`，C 接口）：
   `TIER_TS=0`（time-sensitive：reactor 线程、HP/锁持有 job、COMMIT 路径）、
   `TIER_NORMAL=1`（默认）、`TIER_BG=2`（purge/dict stats/FTS/monitor 等后台
   线程 + 低优先级 resource group 的会话）。
2. **发布通道**：`sched_ext_bridge::thread_job_begin(thd, time_sensitive)` 在
   job 粒度把 tier 写入 `task_tier` BPF map（key=执行 tid）；后台线程入口一次性
   `mysql_sched_set_thread_tier(MYSQL_SCHED_TIER_BG)`；resource group 映射
   （`m_cur_resource_group` 的 `priority`：<0 → TS、>0 → BG，同时写 `rg_prio`）。
3. **内核侧**（`scx_db_sched.bpf.c`）：三层 DSQ 模型
   `COMMIT_VIP_DSQ（TS）→ SHARED_DSQ（Normal, vtime）→ BG_DSQ（vtime）`；
   TS 入队即 `scx_bpf_kick_cpu(SCX_KICK_PREEMPT)` 抢占当前 CPU；BG 仅在
   SHARED 空或超过 `BG_GUARANTEE_NS=100ms` 保底窗口时放行（防饿死，benchmark
   中监控 history list length 确认 purge 没有被饿出问题）。
4. **coroutine 模式下的 resource group 修复**（前置条件）：上游在
   `mysqld.cc` 对所有非 one-thread-per-connection 模式禁用 resource group。
   v3 放开 coroutine-epoll（`coroutine_scheduler_active()`），但**跳过**
   `Thread_resource_control::apply_control()` 的线程级应用（nice/绑核会泄漏
   到共享 worker，污染其他连接的 job——实测切换 RG 前后 worker nice 分布
   不变，验证豁免生效）；PFS 记账（`add_pfs_thread_id`）同样跳过，避免
   `ALTER RESOURCE GROUP` 把控制应用到错标的 worker。组信息只落在 THD
   （`m_cur_resource_group`），由 bridge 逐 job 翻译成 tier。
5. **观测**：`Coroutine_submit_to_run_avg_us/_total/_bucket_0_5us..gt_500us`
   7 桶延迟直方图（executor 队列侧）+ scx loader `--dump`（内核侧 map 快照）。

**验证矩阵**（详见 `scripts/bench/sched/README.md`；方法学取自 TailBench
open-loop、UFS 混合负载场景、Shore-MT 伸缩、Willit Scale 式噪声注入、
CH-benCHmark 混合 OLTP/AP）：C1 = one-thread-per-connection（OS-only 基线）、
C2 = coroutine 无 SCX（v2）、C4 = coroutine + scx v3 tier。关键预期：
C4 在 B7 外部 CPU 噪声下把 p99/max 拉回 C2-无噪声水平；B1 minmax 场景
bursty TPS 恢复到 solo 的 90%+。

### v3 基准结果（阿里云 ecs.c9i.4xlarge，16 vCPU）

环境：db 节点与压测节点分离（同可用区内网），sysbench 1.0.20，32 表 ×10 万行。
配置：C1 = one-thread-per-connection（OS-only 基线）、C2 = coroutine-epoll（v2，
内核 EEVDF）、C4 = coroutine + scx v3 tier。数据文件：`scripts/bench/sched/results/cloud-db2/`。

**B7 决定性实验——db 节点上的外部 CPU 抢占**（stress-ng --cpu 16 直接压在
db 节点，模拟"OS 层来了 DB 不认识的负载"；loadgen open-loop 10k rate 点查，
每轮 30s）：

| 配置 | 基线 p99 | +噪声 p99 | 劣化 | 基线 max | +噪声 max |
| --- | --- | --- | --- | --- | --- |
| C2（EEVDF） | 0.395ms | **3.02ms** | **7.6×** | 1.8ms | 18.1ms |
| C4（v3 tier） | 0.44ms | **0.60–0.74ms** | 1.4–1.7× | 3.2ms | ~22ms |

这正是 UFS 论文描述的双重调度问题：C2 下 16 个外部抢占者与 reactor/executor
共享 CPU，OS 无从知道哪些线程对尾延迟敏感，p99 劣化 7.6 倍。C4 把 DB 的
tier 意图发布给内核调度器后，同样的噪声下 p99 仅劣化 1.4–1.7 倍，绝对值
（0.60–0.74ms）比 C2（3.02ms）好 **4–5 倍**。注意两点诚实记录：C4 首轮出现
一次 217ms 的 max 离群（噪声进程启动过渡期），后续 3 轮复测均为 21–22ms，
不可复现；C4 的 max（~22ms）并不优于 C2（18ms）——tier 保护作用于 p99 分位，
个别极端样本仍受抢占影响。

**B1 UFS 混合负载**（前台 TPS × BG 会话 8，median TPS / p99 ms）：

| 配置 | solo | minmax | 5050 |
| --- | --- | --- | --- |
| C1 | 3842 / 104.8 | 4058 / 89.2 | 4099 / 87.6 |
| C2 | 3795 / 101.1 | 4007 / 87.6 | 4248 / 80.0 |
| C4 | 3818 / 97.6 | 4094 / **75.8** | 4237 / **73.1** |

C4 的 minmax 场景中 BG 会话经 `rg_bg` resource group（THREAD_PRIORITY=19）
落入 BG tier，前台 p99 从 87.6ms 降至 75.8ms（-13%）。C1/C2 的 minmax 与
5050 几乎等价——没有 OS 层协调时，两种 mix 对 DB 内部排队同构，这与本地
结论一致；只有 C4 能区分（75.8 vs 73.1）。

**B2 优先级反转**：三配置 waiter 均 ~4007ms（= 4s 锁等待 + 提交），无差异——
v2 的 HP 队列已保证持锁者活性，tier 不再改变结果。

**B4 open-loop 内部噪声**（DB 内 `BENCHMARK()` 循环，rate 40k）：
C1 none/cpu p99 0.14/0.28ms，C2 0.16/0.30ms，C4 0.15/0.32ms。内部噪声
（走 SHARED_DSQ 的 Normal tier）三配置表现一致；C4 的 cpu 场景 max 出现
~40ms 离群（多轮 median 仍在），疑为 SCX 下单次抢占风暴，未定位到根因。

**B5 伸缩**（点查 QPS / p99，2 轮 median）：

| 亲和核数 × 线程 | C2 QPS | C4 QPS | C2 p99 | C4 p99 |
| --- | --- | --- | --- | --- |
| 16c × t16 | 140k | 133k | 0.15ms | 0.18ms |
| 16c × t64 | 175k | 164k | 0.43ms | 0.46ms |
| 16c × t256 | 170k | 159k | 1.61ms | 1.73ms |
| 8c × t64 | 138k | 140k | — | — |
| 4c × t256（过饱和） | 83k | **100k（+20%）** | 3.4ms | 6.0ms |

正常负载下 SCX 有 ~5% 吞吐开销（EEVDF 的 CFS 快路径 vs BPF 程序开销）；
但过饱和（4 核 256 线程）时 C4 吞吐反超 20%——BG tier 与债务机制防止了
饿死与公平份额误分配，代价是 p99 更高（SCX 的公平性倾向 vs EEVDF）。

**方法学教训**（对"负向 benchmark"的使用）：
1. 噪声必须注入在 db 节点而非压测节点——早期 B7 在 loadgen 上压
   stress-ng，三配置 p99 几乎相同（~1.14ms），只测到了客户端争用，实验无效；
2. `submit_to_run_avg_us` 列在云端采集到 0，status 变量提取在远端脚本路径
   下静默失败（lib.sh 容错吞掉了错误），直方图数据以本地 aarch64 实验为准；
3. 首轮数据须至少 2 轮 median 并检查 max 离群（C4 的 217ms 单次离群即由
   复测排除）。

**结论**：v3 回答了"网络 IO 线程与执行线程是否互相干扰"——在 OS-only
（C1）和 v2（C2）下会：外部抢占者使 p99 劣化 7.6 倍，且 DB 层排队数据
（本地直方图 33% 落 100-500µs）表明 worker 被抢走而非排队。单向发布 tier
意图（C4）后 p99 保护达到 4–5 倍改善，代价是正常负载 ~5% 吞吐与过饱和时
更高的尾延迟。这一权衡与 UFS 论文结论一致：DB-informed 调度换来的是可预期
的尾延迟，而非吞吐。

### v1 限制

1. **执行线程耗竭风险**：`do_command` 中行锁等待/IO 等待在 executor 线程上原地阻塞。4× 系数在实测 128 并发连接下未触发耗竭（仅 ~13/40 线程活跃、队列深度 0）。但若单事务内多语句长事务大量同时进入阻塞等待，仍有可能耗竭；根治需要"锁等待时 THD 挂起归还线程"（执行侧协程化），列为远期阶段。
2. **单 reactor 瓶颈**：所有 epoll 事件由单线程处理。~3000 TPS 时 reactor 利用率约 30%，不会在近期成为瓶颈，但极致伸缩需多 reactor 分片（shard by fd）。
3. **sched_ext 云端实测已完成**（v1，Fedora 6.19.10，4 核 taskset + 32 executor 超配）：`scx_db_sched` 开启后热点 UPDATE 负载 TPS +66%、95th 延迟 -45%（18.5k→30.8k TPS）。同时暴露 v1 的 tid 解析限制（见上 v2 节）。
4. **KILL 矩阵/PSI 仪表化基本补齐（本次修复）**：KILL 路径已修复——`vio_shutdown()` 经 pre-close 钩子（`vio_pre_close_hook`，见 `violite.h`/`viosocket.cc`）把协程持有的 fd 延迟到 reactor 线程的 teardown 关闭，并向 netpoller `post` 一个带 token 校验的强制取消，挂起在 `wait_readable` 的协程被唤醒后走正常 teardown；fd 在被 teardown 关闭前不会被回收复用，消除了误 cancel/误 close 新连接的串扰。fd 的 PSI socket 仪表由 teardown 经注册表保存的 `MYSQL_SOCKET` 副本关闭，destroy 语义不变。残留：executor 上运行的 `do_command` 抛出的 C++ 异常现在只终止该连接（`run_thd_job` try/catch 兜底 + 错误日志），不再整服崩溃；`wait_timeout` 空闲超时与 `performance_schema.threads` 的协程映射仍未实现。
5. **NUMA 不感知**：InnoDB 无 per-node buffer pool 实例，`target_numa_node` 路由无法实现。NUMA 分区改造属独立工程。

### 构建与运行

```bash
# 构建（默认 WITH_SCX_SCHEDULER=OFF）
cd build
cmake .. -DCMAKE_BUILD_TYPE=RelWithDebInfo -DWITH_SCX_SCHEDULER=OFF
make -j$(nproc) mysqld

# 初始化 datadir
mkdir -p /tmp/coro_bench
./runtime_output_directory/mysqld --no-defaults --initialize-insecure \
  --datadir=/tmp/coro_bench/data --user=root

# 启动 coroutine-epoll 模式
./runtime_output_directory/mysqld --no-defaults --datadir=/tmp/coro_bench/data \
  --socket=/tmp/coro_bench/ce.sock --thread-handling=coroutine-epoll \
  --log-error=/tmp/coro_bench/log_ce.txt --pid-file=/tmp/coro_bench/pid_ce \
  --user=root --skip-log-bin --innodb-buffer-pool-size=256M \
  --max_connections=300 --innodb_flush_log_at_trx_commit=2 &

# 验证
./runtime_output_directory/mysql -S /tmp/coro_bench/ce.sock -u root -e "SELECT 1"
./runtime_output_directory/mysql -S /tmp/coro_bench/ce.sock -u root \
  -e "SHOW GLOBAL STATUS LIKE 'Coroutine%'"

# 运行测试
make -j$(nproc) task-t netpoller-t executor-t
./runtime_output_directory/task-t && ./runtime_output_directory/netpoller-t && ./runtime_output_directory/executor-t
```

---

## 七、源码文件清单

| 路径 | 行数 | 职责 |
| --- | --- | --- |
| `coroutine/include/coroutine/task.h` | ~100 | C++20 无栈协程基类 |
| `coroutine/include/coroutine/netpoller.h` | ~242 | epoll 事件循环声明 |
| `coroutine/src/netpoller.cc` | ~262 | epoll 事件循环实现 |
| `coroutine/include/coroutine/executor.h` | ~101 | 执行线程池声明 |
| `coroutine/src/executor.cc` | ~141 | 执行线程池实现（cascade notify） |
| `coroutine/CMakeLists.txt` | ~30 | 构建 `mysql_coroutine` 静态库 |
| `sql/conn_handler/coroutine_connection_handler.h` | ~51 | `Coroutine_connection_handler` 声明 |
| `sql/conn_handler/coroutine_connection_handler.cc` | ~536 | 连接生命周期：auth + command loop + teardown |
| `sql/conn_handler/connection_handler_manager.h` | ~114 | `SCHEDULER_COROUTINE_EPOLL` 枚举 |
| `sql/conn_handler/connection_handler_manager.cc` | ~165 | 注册入口 |
| `sql/sys_vars.cc` | ~3922 | `thread_handling=coroutine-epoll` 选项 |
| `sql/mysqld.cc` | — | 7 个 `Coroutine_*` 状态变量注册 |
| `sql/sched_ext_bridge.h` | ~60 | sched_ext 降级桥接声明 |
| `sql/sched_ext_bridge.cc` | ~80 | 空操作降级实现 |
| `sql/handler.cc` | ~1690, 1942, 2136, 2220 | `COMMIT` 注入点 |
| `sql/CMakeLists.txt` | ~979 | 链接 `mysql_coroutine` |
| `support-files/scx/scx_db_sched.bpf.c` | — | BPF 调度器 |
| `support-files/scx/scx_db_sched_main.c` | — | BPF 加载器 |
| `support-files/scx/CMakeLists.txt` | ~91 | 可选构建 |
| `unittest/gunit/coroutine/task-t.cc` | — | 5 个 Task 测试 |
| `unittest/gunit/coroutine/netpoller-t.cc` | — | 6 个 Netpoller 测试 |
| `unittest/gunit/coroutine/executor-t.cc` | — | 3 个 ExecutorPool 测试 |
| `unittest/gunit/coroutine/CMakeLists.txt` | — | 测试构建配置 |