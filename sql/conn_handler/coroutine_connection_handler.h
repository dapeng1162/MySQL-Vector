#ifndef COROUTINE_CONNECTION_HANDLER_INCLUDED
#define COROUTINE_CONNECTION_HANDLER_INCLUDED

#include "coroutine/executor.h"
#include "coroutine/netpoller.h"
#include "my_inttypes.h"
#include "sql/conn_handler/connection_handler.h"
#include <atomic>
#include <memory>
#include <thread>

class Coroutine_connection_handler : public Connection_handler {
 public:
  Coroutine_connection_handler();
  ~Coroutine_connection_handler() override;

  // 禁止拷贝
  Coroutine_connection_handler(const Coroutine_connection_handler &) = delete;
  Coroutine_connection_handler &operator=(const Coroutine_connection_handler &) = delete;

  static void init();
  static void destroy();

  // Accessors for file-scope helper functions
  static coroutine::Netpoller *netpoller(size_t i = 0) {
    return (s_netpollers && i < s_num_reactors) ? s_netpollers[i] : nullptr;
  }
  static coroutine::Netpoller *netpoller_for_fd(int fd) {
    return (s_netpollers && s_num_reactors > 0)
               ? s_netpollers[(size_t)fd % s_num_reactors]
               : nullptr;
  }
  static size_t reactor_count() { return s_num_reactors; }
  static coroutine::ExecutorPool *executor() { return s_executor; }

  // Observability accessors (return 0 when coroutine mode is not active).
  static long long connections();
  static long long handoff_queue_depth();
  static long long reactor_events();
  static long long executor_threads();
  static long long commands_completed();
  static long long connections_closed();
  static long long active_coroutines();
  static long long hp_jobs_submitted();
  static long long hp_jobs_executed();
  static long long admission_deferred();
  static long long reactor_parsed();

  // Submit → run latency of executor jobs (userspace queue wait plus the
  // OS run-queue wait of the woken worker).
  static long long submit_to_run_avg_us();
  static long long submit_to_run_total();
  static long long submit_to_run_bucket(size_t i);

 protected:
  bool add_connection(Channel_info *channel_info) override;
  uint get_max_threads() const override;

 private:
  // Sharded reactors: s_num_reactors Netpoller instances, each driven by
  // its own thread.  A connection is pinned to one reactor for its whole
  // lifetime (fd % s_num_reactors), so fd cancel/close always happens on
  // the same thread that owns its epoll registration.
  static coroutine::Netpoller **s_netpollers;
  static std::thread **s_reactor_threads;
  static size_t s_num_reactors;

  // 后台执行线程池
  static coroutine::ExecutorPool *s_executor;

  static bool s_initialized;
};

/** Number of executor worker threads for coroutine-epoll mode.
 *  0 = auto (max(4 * hardware_concurrency, 16)). */
extern ulong coroutine_executor_threads;
/** Number of VIP workers consuming only the high-priority queue. */
extern ulong coroutine_executor_vip_workers;
/** Number of reactor threads (sharded epoll frontends). Default 1. */
extern ulong coroutine_reactor_threads;
/** Parse complete plain COM_QUERY packets on the reactor before execution. */
extern bool coroutine_reactor_parse;
/** Admission control: max allowed submit→run queue time in µs (0 = off). */
extern ulong coroutine_admission_queue_time_us;

#endif