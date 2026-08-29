#ifndef COROUTINE_CONNECTION_HANDLER_INCLUDED
#define COROUTINE_CONNECTION_HANDLER_INCLUDED

#include "coroutine/executor.h"
#include "coroutine/netpoller.h"
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
  static coroutine::Netpoller *netpoller() { return s_netpoller; }
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

 protected:
  bool add_connection(Channel_info *channel_info) override;
  uint get_max_threads() const override;

 private:
  // 单个 Reactor 线程 + 整个 Netpoller
  static coroutine::Netpoller *s_netpoller;
  static std::thread *s_reactor_thread;

  // 后台执行线程池
  static coroutine::ExecutorPool *s_executor;

  static bool s_initialized;
};

#endif