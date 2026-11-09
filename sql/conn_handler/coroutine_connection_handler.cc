/*
   Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#include "sql/conn_handler/coroutine_connection_handler.h"

#include "coroutine/task.h"
#include <sys/ioctl.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <memory>
#include <mutex>
#include <new>
#include <thread>
#include <unordered_map>

#include "mysql/components/services/log_builtins.h"  // LogErr
#include "my_inttypes.h"
#include "my_macros.h"
#include "my_thread.h"  // my_thread_init, my_thread_end
#include "mysql/psi/mysql_socket.h"  // mysql_socket_getfd, mysql_socket_set_thread_owner
#include "mysql_com.h"
#include "mysqld_error.h"             // ER_*
#include "sql/conn_handler/channel_info.h"
#include "sql/conn_handler/connection_handler_manager.h"  // Connection_handler_manager
#include "sql/current_thd.h"
#include "sql/mysqld.h"               // max_connections, connection_events_loop_aborted
#include "sql/mysqld_thd_manager.h"   // Global_THD_manager
#include "sql/protocol_classic.h"     // get_vio
#include "sql/sql_class.h"            // THD
#include "sql/sql_connect.h"          // thd_prepare_connection, close_connection
#include "sql/sql_lex.h"              // Parser_state
#include "sql/sql_parse.h"            // do_command
#include "sql/sql_thd_internal_api.h" // thd_set_thread_stack
#include "sql/sched_ext_bridge.h"     // sched_ext::init
#include "sql/stmt_priority.h"        // stmt_priority::classify
#include "sql/table.h"                // Table_ref
#include "violite.h"                  // vio_is_blocking, vio_set_blocking_flag

// -------------------------------------------------------------------
// Static members
// -------------------------------------------------------------------

coroutine::Netpoller **Coroutine_connection_handler::s_netpollers = nullptr;
std::thread **Coroutine_connection_handler::s_reactor_threads = nullptr;
size_t Coroutine_connection_handler::s_num_reactors = 0;
coroutine::ExecutorPool *Coroutine_connection_handler::s_executor = nullptr;

ulong coroutine_executor_threads = 0;
ulong coroutine_executor_vip_workers = 1;
ulong coroutine_reactor_threads = 1;
bool coroutine_reactor_parse = false;
bool coroutine_reactor_exec = false;
ulong coroutine_reactor_exec_budget_us = 500;
ulong coroutine_admission_queue_time_us = 2000;
bool Coroutine_connection_handler::s_initialized = false;

// -------------------------------------------------------------------
// Per-connection state (lives on the reactor thread, also referenced
// from executor pool threads)
// -------------------------------------------------------------------

namespace {

struct ConnectionState : std::enable_shared_from_this<ConnectionState> {
  THD *thd{nullptr};
  int client_fd{-1};
  // Owning reactor for this connection's whole lifetime (fd sharding).
  coroutine::Netpoller *np{nullptr};
  // Set at submit time for admission queue-time sampling.
  std::chrono::steady_clock::time_point enqueued_at{};

  // Set once when the connection closes (avoids double-cleanup).
  std::atomic<bool> connection_closed{false};

  // Completion signalling (frontend <-> executor pool thread).
  std::mutex cmd_mu;
  std::coroutine_handle<> cmd_waiting{nullptr};
  std::atomic<bool> cmd_done{false};

  // Results from the executor jobs.
  bool auth_ok{false};
  bool cmd_result{false};
  // Set when a THD job threw an exception on the executor thread; the
  // frontend tears the connection down instead of continuing.
  bool job_threw{false};

  // While now() < reactor_exec_cooldown_until, skip inline execution for
  // this connection (a previous inline statement exceeded the budget).
  std::chrono::steady_clock::time_point reactor_exec_cooldown_until{};

  // The main connection coroutine.
  coroutine::DetachedTask run();
};

// -------------------------------------------------------------------
// fd ownership registry (KILL / fd-recycling fixes)
// -------------------------------------------------------------------
// vio_shutdown() may be called from any thread (e.g. a KILL issuing
// THD::awake) and would close the client fd behind the reactor's back:
// the kernel silently removes a closed fd from epoll, so a coroutine
// suspended in wait_readable() would never wake up, and the fd number
// could be recycled by a new connection while still registered.
//
// To prevent this, every fd owned by this handler is registered here.
// The vio pre-close hook (coroutine_vio_pre_close) claims such fds:
// vio_shutdown() then skips the close, and the fd stays open — hence
// cannot be recycled — until teardown() closes it on the reactor
// thread.  The hook additionally posts a token-checked cancellation so
// a suspended waiter wakes up and tears the connection down.

struct FdRegistration {
  ConnectionState *owner;  // cancellation token
  MYSQL_SOCKET socket;     // instrumented socket, closed by teardown()
};

std::mutex g_fd_mu;
std::unordered_map<int, FdRegistration> g_fd_registry;

/// vio pre-close hook (installed by init(), see violite.h).  Called by
/// vio_shutdown() from any thread just before it would close the
/// socket.  Returns true when the fd is owned by this handler: the
/// close is then deferred to teardown() on the reactor thread.
static bool coroutine_vio_pre_close(Vio *vio) {
  const int fd = mysql_socket_getfd(vio->mysql_socket);
  if (fd < 0) return false;
  ConnectionState *owner = nullptr;
  {
    std::lock_guard<std::mutex> lk(g_fd_mu);
    auto it = g_fd_registry.find(fd);
    if (it != g_fd_registry.end()) owner = it->second.owner;
  }
  if (owner == nullptr) return false;
  coroutine::Netpoller *np = owner->np;
  if (np == nullptr) return false;
  // Wake any coroutine waiting on this fd; it will observe thd->killed
  // and run teardown(), which performs the actual close.
  np->cancel_fd_async(fd, owner);
  return true;
}

/// Completion-signalling awaiter for the frontend coroutine.
/// The frontend submits a THD job to the executor pool, then
/// co_awaits this awaiter.  When the executor thread finishes the
/// job it calls signal_command_done() which resumes the frontend
/// on the reactor thread.
class CommandDone {
 public:
  explicit CommandDone(ConnectionState *self)
      : m_self(self) {}

  bool await_ready() {
    return m_self->cmd_done.load(std::memory_order_acquire);
  }

  bool await_suspend(std::coroutine_handle<> h) {
    std::lock_guard<std::mutex> lk(m_self->cmd_mu);
    if (m_self->cmd_done.load(std::memory_order_relaxed)) return false;
    m_self->cmd_waiting = h;
    return true;
  }

  void await_resume() {}

 private:
  ConnectionState *m_self;
};

// Forward declarations of helpers.
static void signal_command_done(std::shared_ptr<ConnectionState> self);
static void ensure_nonblocking(const std::shared_ptr<ConnectionState> &self);
static void teardown(std::shared_ptr<ConnectionState> self);

// -------------------------------------------------------------------
// Global observability counters (read by SHOW STATUS from any thread).
// -------------------------------------------------------------------

std::atomic<long long> g_commands_completed{0};   // do_command jobs finished
std::atomic<long long> g_connections_closed{0};   // connections torn down
std::atomic<long long> g_active_coroutines{0};    // live frontend coroutines
std::atomic<long long> g_hp_jobs_submitted{0};    // jobs submitted to the HP queue
std::atomic<long long> g_hp_jobs_executed{0};     // HP jobs that finished
std::atomic<long long> g_reactor_parsed{0};       // queries parsed on reactors
std::atomic<long long> g_reactor_exec{0};         // queries executed inline on reactors
std::atomic<long long> g_reactor_exec_slow{0};    // inline executions over budget

// -------------------------------------------------------------------
// Admission control (queue-time credit)
//
// Signal: EWMA of per-job submit→run latency (includes both the userspace
// queue wait and the OS run-queue wait, so it cannot go blind the way a
// pure queue-depth signal can).  Credits shrink multiplicatively when the
// EWMA exceeds coroutine_admission_queue_time_us and grow additively when
// it falls below 1/4 of the target.  Deferred commands wait in the
// frontend coroutine (never blocking a reactor thread), so overload shows
// up as queueing delay, not as executor-pool exhaustion or client errors.
// -------------------------------------------------------------------

std::mutex g_adm_mu;
long long g_adm_credits = 64;
long long g_adm_outstanding = 0;
double g_adm_ewma_us = 0.0;
std::atomic<long long> g_admission_deferred{0};

static void admission_sample(uint64_t latency_us) {
  std::lock_guard<std::mutex> lk(g_adm_mu);
  g_adm_ewma_us = 0.875 * g_adm_ewma_us + 0.125 * (double)latency_us;
}

static void admission_job_done() {
  std::lock_guard<std::mutex> lk(g_adm_mu);
  if (g_adm_outstanding > 0) g_adm_outstanding--;
  if (coroutine_admission_queue_time_us == 0) return;
  const double hi = (double)coroutine_admission_queue_time_us;
  const double lo = hi / 4.0;
  if (g_adm_ewma_us > hi) {
    g_adm_credits = std::max(1LL, (long long)(g_adm_credits * 0.9));
  } else if (g_adm_ewma_us < lo) {
    g_adm_credits = std::min(1LL << 20, g_adm_credits + 1);
  }
}

// Awaiter form of the admission gate: await_ready() tries to take one
// credit; when none is available the caller sleeps briefly and retries.
// Used as:  while (!(co_await AdmissionReady{self.get()})) { sleep; }
struct AdmissionReady {
  ConnectionState *self;
  bool admitted{false};

  bool await_ready() {
    std::lock_guard<std::mutex> lk(g_adm_mu);
    if (coroutine_admission_queue_time_us == 0 ||
        g_adm_outstanding < g_adm_credits) {
      g_adm_outstanding++;
      admitted = true;
      return true;
    }
    return false;
  }
  bool await_suspend(std::coroutine_handle<>) { return false; }
  bool await_resume() { return admitted; }
};

// -------------------------------------------------------------------
// Executor job helpers
// -------------------------------------------------------------------

/// Run a THD-bound function on an executor pool thread.
/// Sets up thread-local globals and stack, then signals the frontend
/// coroutine when done.
static void run_thd_job(std::shared_ptr<ConnectionState> self,
                         std::function<void(THD *)> fn,
                         bool time_sensitive = false) {
  admission_sample((uint64_t)std::chrono::duration_cast<std::chrono::microseconds>(
                       std::chrono::steady_clock::now() - self->enqueued_at)
                       .count());
  THD *thd = self->thd;
  thd_set_thread_stack(thd, reinterpret_cast<const char *>(&thd));
  thd->store_globals();
  /* Publish the holder's wait count to the sched_ext scheduler while this
   * job (which may hold row locks others are waiting on) runs on this OS
   * thread.  High-priority jobs additionally run at time-sensitive tier
   * for their duration (preempting normal work on this CPU).  No-op when
   * not connected / not a lock holder. */
  sched_ext::thread_job_begin(thd, time_sensitive);
  try {
    fn(thd);
  } catch (...) {
    // Never let an exception escape the worker thread: it would skip
    // signal_command_done() (hanging the frontend coroutine forever)
    // and then kill the whole server via std::terminate.  Flag the job
    // so the frontend tears the connection down instead of continuing.
    LogErr(ERROR_LEVEL, ER_INTERNAL_ERROR,
           "coroutine executor: uncaught exception in THD job");
    self->job_threw = true;
  }
  sched_ext::thread_job_end(thd);
  thd->restore_globals();
  admission_job_done();
  signal_command_done(std::move(self));
}

/// Submit a THD job to the executor pool.  When the THD currently holds
/// locks that other transactions are waiting on, the job goes to the
/// high-priority queue so a lock holder's next statement (notably its
/// COMMIT) is not stuck behind every other job in the pool.
static void submit_thd_job(std::shared_ptr<ConnectionState> self,
                            std::function<void(THD *)> fn,
                            bool high_priority = false) {
  self->enqueued_at = std::chrono::steady_clock::now();
  if (high_priority) {
    g_hp_jobs_submitted.fetch_add(1, std::memory_order_relaxed);
    Coroutine_connection_handler::executor()->submit(
        [self = std::move(self), fn = std::move(fn)]() mutable {
          run_thd_job(std::move(self), std::move(fn), /*time_sensitive=*/true);
        },
        /*high_priority=*/true);
    return;
  }
  Coroutine_connection_handler::executor()->submit(
      [self = std::move(self), fn = std::move(fn)]() mutable {
        run_thd_job(std::move(self), std::move(fn));
      });
}

// -------------------------------------------------------------------
// Connection lifecycle helpers
// -------------------------------------------------------------------

/// @return true if the connection should be torn down.
static bool should_close(const std::shared_ptr<ConnectionState> &self) {
  if (self->connection_closed.load(std::memory_order_relaxed)) return true;
  THD *thd = self->thd;
  if (connection_events_loop_aborted()) return true;
  if (thd->killed.load(std::memory_order_relaxed) != THD::NOT_KILLED)
    return true;
  return false;
}

/// Signal that the executor job has finished.  Resumes the frontend
/// coroutine via the reactor's post queue.
static void signal_command_done(std::shared_ptr<ConnectionState> self) {
  std::coroutine_handle<> h;
  {
    std::lock_guard<std::mutex> lk(self->cmd_mu);
    self->cmd_done.store(true, std::memory_order_release);
    h = self->cmd_waiting;
    self->cmd_waiting = nullptr;
  }
  if (h) {
    // Resume on the connection's owning reactor thread.
    self->np->post([h]() { h.resume(); });
  }
}

/// Ensure the underlying VIO socket is in non-blocking mode
/// (required by the epoll frontend).
static void ensure_nonblocking(const std::shared_ptr<ConnectionState> &self) {
  Vio *vio = self->thd->get_protocol_classic()->get_vio();
  if (vio != nullptr && vio_is_blocking(vio)) {
    vio_set_blocking_flag(vio, false);
  }
}

static bool has_buffered_input(THD *thd) {
  NET *net = thd->get_protocol_classic()->get_net();
  Vio *vio = thd->get_protocol_classic()->get_vio();
  return net->remain_in_buf != 0 ||
         (vio != nullptr && vio->read_pos != vio->read_end);
}

static bool can_parse_on_reactor(
    const std::shared_ptr<ConnectionState> &self) {
  if (!coroutine_reactor_parse) return false;
  THD *thd = self->thd;
  NET *net = thd->get_protocol_classic()->get_net();
  Vio *vio = thd->get_protocol_classic()->get_vio();
  if (vio == nullptr || vio->ssl_arg != nullptr || net->compress ||
      has_buffered_input(thd)) {
    return false;
  }

  uchar header[NET_HEADER_SIZE + 1];
  const ssize_t n = mysql_socket_recv(
      vio->mysql_socket, reinterpret_cast<SOCKBUF_T *>(header), sizeof(header),
      MSG_PEEK | MSG_DONTWAIT);
  if (n != static_cast<ssize_t>(sizeof(header))) return false;
  const size_t packet_length = uint3korr(header);
  if (packet_length < 1 || packet_length >= MAX_PACKET_LENGTH ||
      header[NET_HEADER_SIZE] != COM_QUERY) {
    return false;
  }

  int available = 0;
  if (ioctl(self->client_fd, FIONREAD, &available) < 0) return false;
  return available >= 0 &&
         static_cast<size_t>(available) >= NET_HEADER_SIZE + packet_length;
}

static bool parse_on_reactor(const std::shared_ptr<ConnectionState> &self,
                             COM_DATA *com_data,
                             enum enum_server_command *command,
                             Parser_state *parser_state, bool *ready) {
  THD *thd = self->thd;
  thd_set_thread_stack(thd, reinterpret_cast<const char *>(&thd));
  thd->store_globals();
  bool close_connection = true;
  try {
    mysql_socket_set_thread_owner(
        thd->get_protocol_classic()->get_vio()->mysql_socket);
    close_connection =
        parse_com_query(thd, com_data, command, parser_state, ready);
  } catch (...) {
    LogErr(ERROR_LEVEL, ER_INTERNAL_ERROR,
           "coroutine reactor: uncaught exception while parsing COM_QUERY");
    self->job_threw = true;
  }
  thd->restore_globals();
  return close_connection;
}

// -------------------------------------------------------------------
// Statement-priority taxonomy (D5)
// -------------------------------------------------------------------

/// Whether the just-parsed statement's top-level SELECT list contains
/// RELEASE_LOCK()/RELEASE_ALL_LOCKS(): executing it releases advisory
/// user locks that other sessions may be blocked on in GET_LOCK().
/// Only the top-level SELECT list is inspected; a RELEASE_LOCK() hidden
/// in a subquery or a WHERE clause is not detected (documented limit).
static bool parsed_select_releases_user_lock(const THD *thd) {
  const LEX *lex = thd->lex;
  if (lex->sql_command != SQLCOM_SELECT) return false;
  const Query_block *qb = lex->query_block;
  if (qb == nullptr) return false;
  for (const Item *item : qb->fields) {
    if (item == nullptr || item->type() != Item::FUNC_ITEM) continue;
    const Item_func *func = down_cast<const Item_func *>(item);
    if (strcmp(func->func_name(), "release_lock") == 0 ||
        strcmp(func->func_name(), "release_all_locks") == 0) {
      return true;
    }
  }
  return false;
}

/// Whether the just-parsed statement is safe to execute inline on the
/// reactor thread (D5b): a single plain SELECT that only takes plain read
/// locks (no FOR UPDATE / LOCK IN SHARE MODE) and whose top-level select
/// list contains no function with user-visible side effects or unbounded
/// runtime.  Anything else — DML, locks, stored programs, advisory-lock
/// functions — goes through the executor pool as before.  Deeply nested
/// or expensive expressions are not detected here; the runtime budget
/// (coroutine_reactor_exec_budget_us) plus per-connection cooldown bounds
/// their cost.
static bool parsed_select_is_reactor_safe(const THD *thd) {
  const LEX *lex = thd->lex;
  if (lex->sql_command != SQLCOM_SELECT) return false;
  const Query_block *qb = lex->query_block;
  if (qb == nullptr) return false;

  // Only plain table reads.  At parse time a plain SELECT's tables carry
  // the parser-only TL_READ_DEFAULT (resolved to TL_READ / TL_READ_NO_
  // INSERTS at open_tables(), both read locks).  FOR UPDATE rewrites the
  // descriptor to TL_WRITE and LOCK IN SHARE MODE to TL_READ_WITH_SHARED_
  // LOCKS during contextualization, so both are excluded here and run on
  // executors (they can block and hold locks others wait on).
  for (Table_ref *tr = lex->query_tables; tr != nullptr;
       tr = tr->next_global) {
    const thr_lock_type lt = tr->lock_descriptor().type;
    if (lt != TL_IGNORE && lt != TL_READ_DEFAULT && lt != TL_READ)
      return false;
  }

  // Exclude advisory-lock / sleeping / unbounded functions in the
  // top-level select list (same scope as the RELEASE_LOCK heuristic).
  for (const Item *item : qb->fields) {
    if (item == nullptr || item->type() != Item::FUNC_ITEM) continue;
    const Item_func *func = down_cast<const Item_func *>(item);
    const char *name = func->func_name();
    if (strcmp(name, "get_lock") == 0 || strcmp(name, "sleep") == 0 ||
        strcmp(name, "benchmark") == 0 || strcmp(name, "release_lock") == 0 ||
        strcmp(name, "release_all_locks") == 0) {
      return false;
    }
  }
  return true;
}

/// Adapt a reactor-parsed statement into the pure classifier input
/// (sql/stmt_priority.h).  Runs on the reactor thread after
/// parse_on_reactor() and before the job is submitted, i.e. while this
/// coroutine has exclusive access to the THD.
static stmt_priority::Input stmt_priority_input(
    const THD *thd, enum enum_server_command command,
    bool lock_conflict_victim) {
  stmt_priority::Input in;
  in.command = command;
  in.parsed = true;
  in.sql_command = thd->lex->sql_command;
  in.holds_open_tables = thd->open_tables != nullptr;
  in.contains_release_lock = parsed_select_releases_user_lock(thd);
  in.lock_conflict_victim = lock_conflict_victim;
  return in;
}

/// Whether the previous executed statement on this connection failed
/// with a lock conflict, marking it a deadlock / lock-wait-timeout
/// victim whose next COMMIT/ROLLBACK should be prioritized.  Runs on
/// the reactor thread after the executor job completed (exclusive THD
/// access via the coroutine handoff).
static bool is_lock_conflict_victim(const THD *thd) {
  const Diagnostics_area *da = thd->get_stmt_da();
  return da->is_error() &&
         (da->mysql_errno() == ER_LOCK_DEADLOCK ||
          da->mysql_errno() == ER_LOCK_WAIT_TIMEOUT);
}

/// Clean up THD, VIO, globals, fd, and decrement the connection count.
/// Safe to call multiple times (idempotent via connection_closed flag).
static void teardown(std::shared_ptr<ConnectionState> self) {
  if (self->connection_closed.exchange(true)) return;

  g_active_coroutines.fetch_sub(1, std::memory_order_relaxed);
  g_connections_closed.fetch_add(1, std::memory_order_relaxed);

  THD *thd = self->thd;

  // Remove the fd from the epoll set before closing it.  The token
  // guards against cancelling a waiter that belongs to a different
  // connection which recycled this fd number.
  if (self->client_fd >= 0) {
    self->np->cancel_fd(self->client_fd, self.get());
  }

  // Teardown runs on the connection's owning reactor thread (not a pool
  // thread), so set up thread-local state for this THD.  Each connection
  // is pinned to one reactor and teardown is synchronous, so this cannot
  // interfere with other connections on that reactor.
  thd_set_thread_stack(thd, reinterpret_cast<const char *>(&thd));
  thd->store_globals();

  end_connection(thd);
  close_connection(thd, 0, false, false);

  /* Remove any residual lock-wait graph entries for this THD (both as
   * waiter and as holder) so the bridge's wait counts cannot leak. */
  sched_ext::forget_thd(thd);

  thd->get_stmt_da()->reset_diagnostics_area();
  thd->release_resources();

  Global_THD_manager::get_instance()->remove_thd(thd);
  Connection_handler_manager::dec_connection_count();

  thd->restore_globals();
  delete thd;
  self->thd = nullptr;

  // Close the fd exactly once, here on the reactor thread.  The vio
  // pre-close hook prevented vio_shutdown() (invoked above via
  // close_connection(), or earlier by a KILL on another thread) from
  // closing it, so the fd number cannot have been recycled while
  // registered.  The registry copy of the instrumented socket lets
  // mysql_socket_close() also destroy the PSI socket instrument.
  if (self->client_fd >= 0) {
    MYSQL_SOCKET sock = MYSQL_INVALID_SOCKET;
    {
      std::lock_guard<std::mutex> lk(g_fd_mu);
      auto it = g_fd_registry.find(self->client_fd);
      if (it != g_fd_registry.end() && it->second.owner == self.get()) {
        sock = it->second.socket;
        g_fd_registry.erase(it);
      }
    }
    if (mysql_socket_getfd(sock) >= 0)
      mysql_socket_close(sock);
    else
      ::close(self->client_fd);
    self->client_fd = -1;
  }
}

// -------------------------------------------------------------------
// Main connection coroutine
// -------------------------------------------------------------------

coroutine::DetachedTask ConnectionState::run() {
  auto self = shared_from_this();

  // Ensure non-blocking mode before any reactor interaction.
  ensure_nonblocking(self);

  // ---- Phase 1: Authentication ----
  // The server initiates the handshake by sending the greeting, so we
  // must NOT wait for client data first (that would deadlock: the client
  // waits for our greeting before sending anything).  Instead submit the
  // auth job immediately; the executor thread performs the blocking I/O
  // via vio_io_wait()/poll() internally and signals us when done.
  {
    if (should_close(self)) {
      teardown(std::move(self));
      co_return;
    }
    {
      std::lock_guard<std::mutex> lk(self->cmd_mu);
      self->cmd_done.store(false, std::memory_order_relaxed);
      self->cmd_waiting = nullptr;
    }
    for (;;) {
      if (co_await AdmissionReady{self.get()}) break;
      g_admission_deferred.fetch_add(1, std::memory_order_relaxed);
      co_await self->np->sleep_for(std::chrono::milliseconds(1));
      if (should_close(self)) {
        teardown(std::move(self));
        co_return;
      }
    }
    bool auth_ok = false;
    submit_thd_job(self, [self, &auth_ok](THD *thd) {
      const MYSQL_SOCKET sock =
          thd->get_protocol_classic()->get_vio()->mysql_socket;
      mysql_socket_set_thread_owner(sock);
      auth_ok = !thd_prepare_connection(thd);
    });
    co_await CommandDone(self.get());

    if (self->connection_closed.load()) {
      teardown(std::move(self));
      co_return;
    }
    if (self->job_threw) {
      teardown(std::move(self));
      co_return;
    }
    if (!auth_ok) {
      teardown(std::move(self));
      co_return;
    }
  }

  // ---- Phase 2: Command loop ----
  // Set while the previous executed statement failed with a lock conflict
  // (deadlock victim / lock wait timeout): the next COMMIT or ROLLBACK
  // releases locks other transactions are waiting on and is prioritized.
  bool lock_conflict_victim = false;
  for (;;) {
    if (should_close(self)) break;

    // Authentication and the previous my_net_read() may have read the next
    // command into NET/VIO buffers.  Waiting on the kernel fd in that case
    // would deadlock: the bytes are no longer visible to epoll.
    bool readable = true;
    if (!has_buffered_input(self->thd)) {
      readable = co_await self->np->wait_readable(self->client_fd, self.get());
    }
    if (!readable || should_close(self)) break;

    // The socket may have been set to blocking by my_net_read during
    // the previous command.  Reset it to non-blocking so the epoll
    // registration stays valid.
    ensure_nonblocking(self);

    // Submit the command to the executor pool.  The executor thread
    // runs do_command() and signals us when done.
    {
      std::lock_guard<std::mutex> lk(self->cmd_mu);
      self->cmd_done.store(false, std::memory_order_relaxed);
      self->cmd_waiting = nullptr;
    }
    bool admitted = false;
    for (;;) {
      if (co_await AdmissionReady{self.get()}) {
        admitted = true;
        break;
      }
      g_admission_deferred.fetch_add(1, std::memory_order_relaxed);
      co_await self->np->sleep_for(std::chrono::milliseconds(1));
      if (should_close(self)) break;
    }
    if (should_close(self)) {
      if (admitted) admission_job_done();
      break;
    }

    COM_DATA com_data;
    enum enum_server_command command = COM_SLEEP;
    Parser_state parser_state;
    bool parsed = false;
    if (can_parse_on_reactor(self)) {
      const bool close = parse_on_reactor(self, &com_data, &command,
                                          &parser_state, &parsed);
      if (!parsed) admission_job_done();
      if (close || self->job_threw) break;
      if (!parsed) continue;
      g_reactor_parsed.fetch_add(1, std::memory_order_relaxed);
    }

    bool cmd_result = false;
    // Statement-priority taxonomy (D5): rare, globally beneficial
    // statements (KILL, post-lock-conflict COMMIT/ROLLBACK, UNLOCK
    // TABLES, RELEASE_LOCK) use the high-priority queue in addition to
    // the existing lock-holder path.  Requires the reactor parse, so it
    // is a no-op when coroutine_reactor_parse is OFF.
    stmt_priority::Reason stmt_prio = stmt_priority::Reason::None;
    if (parsed) {
      stmt_prio = stmt_priority::classify(
          stmt_priority_input(self->thd, command, lock_conflict_victim));
      if (stmt_prio != stmt_priority::Reason::None)
        stmt_priority::note_prioritized(stmt_prio);
    }
    const bool hp = stmt_prio != stmt_priority::Reason::None ||
                    sched_ext::is_lock_holder(self->thd);

    // Inline execution on the reactor thread (D5b): a reactor-safe
    // SELECT skips the executor handoff entirely (two cross-thread
    // wakeups, epoll re-arming) and runs right here while the packet is
    // hot in cache.  Plain TL_READ tables may still wait on MDL, so a
    // statement that exceeds coroutine_reactor_exec_budget_us puts this
    // connection on a 1s cooldown back to the executor path.  Requires
    // coroutine_reactor_parse; OFF by default.
    if (parsed && coroutine_reactor_exec &&
        stmt_prio == stmt_priority::Reason::None &&
        std::chrono::steady_clock::now() >=
            self->reactor_exec_cooldown_until &&
        parsed_select_is_reactor_safe(self->thd)) {
      THD *thd = self->thd;
      const auto t0 = std::chrono::steady_clock::now();
      thd_set_thread_stack(thd, reinterpret_cast<const char *>(&thd));
      thd->store_globals();
      bool close_conn = true;
      try {
        close_conn =
            execute_parsed_command(thd, &com_data, command, &parser_state);
      } catch (...) {
        LogErr(ERROR_LEVEL, ER_INTERNAL_ERROR,
               "coroutine reactor: uncaught exception in inline execution");
        self->job_threw = true;
      }
      thd->restore_globals();
      const auto took =
          std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now() - t0)
              .count();
      if ((ulonglong)took > coroutine_reactor_exec_budget_us) {
        self->reactor_exec_cooldown_until =
            std::chrono::steady_clock::now() + std::chrono::seconds(1);
        g_reactor_exec_slow.fetch_add(1, std::memory_order_relaxed);
      }
      // The admission credit was taken before parse; return it.
      admission_job_done();
      lock_conflict_victim = is_lock_conflict_victim(self->thd);
      g_reactor_exec.fetch_add(1, std::memory_order_relaxed);
      g_commands_completed.fetch_add(1, std::memory_order_relaxed);
      if (self->connection_closed.load() || self->job_threw || close_conn)
        break;
      continue;
    }

    if (parsed) {
      submit_thd_job(
          self,
          [&cmd_result, &com_data, command, &parser_state](THD *thd) {
            cmd_result =
                execute_parsed_command(thd, &com_data, command, &parser_state);
          },
          hp);
    } else {
      submit_thd_job(self, [&cmd_result](THD *thd) {
        cmd_result = do_command(thd);
      }, hp);
    }
    co_await CommandDone(self.get());

    // Sample the statement outcome for the victim heuristic.  The
    // executor job completed, so this coroutine owns the THD again;
    // reading the diagnostics area is race-free.
    lock_conflict_victim = is_lock_conflict_victim(self->thd);

    if (hp) g_hp_jobs_executed.fetch_add(1, std::memory_order_relaxed);
    g_commands_completed.fetch_add(1, std::memory_order_relaxed);

    if (self->connection_closed.load()) break;
    if (self->job_threw) break;  // executor job threw; tear down
    if (cmd_result) break;  // do_command requested disconnect
  }

  teardown(std::move(self));
}

}  // anonymous namespace

// -------------------------------------------------------------------
// Coroutine_connection_handler implementation
// -------------------------------------------------------------------

Coroutine_connection_handler::Coroutine_connection_handler() {
  // Lazy one-time initialisation of the global runtime.
  if (!s_initialized) {
    init();
  }
}

Coroutine_connection_handler::~Coroutine_connection_handler() {
  destroy();
}

void Coroutine_connection_handler::init() {
  if (s_initialized) return;
  /* Probe (once) whether a sched_ext scheduler is running with its maps
   * pinned.  When it is not (or WITH_SCX_SCHEDULER is OFF), this is a
   * no-op and the server keeps working normally. */
  sched_ext::init();
  try {
    size_t n_reactors = coroutine_reactor_threads ? coroutine_reactor_threads : 1;
    if (n_reactors > 64) n_reactors = 64;
    s_netpollers = new (std::nothrow) coroutine::Netpoller *[n_reactors]();
    s_reactor_threads = new (std::nothrow) std::thread *[n_reactors]();
    if (s_netpollers == nullptr || s_reactor_threads == nullptr) {
      delete[] s_netpollers;
      s_netpollers = nullptr;
      delete[] s_reactor_threads;
      s_reactor_threads = nullptr;
      return;
    }
    s_num_reactors = n_reactors;
    for (size_t i = 0; i < n_reactors; i++) {
      s_netpollers[i] = new (std::nothrow) coroutine::Netpoller();
      if (s_netpollers[i] == nullptr) {
        for (size_t j = 0; j < i; j++) delete s_netpollers[j];
        delete[] s_netpollers;
        s_netpollers = nullptr;
        delete[] s_reactor_threads;
        s_reactor_threads = nullptr;
        s_num_reactors = 0;
        return;
      }
    }

    size_t n = std::thread::hardware_concurrency();
    if (n == 0) n = 4;
    // The auto pool size leaves room for I/O-blocked workers; an explicit
    // coroutine_executor_threads value is used without a 4× multiplier.
    size_t pool_size = coroutine_executor_threads
                           ? (size_t)coroutine_executor_threads
                           : std::max(n * 4, size_t{16});
    size_t num_vip = (size_t)coroutine_executor_vip_workers;
    // When a DB-aware sched_ext scheduler is connected it owns placement
    // (workers are placed by tier/vtime policy); otherwise pin workers
    // round-robin over the allowed CPUs as a fallback.
    size_t cpu_offset = sched_ext::connected()
                            ? coroutine::ExecutorPool::kNoPinning
                            : 0;
    s_executor = new (std::nothrow) coroutine::ExecutorPool(pool_size,
                                                            cpu_offset,
                                                            num_vip);
    if (s_executor == nullptr) {
      for (size_t i = 0; i < s_num_reactors; i++) delete s_netpollers[i];
      delete[] s_netpollers;
      s_netpollers = nullptr;
      delete[] s_reactor_threads;
      s_reactor_threads = nullptr;
      s_num_reactors = 0;
      return;
    }

    // NOTE: we intentionally do NOT set a notify_reactor callback.  The
    // reactor is already woken by signal_command_done(), which posts the
    // coroutine resume callback for every completed job; a per-job empty
    // post here would only add a redundant eventfd write per command.

    coroutine::ExecutorPool::ThreadHooks hooks;
    hooks.on_start = []() {
      my_thread_init();
      // Register this worker thread in the sched_ext bridge so lock-wait
      // edges can be translated into the OS tid the BPF maps are keyed by.
      // No-op when WITH_SCX_SCHEDULER=OFF.
      sched_ext::thread_start();
    };
    hooks.on_stop = []() {
      sched_ext::thread_end();
      my_thread_end();
    };
    s_executor->set_thread_hooks(std::move(hooks));
    s_executor->start();

    for (size_t i = 0; i < n_reactors; i++) {
      s_reactor_threads[i] = new (std::nothrow) std::thread([i]() {
        my_thread_init();
        sched_ext::thread_start();
        /* Each reactor is a latency-critical dispatch point (epoll +
         * handoff + result writeback): mark it time-sensitive so the
         * DB-aware scheduler preempts executor and background work for it
         * instead of giving it a fair share of a busy CPU.  No-op without
         * a connected sched_ext scheduler. */
        sched_ext::set_thread_tier(sched_ext::kTierTS);
        if (s_netpollers[i]) s_netpollers[i]->run();
        sched_ext::clear_thread_tier();
        sched_ext::thread_end();
        my_thread_end();
      });
      if (s_reactor_threads[i] == nullptr) {
        s_executor->stop();
        s_executor->join();
        delete s_executor;
        s_executor = nullptr;
        for (size_t j = 0; j < s_num_reactors; j++) {
          if (s_netpollers[j]) s_netpollers[j]->stop();
        }
        for (size_t j = 0; j < i; j++) {
          if (s_reactor_threads[j]->joinable()) s_reactor_threads[j]->join();
          delete s_reactor_threads[j];
        }
        for (size_t j = 0; j < s_num_reactors; j++) delete s_netpollers[j];
        delete[] s_netpollers;
        s_netpollers = nullptr;
        delete[] s_reactor_threads;
        s_reactor_threads = nullptr;
        s_num_reactors = 0;
        return;
      }
    }

    // Install the vio pre-close hook so that vio_shutdown() (e.g. from
    // a KILL on another thread) defers closing coroutine-owned fds to
    // teardown() on the reactor thread.  Must be set before connections
    // can be accepted.
    vio_pre_close_hook = coroutine_vio_pre_close;

    s_initialized = true;
  } catch (...) {
    // If construction failed, clean up everything we have.
    if (s_executor) {
      s_executor->stop();
      s_executor->join();
      delete s_executor;
      s_executor = nullptr;
    }
    for (size_t i = 0; i < s_num_reactors; i++) {
      if (s_netpollers && s_netpollers[i]) s_netpollers[i]->stop();
    }
    for (size_t i = 0; i < s_num_reactors; i++) {
      if (s_reactor_threads && s_reactor_threads[i]) {
        if (s_reactor_threads[i]->joinable()) s_reactor_threads[i]->join();
        delete s_reactor_threads[i];
      }
      if (s_netpollers && s_netpollers[i]) delete s_netpollers[i];
    }
    delete[] s_netpollers;
    s_netpollers = nullptr;
    delete[] s_reactor_threads;
    s_reactor_threads = nullptr;
    s_num_reactors = 0;
    s_initialized = false;
  }
}

void Coroutine_connection_handler::destroy() {
  if (!s_initialized) return;
  s_initialized = false;

  // Detach the vio pre-close hook first: fds shut down from now on are
  // closed directly by vio_shutdown() again.
  vio_pre_close_hook = nullptr;

  // Stop the executor first so it stops posting to the netpollers.
  if (s_executor) {
    s_executor->stop();
    s_executor->join();
    delete s_executor;
    s_executor = nullptr;
  }

  // Stop every reactor and join every reactor thread.
  for (size_t i = 0; i < s_num_reactors; i++) {
    if (s_netpollers && s_netpollers[i]) s_netpollers[i]->stop();
  }
  for (size_t i = 0; i < s_num_reactors; i++) {
    if (s_reactor_threads && s_reactor_threads[i]) {
      if (s_reactor_threads[i]->joinable()) s_reactor_threads[i]->join();
      delete s_reactor_threads[i];
    }
    if (s_netpollers && s_netpollers[i]) delete s_netpollers[i];
  }
  delete[] s_netpollers;
  s_netpollers = nullptr;
  delete[] s_reactor_threads;
  s_reactor_threads = nullptr;
  s_num_reactors = 0;
}

bool Coroutine_connection_handler::add_connection(
    Channel_info *channel_info) {
  if (!s_initialized) {
    channel_info->send_error_and_close_channel(ER_CANT_CREATE_THREAD, 0, true);
    Connection_handler_manager::dec_connection_count();
    return true;
  }

  THD *thd = channel_info->create_thd();
  if (thd == nullptr) {
    channel_info->send_error_and_close_channel(ER_OUT_OF_RESOURCES, 0, false);
    Connection_handler_manager::dec_connection_count();
    return true;
  }

  thd->set_new_thread_id();

  // Channel_info is no longer needed; the THD has its own copy of
  // everything it needs.
  delete channel_info;

  Vio *vio = thd->get_protocol_classic()->get_vio();
  if (vio == nullptr) {
    Connection_handler_manager::dec_connection_count();
    delete thd;
    return true;
  }

  const MYSQL_SOCKET mysql_sock = vio->mysql_socket;
  int fd = mysql_socket_getfd(mysql_sock);
  if (fd < 0) {
    Connection_handler_manager::dec_connection_count();
    delete thd;
    return true;
  }

  // Set the socket to non-blocking for the epoll frontend.
  vio_set_blocking_flag(vio, false);

  auto state = std::make_shared<ConnectionState>();
  state->thd = thd;
  state->client_fd = fd;
  state->np = netpoller_for_fd(fd);
  if (state->np == nullptr) {
    Connection_handler_manager::dec_connection_count();
    delete thd;
    return true;
  }

  // Register the fd before the THD becomes visible to KILL: the vio
  // pre-close hook looks it up here to defer the close to teardown().
  {
    std::lock_guard<std::mutex> lk(g_fd_mu);
    g_fd_registry.emplace(fd, FdRegistration{state.get(), mysql_sock});
  }

  Global_THD_manager::get_instance()->add_thd(thd);

  // Spawn the connection coroutine on its owning reactor thread.
  state->np->post([state]() { state->run(); });

  g_active_coroutines.fetch_add(1, std::memory_order_relaxed);

  return false;
}

uint Coroutine_connection_handler::get_max_threads() const {
  return max_connections;
}

// -------------------------------------------------------------------
// Observability accessors
// -------------------------------------------------------------------

long long Coroutine_connection_handler::connections() {
  if (!s_initialized) return 0;
  return Connection_handler_manager::get_connection_count();
}

long long Coroutine_connection_handler::handoff_queue_depth() {
  if (s_executor == nullptr) return 0;
  return static_cast<long long>(s_executor->queued_jobs());
}

long long Coroutine_connection_handler::reactor_events() {
  long long total = 0;
  for (size_t i = 0; i < s_num_reactors; i++) {
    if (s_netpollers && s_netpollers[i])
      total += s_netpollers[i]->events_processed();
  }
  return total;
}

long long Coroutine_connection_handler::executor_threads() {
  if (s_executor == nullptr) return 0;
  return static_cast<long long>(s_executor->num_workers());
}

long long Coroutine_connection_handler::commands_completed() {
  return g_commands_completed.load(std::memory_order_relaxed);
}

long long Coroutine_connection_handler::connections_closed() {
  return g_connections_closed.load(std::memory_order_relaxed);
}

long long Coroutine_connection_handler::active_coroutines() {
  return g_active_coroutines.load(std::memory_order_relaxed);
}

long long Coroutine_connection_handler::hp_jobs_submitted() {
  return g_hp_jobs_submitted.load(std::memory_order_relaxed);
}

long long Coroutine_connection_handler::hp_jobs_executed() {
  return g_hp_jobs_executed.load(std::memory_order_relaxed);
}

long long Coroutine_connection_handler::admission_deferred() {
  return g_admission_deferred.load(std::memory_order_relaxed);
}

long long Coroutine_connection_handler::reactor_parsed() {
  return g_reactor_parsed.load(std::memory_order_relaxed);
}

long long Coroutine_connection_handler::reactor_exec() {
  return g_reactor_exec.load(std::memory_order_relaxed);
}

long long Coroutine_connection_handler::reactor_exec_slow() {
  return g_reactor_exec_slow.load(std::memory_order_relaxed);
}

long long Coroutine_connection_handler::submit_to_run_avg_us() {
  if (s_executor == nullptr) return 0;
  uint64_t count = s_executor->latency_count();
  if (count == 0) return 0;
  return static_cast<long long>(s_executor->latency_total_us() / count);
}

long long Coroutine_connection_handler::submit_to_run_total() {
  if (s_executor == nullptr) return 0;
  return static_cast<long long>(s_executor->latency_count());
}

long long Coroutine_connection_handler::submit_to_run_bucket(size_t i) {
  if (s_executor == nullptr) return 0;
  if (i >= coroutine::ExecutorPool::kNumLatencyBuckets) return 0;
  return static_cast<long long>(s_executor->latency_bucket_count(i));
}