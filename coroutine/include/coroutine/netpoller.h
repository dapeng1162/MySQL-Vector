#pragma once

#include <coroutine>
#include <cstdint>
#include <cassert>
#include <atomic>
#include <functional>
#include <memory>
#include <unordered_map>
#include <deque>
#include <mutex>
#include <vector>
#include <chrono>
#include <sys/epoll.h>
#include <sys/eventfd.h>

namespace coroutine {

/**
 * @brief Single-threaded epoll-driven reactor for the coroutine front-end.
 *
 * All public methods (except `post()` and `stop()`) must be called from
 * the reactor thread, i.e. the thread that runs `run()`.  Cross-thread
 * notifications arrive via the wake-up eventfd and are drained as
 * callbacks in the run loop.
 *
 * Awaiters
 * --------
 * The netpoller exposes three awaiter types:
 *   - ReadableAwaiter    — co_await poller.wait_readable(fd)
 *   - WritableAwaiter    — co_await poller.wait_writable(fd)
 *   - SleepAwaiter       — co_await poller.sleep_for(duration)
 *
 * Timer management
 * ----------------
 * Timers are kept in a min-heap (deadline + sequence number).  The
 * epoll_wait timeout is set to the next timer deadline each iteration.
 *
 * Thread safety
 * -------------
 * `post()`, `stop()` and `cancel_fd_async()` are the only thread-safe
 * methods.  Everything else must be called from the reactor thread.
 */
class Netpoller {
 public:
  Netpoller();
  ~Netpoller();

  // Non-copyable, non-movable.
  Netpoller(const Netpoller &) = delete;
  Netpoller &operator=(const Netpoller &) = delete;

  // ------------------------------------------------------------------
  // Lifecycle (called from the reactor thread)
  // ------------------------------------------------------------------

  /** Run the event loop until stop() is called. */
  void run();

  /** Request the event loop to exit.  Thread-safe. */
  void stop();

  /**
   * Schedule a callback to run on the reactor thread.  Thread-safe.
   * The callback is enqueued in a mutex-guarded queue and the reactor
   * is woken via eventfd.
   */
  void post(std::function<void()> fn);

  // ------------------------------------------------------------------
  // Awaiters (used from coroutines running on the reactor thread)
  // ------------------------------------------------------------------

  class ReadableAwaiter;
  ReadableAwaiter wait_readable(int fd, const void *token = nullptr);

  class WritableAwaiter;
  WritableAwaiter wait_writable(int fd, const void *token = nullptr);

  class SleepAwaiter;
  SleepAwaiter sleep_for(std::chrono::milliseconds d);

  // ------------------------------------------------------------------
  // Cancel & cleanup
  // ------------------------------------------------------------------

  /** Remove all waiters for a given fd.  Must be called from reactor
   *  thread.  The waiters are resumed with an error result. */
  void cancel_fd(int fd);

  /** Token-checked variant of cancel_fd(): the cancellation is ignored
   *  when the waiters currently registered for fd carry a different
   *  token.  This protects against stale cancellations posted before
   *  the fd number was recycled for a new connection.  Must be called
   *  from the reactor thread. */
  void cancel_fd(int fd, const void *token);

  /** Thread-safe cancellation: schedules a token-checked cancel_fd()
   *  on the reactor thread via the post queue.  Used by foreign threads
   *  (e.g. a KILL shutting down the socket) to wake a suspended
   *  coroutine. */
  void cancel_fd_async(int fd, const void *token);

  /** Current number of active fd registrations (for diagnostics). */
  size_t fd_count() const { return m_fds.size(); }

  /** Current number of pending timers (for diagnostics). */
  size_t timer_count() const { return m_timers.size(); }

  /** Number of epoll events processed since startup (for diagnostics). */
  long long events_processed() const { return m_events_processed.load(); }

 private:
  friend class ReadableAwaiter;
  friend class WritableAwaiter;
  friend class SleepAwaiter;

  // ------------------------------------------------------------------
  // Internal types
  // ------------------------------------------------------------------

  struct TimerEntry {
    std::chrono::steady_clock::time_point deadline;
    std::coroutine_handle<> handle;
    uint64_t seq;
    bool operator>(const TimerEntry &o) const {
      return std::tie(deadline, seq) > std::tie(o.deadline, o.seq);
    }
  };

  struct FdInterest {
    std::coroutine_handle<> read_waiter;
    std::coroutine_handle<> write_waiter;
    // Opaque identity of the connection that owns each waiter (e.g. a
    // ConnectionState pointer).  Token-checked cancel_fd() compares
    // against these to ignore stale cancellations after fd recycling.
    const void *read_token{nullptr};
    const void *write_token{nullptr};
    // Shared cancellation flag for each direction.  Set by cancel_fd()
    // before resuming the waiter so the awaiter can report the result.
    std::shared_ptr<bool> read_cancelled;
    std::shared_ptr<bool> write_cancelled;
  };

  // ------------------------------------------------------------------
  // Internal helpers
  // ------------------------------------------------------------------

  /** Register interest in events on an fd and store the coroutine handle
   *  to resume when the event fires.  fd must not already have a waiter
   *  for the same direction.  Returns false when the epoll registration
   *  failed (e.g. the fd was closed by another thread); in that case the
   *  waiter is completed with a cancellation result instead of being
   *  left registered (and suspended) forever. */
  bool add_interest(int fd, uint32_t events, std::coroutine_handle<> h,
                    std::shared_ptr<bool> cancelled, const void *token);

  /** Apply the current epoll events mask to the epoll fd.
   *  Returns false when both ADD and MOD failed. */
  bool apply_epoll(int fd, uint32_t new_events, uint32_t old_events);

  /** Shared implementation of cancel_fd(): erase the map entry, remove
   *  the fd from epoll and resume the waiters with a cancelled result. */
  void cancel_fd_entry(std::unordered_map<int, FdInterest>::iterator it);

  /** Drain the completion queue (callbacks posted from other threads). */
  void drain_callbacks();

  /** Fire all timers whose deadline has passed. */
  void fire_timers();

  int m_epoll_fd{-1};
  int m_wake_fd{-1};

  std::atomic<bool> m_stopped{false};

  // ---- Reactor-thread-only state ----
  std::unordered_map<int, FdInterest> m_fds;
  std::vector<TimerEntry> m_timers;  // min-heap (pop_heap on root)
  uint64_t m_timer_seq{0};

  // ---- Cumulative counter (written from reactor, read from any thread) ----
  std::atomic<long long> m_events_processed{0};

  // ---- Cross-thread post queue ----
  std::mutex m_post_mu;
  std::deque<std::function<void()>> m_post_queue;
};

// ------------------------------------------------------------------
// Awaiter implementations
// ------------------------------------------------------------------

class Netpoller::ReadableAwaiter {
 public:
  ReadableAwaiter(Netpoller *poller, int fd, const void *token)
      : m_poller(poller), m_fd(fd), m_token(token) {}

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> h) {
    m_cancelled = std::make_shared<bool>(false);
    // A failed registration marks the waiter cancelled; returning false
    // resumes the coroutine immediately with an error result.
    return m_poller->add_interest(m_fd, EPOLLIN | EPOLLET | EPOLLONESHOT, h,
                                  m_cancelled, m_token);
  }

  /** Returns true if the fd is readable, false if cancelled/timed out
   *  or if the epoll registration failed. */
  bool await_resume() const noexcept {
    return !(m_cancelled && *m_cancelled);
  }

 private:
  friend class Netpoller;
  Netpoller *m_poller;
  int m_fd;
  const void *m_token;
  std::shared_ptr<bool> m_cancelled;
};

class Netpoller::WritableAwaiter {
 public:
  WritableAwaiter(Netpoller *poller, int fd, const void *token)
      : m_poller(poller), m_fd(fd), m_token(token) {}

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> h) {
    m_cancelled = std::make_shared<bool>(false);
    return m_poller->add_interest(m_fd, EPOLLOUT | EPOLLET | EPOLLONESHOT, h,
                                  m_cancelled, m_token);
  }

  bool await_resume() const noexcept {
    return !(m_cancelled && *m_cancelled);
  }

 private:
  friend class Netpoller;
  Netpoller *m_poller;
  int m_fd;
  const void *m_token;
  std::shared_ptr<bool> m_cancelled;
};

class Netpoller::SleepAwaiter {
 public:
  SleepAwaiter(Netpoller *poller, std::chrono::milliseconds dur)
      : m_poller(poller), m_dur(dur) {}

  bool await_ready() const noexcept { return m_dur.count() <= 0; }

  bool await_suspend(std::coroutine_handle<> h) {
    m_poller->m_timers.push_back(
        TimerEntry{std::chrono::steady_clock::now() + m_dur, h,
                   m_poller->m_timer_seq++});
    std::push_heap(m_poller->m_timers.begin(), m_poller->m_timers.end(),
                   std::greater<>());
    return true;
  }

  void await_resume() const noexcept {}

 private:
  Netpoller *m_poller;
  std::chrono::milliseconds m_dur;
};

}  // namespace coroutine