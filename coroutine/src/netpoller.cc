#include "coroutine/netpoller.h"

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <algorithm>
#include <tuple>

#include "coroutine/executor.h"

namespace coroutine {

// -------------------------------------------------------------------
// Construction / destruction
// -------------------------------------------------------------------

Netpoller::Netpoller() {
  m_epoll_fd = epoll_create1(EPOLL_CLOEXEC);
  if (m_epoll_fd < 0) {
    throw std::runtime_error(
        std::string("Netpoller: epoll_create1 failed: ") +
        std::strerror(errno));
  }

  m_wake_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (m_wake_fd < 0) {
    close(m_epoll_fd);
    throw std::runtime_error(
        std::string("Netpoller: eventfd failed: ") + std::strerror(errno));
  }

  // Register the wake-up fd with epoll.  Use level-triggered EPOLLIN
  // (no EPOLLONESHOT) so that writing to the eventfd always wakes the
  // reactor, even if the previous event was not yet consumed.
  epoll_event ev{};
  ev.events = EPOLLIN;
  ev.data.fd = m_wake_fd;
  if (epoll_ctl(m_epoll_fd, EPOLL_CTL_ADD, m_wake_fd, &ev) < 0) {
    close(m_wake_fd);
    close(m_epoll_fd);
    throw std::runtime_error(
        std::string("Netpoller: epoll_ctl ADD wake_fd failed: ") +
        std::strerror(errno));
  }
}

Netpoller::~Netpoller() {
  if (m_epoll_fd >= 0) close(m_epoll_fd);
  if (m_wake_fd >= 0) close(m_wake_fd);
}

// -------------------------------------------------------------------
// Lifecycle
// -------------------------------------------------------------------

void Netpoller::run() {
  constexpr int kMaxEvents = 1024;
  std::vector<epoll_event> events(kMaxEvents);

  while (!m_stopped.load(std::memory_order_relaxed)) {
    // Compute timeout from the timer heap.
    int timeout_ms = -1;  // infinite
    if (!m_timers.empty()) {
      auto now = std::chrono::steady_clock::now();
      auto deadline = m_timers.front().deadline;
      auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
          deadline - now);
      timeout_ms = std::max(0, static_cast<int>(ms.count()));
    }

    int nfds = epoll_wait(m_epoll_fd, events.data(), kMaxEvents, timeout_ms);
    if (nfds < 0) {
      if (errno == EINTR) continue;
      break;  // fatal
    }
    m_events_processed.fetch_add(nfds, std::memory_order_relaxed);

    // Process IO events.
    for (int i = 0; i < nfds; ++i) {
      int fd = events[i].data.fd;
      uint32_t revents = events[i].events;

      if (fd == m_wake_fd) {
        // Drain the eventfd to avoid spurious wakeups.
        std::uint64_t val;
        if (read(m_wake_fd, &val, sizeof(val)) < 0 && errno != EAGAIN) {
          // Ignore.
        }
        drain_callbacks();
        continue;
      }

      // Snapshot waiters before any resume, then detach them from the
      // entry so that re-entrant add_interest() / cancel_fd() are safe.
      // The map entry itself stays registered: EPOLLONESHOT has disarmed
      // the fd, and the next wait on this fd re-arms it with a single
      // EPOLL_CTL_MOD (no DEL/ADD pair, no map insert/erase, no
      // allocation).  Entries are removed only by cancel_fd().
      auto it = m_fds.find(fd);
      if (it == m_fds.end()) continue;

      bool want_read = (revents & (EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR))
                       && it->second.read_waiter;
      bool want_write = (revents & (EPOLLOUT | EPOLLHUP | EPOLLERR))
                        && it->second.write_waiter;

      if (!want_read && !want_write) continue;

      std::coroutine_handle<> read_h, write_h;
      if (want_read) {
        read_h = it->second.read_waiter;
        if (it->second.read_result) *it->second.read_result = true;
        it->second.read_waiter = nullptr;
        it->second.read_result = nullptr;
      }
      if (want_write) {
        write_h = it->second.write_waiter;
        if (it->second.write_result) *it->second.write_result = true;
        it->second.write_waiter = nullptr;
        it->second.write_result = nullptr;
      }

      // Now resume (safe: map state is already cleaned up).
      if (read_h) read_h.resume();
      if (write_h) write_h.resume();
    }

    // Fire expired timers.
    fire_timers();
  }
}

void Netpoller::stop() {
  m_stopped.store(true, std::memory_order_relaxed);
  std::uint64_t val = 1;
  if (write(m_wake_fd, &val, sizeof(val)) < 0) {
    // Ignore.
  }
}

void Netpoller::post(std::function<void()> fn) {
  bool was_empty;
  {
    std::lock_guard<std::mutex> lock(m_post_mu);
    was_empty = m_post_queue.empty();
    m_post_queue.push_back(std::move(fn));
  }
  if (was_empty) {
    std::uint64_t val = 1;
    if (write(m_wake_fd, &val, sizeof(val)) < 0) {
      // Ignore.
    }
  }
}

// -------------------------------------------------------------------
// Awaiters
// -------------------------------------------------------------------

Netpoller::ReadableAwaiter Netpoller::wait_readable(int fd,
                                                    const void *token) {
  return ReadableAwaiter(this, fd, token);
}

Netpoller::WritableAwaiter Netpoller::wait_writable(int fd,
                                                    const void *token) {
  return WritableAwaiter(this, fd, token);
}

Netpoller::SleepAwaiter Netpoller::sleep_for(std::chrono::milliseconds d) {
  return SleepAwaiter(this, d);
}

// -------------------------------------------------------------------
// Cancel
// -------------------------------------------------------------------

void Netpoller::cancel_fd(int fd) {
  auto it = m_fds.find(fd);
  if (it == m_fds.end()) return;
  cancel_fd_entry(it);
}

void Netpoller::cancel_fd(int fd, const void *token) {
  auto it = m_fds.find(fd);
  if (it == m_fds.end()) return;
  // Stale cancellation: the fd number was recycled and the waiters now
  // registered here belong to a different connection.  Leave them alone.
  if (it->second.read_token != token && it->second.write_token != token)
    return;
  cancel_fd_entry(it);
}

void Netpoller::cancel_fd_async(int fd, const void *token) {
  post([this, fd, token] { cancel_fd(fd, token); });
}

void Netpoller::cancel_fd_entry(
    std::unordered_map<int, FdInterest>::iterator it) {
  int fd = it->first;
  auto read_h = it->second.read_waiter;
  auto write_h = it->second.write_waiter;
  if (it->second.read_result) {
    *it->second.read_result = false;
    it->second.read_result = nullptr;
  }
  if (it->second.write_result) {
    *it->second.write_result = false;
    it->second.write_result = nullptr;
  }
  it->second.read_waiter = nullptr;
  it->second.write_waiter = nullptr;

  m_fds.erase(it);
  epoll_ctl(m_epoll_fd, EPOLL_CTL_DEL, fd, nullptr);

  if (read_h) read_h.resume();
  if (write_h) write_h.resume();
}

// -------------------------------------------------------------------
// Internal helpers
// -------------------------------------------------------------------

bool Netpoller::add_interest(int fd, uint32_t events,
                             std::coroutine_handle<> h, bool *result,
                             const void *token) {
  auto &fi = m_fds[fd];
  if (events & (EPOLLIN | EPOLLRDHUP)) {
    assert(!fi.read_waiter &&
           "A read waiter is already registered for this fd");
    fi.read_waiter = h;
    fi.read_token = token;
    fi.read_result = result;
  }
  if (events & EPOLLOUT) {
    assert(!fi.write_waiter &&
           "A write waiter is already registered for this fd");
    fi.write_waiter = h;
    fi.write_token = token;
    fi.write_result = result;
  }

  // Always apply with EPOLLET | EPOLLONESHOT.  The caller is expected
  // to set events flags without these; we add them here.  A persistent
  // entry is still registered (disarmed) in epoll, so re-arming takes
  // the EEXIST -> MOD path in apply_epoll().
  uint32_t new_events = events | EPOLLET | EPOLLONESHOT;
  if (!apply_epoll(fd, new_events, 0)) {
    // The fd is already dead (e.g. closed by a peer thread): complete
    // the waiters with a cancellation result instead of leaving them
    // registered (and the coroutines suspended) forever.  The waiter
    // being added resumes via await_suspend() returning false; any
    // opposite-direction waiter is resumed here.
    if (fi.read_result) *fi.read_result = false;
    if (fi.write_result) *fi.write_result = false;
    std::coroutine_handle<> other =
        (events & EPOLLOUT) ? fi.read_waiter : fi.write_waiter;
    fi.read_waiter = nullptr;
    fi.write_waiter = nullptr;
    fi.read_result = nullptr;
    fi.write_result = nullptr;
    m_fds.erase(fd);
    if (other) other.resume();
    return false;
  }
  return true;
}

bool Netpoller::apply_epoll(int fd, uint32_t new_events,
                            uint32_t) {
  // Try ADD first; a persistent entry that was already registered (its
  // EPOLLONESHOT mask disarmed after the last event) re-arms via MOD.
  // Both paths are hit on the reactor thread only, so there is no
  // registration race.

  epoll_event ev{};
  ev.events = new_events;
  ev.data.fd = fd;
  if (epoll_ctl(m_epoll_fd, EPOLL_CTL_ADD, fd, &ev) == 0) return true;
  if (errno == EEXIST) {
    return epoll_ctl(m_epoll_fd, EPOLL_CTL_MOD, fd, &ev) == 0;
  }
  // Other errors (EBADF/ENOENT/...) mean the fd cannot be registered;
  // the caller completes the waiter with an error result.
  return false;
}

void Netpoller::drain_callbacks() {
  std::deque<std::function<void()>> batch;
  {
    std::lock_guard<std::mutex> lock(m_post_mu);
    m_post_queue.swap(batch);
  }
  for (auto &fn : batch) {
    if (fn) fn();
  }
}

void Netpoller::fire_timers() {
  auto now = std::chrono::steady_clock::now();
  while (!m_timers.empty() && m_timers.front().deadline <= now) {
    auto h = m_timers.front().handle;
    std::pop_heap(m_timers.begin(), m_timers.end(), std::greater<>());
    m_timers.pop_back();
    if (h) h.resume();
  }
}

}  // namespace coroutine