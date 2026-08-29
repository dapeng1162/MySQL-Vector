#pragma once

#include <coroutine>
#include <exception>
#include <memory>
#include <utility>

namespace coroutine {

/**
 * @brief A lazy-start Task type for C++20 stackless coroutines.
 *
 * Task is move-only. After construction the coroutine is suspended at the
 * initial suspend point; call start() to begin execution.  The frame
 * stays alive until the Task destructor runs (final_suspend returns
 * suspend_always), so the exception pointer can be read via
 * rethrow_if_exception() after the coroutine completes.
 *
 * Usage:
 *   Task my_coro() { co_await something; co_return; }
 *   auto t = my_coro();
 *   t.start();
 *   t.rethrow_if_exception();   // safe after completion
 *
 * Fire-and-forget: use DetachedTask instead.
 */
class Task {
 public:
  struct promise_type {
    std::exception_ptr m_exception{nullptr};

    Task get_return_object() noexcept {
      return Task{
          std::coroutine_handle<promise_type>::from_promise(*this)};
    }

    std::suspend_always initial_suspend() noexcept { return {}; }
    std::suspend_always final_suspend() noexcept { return {}; }

    void unhandled_exception() noexcept {
      m_exception = std::current_exception();
    }

    void return_void() noexcept {}
  };

  Task() noexcept = default;

  Task(const Task &) = delete;
  Task &operator=(const Task &) = delete;

  Task(Task &&other) noexcept
      : m_handle(std::exchange(other.m_handle, nullptr)) {}

  Task &operator=(Task &&other) noexcept {
    if (this != &other) {
      destroy();
      m_handle = std::exchange(other.m_handle, nullptr);
    }
    return *this;
  }

  ~Task() { destroy(); }

  /** Resume the coroutine from its initial suspend point. */
  void start() {
    if (m_handle) m_handle.resume();
  }

  /**
   * Rethrow an exception stored by the coroutine, if any.
   * Safe to call only after the coroutine has completed (i.e. after
   * start() has returned and the coroutine has run to completion
   * synchronously, or after the continuation has been resumed).
   */
  void rethrow_if_exception() {
    if (m_handle && m_handle.promise().m_exception) {
      std::rethrow_exception(m_handle.promise().m_exception);
    }
  }

  explicit operator bool() const noexcept { return m_handle != nullptr; }

 private:
  void destroy() {
    if (m_handle) {
      m_handle.destroy();
      m_handle = nullptr;
    }
  }

  explicit Task(std::coroutine_handle<promise_type> h) : m_handle(h) {}

  std::coroutine_handle<promise_type> m_handle{nullptr};
};

/**
 * @brief Fire-and-forget coroutine type that starts immediately and
 *        self-destructs on completion.
 *
 * initial_suspend returns suspend_never so execution begins as soon as
 * the coroutine is created.  final_suspend returns suspend_never so the
 * frame is destroyed automatically on completion.  Thrown exceptions
 * terminate the process.
 *
 * Use this for long-lived connection coroutines that do not need to be
 * joined or cancelled from outside (cancellation is done via the
 * netpoller, which holds the coroutine handle while it is suspended).
 */
struct DetachedTask {
  struct promise_type {
    DetachedTask get_return_object() noexcept { return {}; }
    std::suspend_never initial_suspend() noexcept { return {}; }
    std::suspend_never final_suspend() noexcept { return {}; }
    void unhandled_exception() noexcept { std::terminate(); }
    void return_void() noexcept {}
  };
};

}  // namespace coroutine