#include "coroutine/executor.h"

#include <sched.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <system_error>

namespace coroutine {

// -------------------------------------------------------------------
// Construction / destruction
// -------------------------------------------------------------------

ExecutorPool::ExecutorPool(size_t num_workers, size_t cpu_offset,
                           size_t num_vip)
    : m_cpu_offset(cpu_offset), m_num_vip(num_vip) {
  if (num_workers == 0) {
    throw std::invalid_argument("ExecutorPool: num_workers must be >= 1");
  }
  if (num_vip >= num_workers) {
    // At least one worker must remain on the normal queue, otherwise
    // normal jobs would starve permanently.
    num_vip = num_workers - 1;
  }
  m_num_vip = num_vip;
  m_workers.reserve(num_workers);
  for (size_t i = 0; i < num_workers; ++i) {
    m_workers.push_back(std::make_unique<Worker>());
  }
}

ExecutorPool::~ExecutorPool() {
  stop();
  join();
}

// -------------------------------------------------------------------
// Lifecycle
// -------------------------------------------------------------------

void ExecutorPool::start() {
  for (size_t i = 0; i < m_workers.size(); ++i) {
    m_workers[i]->thread = std::thread(&ExecutorPool::worker_loop, this, i);
  }
}

void ExecutorPool::stop() {
  {
    std::lock_guard<std::mutex> lock(m_mu);
    m_stopping = true;
  }
  m_cv.notify_all();
}

void ExecutorPool::join() {
  for (auto &w : m_workers) {
    if (w->thread.joinable()) {
      w->thread.join();
    }
  }
}

// -------------------------------------------------------------------
// Job submission
// -------------------------------------------------------------------

void ExecutorPool::submit(Job job, bool high_priority) {
  if (!job) return;
  bool notify = false;
  {
    std::lock_guard<std::mutex> lock(m_mu);
    if (high_priority) {
      notify = m_hp_queue.empty();
      m_hp_queue.push_back(std::move(job));
    } else {
      // A normal job can be consumed by any worker, so we must wake a
      // worker whenever no job of either kind was queued before (an idle
      // non-VIP worker might have gone back to sleep after being woken by
      // a normal submission while the queue was momentarily empty).
      notify = m_queue.empty() && m_hp_queue.empty();
      m_queue.push_back(std::move(job));
    }
  }
  if (notify) {
    // Wake a single worker on the empty -> non-empty transition.  A burst
    // of jobs is spread across workers by the cascade notify in
    // worker_loop() (each worker wakes another while the queue holds more
    // than one job), so we never let one worker monopolize a burst and then
    // block on I/O with everyone else asleep — and we avoid the thundering
    // herd of waking all N workers for every single job.
    //
    // NOTE: notify_one() may hand the wakeup to a VIP worker that cannot
    // consume a normal job (its predicate fails and it goes back to
    // sleep).  That is a lost wakeup for the normal queue.  With num_vip
    // VIP workers and (N - num_vip) normal workers, worst case we burn
    // num_vip spurious wakeups; if num_vip >= 1 we therefore notify
    // num_vip + 1 times so at least one non-VIP worker is guaranteed to
    // wake.  The cascade notify inside worker_loop() spreads any surplus.
    size_t wakes = 1;
    if (!high_priority && m_num_vip > 0) wakes = m_num_vip + 1;
    for (size_t i = 0; i < wakes; ++i) {
      m_cv.notify_one();
    }
  }
}

size_t ExecutorPool::queued_jobs() const {
  std::lock_guard<std::mutex> lock(m_mu);
  return m_queue.size() + m_hp_queue.size();
}

// -------------------------------------------------------------------
// Worker loop
// -------------------------------------------------------------------

void ExecutorPool::worker_loop(size_t idx) {
  auto &w = m_workers[idx];
  const bool vip = idx < m_num_vip;

  // Pin to CPU (cpu_offset + idx), unless disabled.
  if (m_cpu_offset != static_cast<size_t>(-1)) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(static_cast<int>(m_cpu_offset + idx), &cpuset);
    int rc = sched_setaffinity(0, sizeof(cpuset), &cpuset);
    if (rc != 0) {
      // Non-fatal: log and continue (handled by the wrapper).
    }
  }

  // Call the on_start hook (my_thread_init, PSI registration).
  if (m_thread_hooks.on_start) {
    m_thread_hooks.on_start();
  }

  // Main loop.
  for (;;) {
    Job job;
    {
      std::unique_lock<std::mutex> lock(m_mu);
      // Drain all queued jobs before exiting on stop.
      m_cv.wait(lock, [this, &w, vip] {
        return m_stopping || w->stopped.load(std::memory_order_relaxed) ||
               !m_hp_queue.empty() ||
               (!vip && !m_queue.empty());
      });
      // High-priority queue first (all workers); VIP workers stop here.
      if (!m_hp_queue.empty()) {
        // Cascade wake-up: while more than one HP job remains, wake
        // another worker so a burst is shared across the pool.
        if (m_hp_queue.size() > 1) {
          m_cv.notify_one();
        }
        job = std::move(m_hp_queue.front());
        m_hp_queue.pop_front();
      } else if (!vip && !m_queue.empty()) {
        // Cascade wake-up for the normal queue.
        if (m_queue.size() > 1) {
          m_cv.notify_one();
        }
        job = std::move(m_queue.front());
        m_queue.pop_front();
      } else {
        // Nothing left to do.  Exit if stop was requested, else wait
        // again for new work.
        if (m_stopping || w->stopped.load(std::memory_order_relaxed)) break;
        continue;  // spurious wakeup
      }
    }

    job();
    if (m_notify_reactor) {
      m_notify_reactor();
    }
  }

  // Call the on_stop hook.
  if (m_thread_hooks.on_stop) {
    m_thread_hooks.on_stop();
  }
}

}  // namespace coroutine