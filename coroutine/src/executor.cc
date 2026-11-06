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
  const auto now = std::chrono::steady_clock::now();
  bool notify = false;
  {
    std::lock_guard<std::mutex> lock(m_mu);
    if (high_priority) {
      notify = m_hp_queue.empty();
      m_hp_queue.push_back(QueuedJob{std::move(job), now});
    } else {
      // A normal job can be consumed by any worker, so we must wake a
      // worker whenever no job of either kind was queued before (an idle
      // non-VIP worker might have gone back to sleep after being woken by
      // a normal submission while the queue was momentarily empty).
      notify = m_queue.empty() && m_hp_queue.empty();
      m_queue.push_back(QueuedJob{std::move(job), now});
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

namespace {

/**
 * Return the @c rank-th CPU present in @c allowed (0-based), or -1 if
 * fewer than rank+1 CPUs are set.
 */
int nth_cpu(const cpu_set_t &allowed, int rank) {
  for (int cpu = 0, seen = 0; cpu < CPU_SETSIZE; ++cpu) {
    if (CPU_ISSET(cpu, &allowed)) {
      if (seen == rank) return cpu;
      ++seen;
    }
  }
  return -1;
}

/** Upper edges (µs) of the submit → run latency buckets; a latency
 *  greater than the last edge falls into the open-ended final bucket. */
constexpr uint64_t kLatencyEdgesUs[ExecutorPool::kNumLatencyBuckets - 1] = {
    5, 10, 25, 50, 100, 500};

size_t latency_bucket_of(uint64_t us) {
  for (size_t i = 0; i < ExecutorPool::kNumLatencyBuckets - 1; ++i) {
    if (us <= kLatencyEdgesUs[i]) return i;
  }
  return ExecutorPool::kNumLatencyBuckets - 1;
}

}  // namespace

void ExecutorPool::worker_loop(size_t idx) {
  auto &w = m_workers[idx];
  const bool vip = idx < m_num_vip;

  // Pin to a CPU from the process's allowed set, round-robin:
  // allowed[(cpu_offset + idx) % n_allowed].  The old scheme
  // (cpu = cpu_offset + idx) silently failed for idx >= n_allowed on
  // oversubscribed pools (4× cores): sched_setaffinity() returned EINVAL
  // and most workers floated unpinned.  Basing placement on the allowed
  // set also keeps working when the server is restricted with taskset.
  // kNoPinning disables pinning entirely (a DB-aware sched_ext scheduler
  // owns placement in that case).
  if (m_cpu_offset != kNoPinning) {
    cpu_set_t allowed;
    CPU_ZERO(&allowed);
    if (sched_getaffinity(0, sizeof(allowed), &allowed) == 0) {
      int n_allowed = CPU_COUNT(&allowed);
      int cpu = n_allowed > 0
                    ? nth_cpu(allowed,
                              static_cast<int>((m_cpu_offset + idx) %
                                               static_cast<size_t>(n_allowed)))
                    : -1;
      if (cpu >= 0) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(cpu, &cpuset);
        if (sched_setaffinity(0, sizeof(cpuset), &cpuset) != 0) {
          fprintf(stderr,
                  "mysql_coroutine: ExecutorPool worker %zu: "
                  "sched_setaffinity(cpu %d) failed: %s\n",
                  idx, cpu, strerror(errno));
        }
      }
    } else {
      fprintf(stderr,
              "mysql_coroutine: ExecutorPool worker %zu: "
              "sched_getaffinity failed: %s\n",
              idx, strerror(errno));
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
      QueuedJob qj;
      // High-priority queue first (all workers); VIP workers stop here.
      if (!m_hp_queue.empty()) {
        // Cascade wake-up: while more than one HP job remains, wake
        // another worker so a burst is shared across the pool.
        if (m_hp_queue.size() > 1) {
          m_cv.notify_one();
        }
        qj = std::move(m_hp_queue.front());
        m_hp_queue.pop_front();
      } else if (!vip && !m_queue.empty()) {
        // Cascade wake-up for the normal queue.
        if (m_queue.size() > 1) {
          m_cv.notify_one();
        }
        qj = std::move(m_queue.front());
        m_queue.pop_front();
      } else {
        // Nothing left to do.  Exit if stop was requested, else wait
        // again for new work.
        if (m_stopping || w->stopped.load(std::memory_order_relaxed)) break;
        continue;  // spurious wakeup
      }
      job = std::move(qj.job);

      // Record the submit → run latency: from submit()'s enqueue
      // timestamp to now, when a worker picked the job up.  This covers
      // both the userspace queue wait and the OS run-queue wait of the
      // woken worker, i.e. the double-scheduling delay that
      // queued_jobs() cannot observe.
      auto lat_us = std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - qj.enqueued_at)
                        .count();
      m_lat_buckets[latency_bucket_of(
          static_cast<uint64_t>(lat_us < 0 ? 0 : lat_us))]
          .fetch_add(1, std::memory_order_relaxed);
      m_lat_total_us.fetch_add(static_cast<uint64_t>(lat_us < 0 ? 0 : lat_us),
                               std::memory_order_relaxed);
      m_lat_count.fetch_add(1, std::memory_order_relaxed);
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