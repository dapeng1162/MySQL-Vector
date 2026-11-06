#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

namespace coroutine {

/**
 * @brief Fixed thread pool for executing THD-bound queries.
 *
 * All workers share a single task queue guarded by a mutex, so any idle
 * worker can consume any job.  This avoids the load-imbalance problem of
 * per-worker round-robin queues (where jobs pile up on workers that are
 * currently blocked inside a query's I/O wait while other workers sit
 * idle).  Workers block on a condition variable when the queue is empty.
 *
 * Workers are created with `my_thread_init()` / `my_thread_end()`
 * callbacks (set via set_thread_hooks) so that mysys TLS and PSI
 * instrumentation are properly set up.
 *
 * Observability: the pool records, for every job, the submit → run
 * latency (time from submit() enqueuing the job to a worker picking it
 * up).  This includes both the userspace queue wait and the OS-level
 * run-queue wait (a woken worker may not reach the CPU immediately), and
 * is therefore the gauge for the double-scheduling blind spot that
 * queued_jobs() cannot see.
 */
class ExecutorPool {
 public:
  using Job = std::function<void()>;

  struct ThreadHooks {
    std::function<void()> on_start;  // called at the top of each worker loop
    std::function<void()> on_stop;   // called just before worker exits
  };

  /** Marker value for cpu_offset that disables worker CPU pinning. */
  static constexpr size_t kNoPinning = static_cast<size_t>(-1);

  /** Number of submit → run latency histogram buckets.  Edges in µs:
   *  (0,5] (5,10] (10,25] (25,50] (50,100] (100,500] (500,∞). */
  static constexpr size_t kNumLatencyBuckets = 7;

  /**
   * @param num_workers   Number of OS threads in the pool.
   * @param cpu_offset    First index into the allowed-CPU list for pinning:
   *                      worker i is pinned to allowed[(cpu_offset + i) %
   *                      n_allowed].  Pinning never targets a CPU outside
   *                      the process affinity mask, so oversubscribed pools
   *                      (4× cores) share cores round-robin instead of
   *                      failing sched_setaffinity() silently.  Pass
   *                      kNoPinning to let the OS scheduler place workers
   *                      (used when a DB-aware sched_ext scheduler owns
   *                      placement).
   * @param num_vip       Number of dedicated VIP workers.  Worker 0..num_vip-1
   *                      consume ONLY the high-priority queue; the rest
   *                      consume the high-priority queue first, then the
   *                      normal queue.  This guarantees that high-priority
   *                      jobs (e.g. a lock holder's COMMIT) still get a
   *                      thread even when every normal worker is blocked
   *                      inside a lock wait (thread-pool deadlock storm).
   */
  explicit ExecutorPool(size_t num_workers, size_t cpu_offset = 0,
                        size_t num_vip = 1);

  ~ExecutorPool();

  // Non-copyable, non-movable.
  ExecutorPool(const ExecutorPool &) = delete;
  ExecutorPool &operator=(const ExecutorPool &) = delete;

  /** Start all worker threads. */
  void start();

  /** Request all workers to stop (non-blocking). */
  void stop();

  /** Block until all workers have exited. */
  void join();

  /**
   * Enqueue a job to the shared queue.  Thread-safe.  A single worker
   * is woken to process it.
   * @param high_priority  When true the job goes to the high-priority
   *                       queue, consumed before all normal jobs.
   */
  void submit(Job job, bool high_priority = false);

  size_t num_workers() const { return m_workers.size(); }

  /** Current number of queued (not yet started) jobs. */
  size_t queued_jobs() const;

  /** Submit → run latency histogram: count in bucket @c i
   *  (i < kNumLatencyBuckets; the last bucket is open-ended). */
  uint64_t latency_bucket_count(size_t i) const {
    return m_lat_buckets[i].load(std::memory_order_relaxed);
  }

  /** Total submit → run latency accumulated, in microseconds. */
  uint64_t latency_total_us() const {
    return m_lat_total_us.load(std::memory_order_relaxed);
  }

  /** Number of jobs sampled in the submit → run histogram. */
  uint64_t latency_count() const {
    return m_lat_count.load(std::memory_order_relaxed);
  }

  /**
   * Optional callback invoked from a worker thread each time it finishes
   * a job.  Used by the reactor integration to wake up the epoll loop.
   */
  void set_notify_reactor(std::function<void()> cb) {
    m_notify_reactor = std::move(cb);
  }

  void set_thread_hooks(ThreadHooks hooks) {
    m_thread_hooks = std::move(hooks);
  }

 private:
  struct Worker {
    std::thread thread;
    std::atomic<bool> stopped{false};
  };

  /** A queued job together with its enqueue timestamp (for the
   *  submit → run latency histogram). */
  struct QueuedJob {
    Job job;
    std::chrono::steady_clock::time_point enqueued_at;
  };

  void worker_loop(size_t idx);

  std::vector<std::unique_ptr<Worker>> m_workers;
  std::function<void()> m_notify_reactor;
  ThreadHooks m_thread_hooks;
  size_t m_cpu_offset{0};
  size_t m_num_vip{0};

  // Shared task queues (both guarded by m_mu; high-priority first).
  mutable std::mutex m_mu;
  std::condition_variable m_cv;     // normal workers
  std::condition_variable m_hp_cv;  // VIP workers (and HP-job cascade)
  std::deque<QueuedJob> m_queue;
  std::deque<QueuedJob> m_hp_queue;
  bool m_stopping{false};

  // Submit → run latency histogram (atomics; no lock on the hot path).
  std::atomic<uint64_t> m_lat_buckets[kNumLatencyBuckets]{};
  std::atomic<uint64_t> m_lat_total_us{0};
  std::atomic<uint64_t> m_lat_count{0};
};

}  // namespace coroutine