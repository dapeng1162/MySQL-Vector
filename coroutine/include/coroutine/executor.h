#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <deque>
#include <vector>
#include <atomic>

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
 */
class ExecutorPool {
 public:
  using Job = std::function<void()>;

  struct ThreadHooks {
    std::function<void()> on_start;  // called at the top of each worker loop
    std::function<void()> on_stop;   // called just before worker exits
  };

  /**
   * @param num_workers   Number of OS threads in the pool.
   * @param cpu_offset    First CPU index for pinning (worker i -> cpu i + offset).
   *                      Pass (size_t)-1 to disable CPU pinning.
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

  void worker_loop(size_t idx);

  std::vector<std::unique_ptr<Worker>> m_workers;
  std::function<void()> m_notify_reactor;
  ThreadHooks m_thread_hooks;
  size_t m_cpu_offset{0};
  size_t m_num_vip{0};

  // Shared task queues (both guarded by m_mu; high-priority first).
  mutable std::mutex m_mu;
  std::condition_variable m_cv;
  std::deque<Job> m_queue;
  std::deque<Job> m_hp_queue;
  bool m_stopping{false};
};

}  // namespace coroutine