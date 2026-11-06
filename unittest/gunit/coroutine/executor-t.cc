#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <sched.h>
#include <set>
#include <thread>
#include <vector>

#include "coroutine/executor.h"

namespace coroutine {
namespace {

TEST(ExecutorPoolTest, SubmitJobs) {
  constexpr size_t kNumWorkers = 2;
  ExecutorPool pool(kNumWorkers, static_cast<size_t>(-1));  // no CPU pinning

  std::atomic<int> counter{0};
  constexpr int kNumJobs = 100;

  pool.start();
  for (int i = 0; i < kNumJobs; ++i) {
    pool.submit([&counter] { counter.fetch_add(1, std::memory_order_relaxed); });
  }

  // Wait for all jobs to complete.
  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::seconds(5);
  while (counter.load(std::memory_order_relaxed) < kNumJobs &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }

  pool.stop();
  pool.join();

  EXPECT_EQ(counter.load(), kNumJobs);
}

// The pool uses a single shared FIFO queue + condition variable, so a single
// worker may legitimately drain the whole queue (no per-worker work stealing).
// What we guarantee: every submitted job runs exactly once, and the pool works
// under contention. To exercise multiple workers, tasks are given a tiny sleep
// so that a busy worker does not monopolize the queue.
TEST(ExecutorPoolTest, JobsAllCompleteUnderSharedQueue) {
  constexpr size_t kNumWorkers = 4;
  ExecutorPool pool(kNumWorkers, static_cast<size_t>(-1));

  std::atomic<int> worker_count{0};
  std::mutex mu;
  std::set<std::thread::id> tids;

  constexpr size_t kNumJobs = kNumWorkers * 10;

  pool.start();
  for (size_t i = 0; i < kNumJobs; ++i) {
    pool.submit([&mu, &tids, &worker_count] {
      std::this_thread::sleep_for(std::chrono::microseconds(200));
      {
        std::lock_guard<std::mutex> lock(mu);
        tids.insert(std::this_thread::get_id());
      }
      worker_count.fetch_add(1, std::memory_order_relaxed);
    });
  }

  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::seconds(5);
  while (worker_count.load(std::memory_order_relaxed) < static_cast<int>(kNumJobs) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }

  pool.stop();
  pool.join();

  EXPECT_EQ(worker_count.load(), static_cast<int>(kNumJobs));
  // Every job ran on some pool thread, and with enough work at least one
  // worker was used. We do not require all kNumWorkers to be touched, because
  // the shared queue does not guarantee distribution.
  EXPECT_GE(tids.size(), 1u);
  EXPECT_LE(tids.size(), kNumWorkers);
}

TEST(ExecutorPoolTest, ThreadHooksCalled) {
  ExecutorPool pool(1, static_cast<size_t>(-1));

  std::atomic<bool> start_called{false};
  std::atomic<bool> stop_called{false};

  ExecutorPool::ThreadHooks hooks;
  hooks.on_start = [&start_called] { start_called.store(true); };
  hooks.on_stop = [&stop_called] { stop_called.store(true); };
  pool.set_thread_hooks(std::move(hooks));

  pool.start();
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  pool.stop();
  pool.join();

  EXPECT_TRUE(start_called.load());
  EXPECT_TRUE(stop_called.load());
}

// ---------------------------------------------------------------------------
// High-priority queue tests
// ---------------------------------------------------------------------------

// HP jobs must be consumed before earlier-submitted normal jobs.
//
// Uses a SINGLE worker: with more workers the HP and normal jobs would run in
// parallel on different workers after release, and the recorded completion
// order would be a mutex-acquisition coin flip.  The guarantee under test is
// dequeue order, which is observable only when execution is serialized.
TEST(ExecutorPoolTest, HighPriorityRunsFirst) {
  constexpr size_t kNumWorkers = 1;
  // num_vip=0: all workers serve both queues (HP first).
  ExecutorPool pool(kNumWorkers, static_cast<size_t>(-1), 0);

  std::mutex mu;
  std::condition_variable cv;
  std::vector<int> order;
  std::atomic<int> done{0};
  // Block the worker until both test jobs are queued.
  std::atomic<bool> release{false};
  std::atomic<bool> blocker_entered{false};

  pool.start();

  // Occupy the worker with a blocking normal job.
  for (size_t i = 0; i < kNumWorkers; ++i) {
    pool.submit([&] {
      blocker_entered.store(true);
      std::unique_lock<std::mutex> lock(mu);
      cv.wait(lock, [&] { return release.load(); });
      order.push_back(0);
      done.fetch_add(1);
      cv.notify_all();
    });
  }

  // Wait until the worker is actually inside the blocking job.
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!blocker_entered.load() &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  ASSERT_TRUE(blocker_entered.load());

  // Queue: one normal job, then one HP job.  The HP job must run first
  // once the worker is released.
  pool.submit([&] {
    std::lock_guard<std::mutex> lock(mu);
    order.push_back(1);  // normal (queued later than HP)
    done.fetch_add(1);
    cv.notify_all();
  });
  pool.submit(
      [&] {
        std::lock_guard<std::mutex> lock(mu);
        order.push_back(2);  // high priority (queued after the normal one)
        done.fetch_add(1);
        cv.notify_all();
      },
      /*high_priority=*/true);

  // Let the blocker finish; the worker then drains HP before normal.
  {
    std::lock_guard<std::mutex> lock(mu);
    release.store(true);
  }
  cv.notify_all();

  deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (done.load() < 3 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }

  pool.stop();
  pool.join();

  ASSERT_EQ(order.size(), 3u);
  // The HP job (2) must precede the later-queued normal job (1);
  // the blocker (0) always runs first.
  auto pos_hp = std::find(order.begin(), order.end(), 2);
  auto pos_normal = std::find(order.begin(), order.end(), 1);
  ASSERT_NE(pos_hp, order.end());
  ASSERT_NE(pos_normal, order.end());
  EXPECT_EQ(order.front(), 0);
  EXPECT_LT(pos_hp, pos_normal);
}

// VIP worker test: with every normal worker blocked inside a job, an HP
// job must still execute (this is the thread-pool deadlock-storm guarantee
// for a lock holder's COMMIT).
TEST(ExecutorPoolTest, VipWorkerRescuesBlockedPool) {
  constexpr size_t kNumWorkers = 4;  // 1 VIP + 3 normal
  ExecutorPool pool(kNumWorkers, static_cast<size_t>(-1), 1);

  std::mutex mu;
  std::condition_variable cv;
  std::atomic<int> blocked{0};
  std::atomic<bool> release{false};
  std::atomic<bool> hp_done{false};

  pool.start();

  // Occupy ALL normal workers with blocking jobs.
  for (size_t i = 0; i < kNumWorkers - 1; ++i) {
    pool.submit([&] {
      blocked.fetch_add(1);
      std::unique_lock<std::mutex> lock(mu);
      cv.wait(lock, [&] { return release.load(); });
    });
  }

  // Wait until all normal workers are inside the blocking job.
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (blocked.load() < static_cast<int>(kNumWorkers - 1) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  ASSERT_EQ(blocked.load(), static_cast<int>(kNumWorkers - 1));

  // Submit an HP job: the VIP worker must pick it up even though every
  // normal worker is blocked.
  pool.submit(
      [&] {
        hp_done.store(true);
        {
          std::lock_guard<std::mutex> lock(mu);
          release.store(true);  // let the blocked jobs finish
        }
        cv.notify_all();
      },
      /*high_priority=*/true);

  deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!hp_done.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }

  pool.stop();
  pool.join();

  EXPECT_TRUE(hp_done.load());
}

// A VIP worker must not starve normal jobs when no HP jobs exist: the VIP
// worker only serves the HP queue, so normal jobs still complete via the
// remaining workers.
TEST(ExecutorPoolTest, NormalJobsCompleteWithVipWorker) {
  constexpr size_t kNumWorkers = 3;  // 1 VIP + 2 normal
  ExecutorPool pool(kNumWorkers, static_cast<size_t>(-1), 1);

  std::atomic<int> counter{0};
  constexpr int kNumJobs = 100;

  pool.start();
  for (int i = 0; i < kNumJobs; ++i) {
    pool.submit([&counter] { counter.fetch_add(1, std::memory_order_relaxed); });
  }

  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (counter.load(std::memory_order_relaxed) < kNumJobs &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }

  pool.stop();
  pool.join();

  EXPECT_EQ(counter.load(), kNumJobs);
}

// num_vip >= num_workers is clamped so normal jobs cannot be starved
// permanently.
TEST(ExecutorPoolTest, VipCountClamped) {
  ExecutorPool pool(2, static_cast<size_t>(-1), 5);  // clamped to 1
  std::atomic<int> counter{0};

  pool.start();
  pool.submit([&counter] { counter.fetch_add(1); });
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  pool.stop();
  pool.join();

  EXPECT_EQ(counter.load(), 1);
}

// ---------------------------------------------------------------------------
// Wakeup-protocol regression tests (lost-signal self-healing)
// ---------------------------------------------------------------------------

// A job submitted while every worker is busy inside a job cannot be
// delivered by a condvar signal (nobody is waiting, POSIX allows the
// signal to be lost).  It must still complete as soon as a worker
// finishes and re-checks the queue.  Regression guard for the permanent
// stall observed when submit() stopped notifying on a non-empty queue.
TEST(ExecutorPoolTest, JobQueuedWhileAllWorkersBusyRunsAfterRelease) {
  constexpr size_t kNumWorkers = 3;  // 1 VIP + 2 normal
  ExecutorPool pool(kNumWorkers, static_cast<size_t>(-1), 1);

  std::mutex mu;
  std::condition_variable cv;
  std::atomic<int> blocked{0};
  std::atomic<bool> release{false};
  std::atomic<bool> queued_job_done{false};

  pool.start();

  // Occupy both normal workers with blocking jobs (the VIP worker cannot
  // consume normal jobs, so it stays idle for them).
  for (size_t i = 0; i < kNumWorkers - 1; ++i) {
    pool.submit([&] {
      blocked.fetch_add(1);
      std::unique_lock<std::mutex> lock(mu);
      cv.wait(lock, [&] { return release.load(); });
    });
  }
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (blocked.load() < static_cast<int>(kNumWorkers - 1) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  ASSERT_EQ(blocked.load(), static_cast<int>(kNumWorkers - 1));

  // Both normal workers are inside their jobs, so this submit's signal
  // necessarily finds no waiter and is lost.  The job must still run
  // once the blockers finish and the workers re-check the queue.
  pool.submit([&] { queued_job_done.store(true); });

  {
    std::lock_guard<std::mutex> lock(mu);
    release.store(true);
  }
  cv.notify_all();

  deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!queued_job_done.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }

  pool.stop();
  pool.join();

  EXPECT_TRUE(queued_job_done.load());
}

// When the VIP worker is busy inside an HP job, a further HP job must be
// picked up by a normal worker (HP submits wake both condvars).
TEST(ExecutorPoolTest, HpJobRunsWhenVipWorkerBusy) {
  constexpr size_t kNumWorkers = 3;  // 1 VIP + 2 normal
  ExecutorPool pool(kNumWorkers, static_cast<size_t>(-1), 1);

  std::mutex mu;
  std::condition_variable cv;
  std::atomic<bool> vip_blocker_entered{false};
  std::atomic<bool> release{false};
  std::atomic<bool> second_hp_done{false};

  pool.start();

  // Occupy the VIP worker with a blocking HP job.
  pool.submit(
      [&] {
        vip_blocker_entered.store(true);
        std::unique_lock<std::mutex> lock(mu);
        cv.wait(lock, [&] { return release.load(); });
      },
      /*high_priority=*/true);

  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!vip_blocker_entered.load() &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  ASSERT_TRUE(vip_blocker_entered.load());

  // A second HP job while the VIP is busy: a normal worker must take it.
  pool.submit(
      [&] {
        second_hp_done.store(true);
        {
          std::lock_guard<std::mutex> lock(mu);
          release.store(true);
        }
        cv.notify_all();
      },
      /*high_priority=*/true);

  deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!second_hp_done.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }

  pool.stop();
  pool.join();

  EXPECT_TRUE(second_hp_done.load());
}

// Soak test for the wakeup protocol: concurrent submitters push a mix of
// normal and HP jobs while other jobs periodically block the workers.
// Every job must complete.  This gives probabilistic coverage of the
// lost-signal / stranded-job class of bugs that the deterministic tests
// above cannot inject.
TEST(ExecutorPoolTest, MixedSubmitSoakAllJobsComplete) {
  constexpr size_t kNumWorkers = 8;  // 1 VIP + 7 normal
  constexpr int kNumJobs = 2000;
  constexpr int kNumSubmitters = 4;
  ExecutorPool pool(kNumWorkers, static_cast<size_t>(-1), 1);

  std::atomic<int> completed{0};
  std::atomic<int> hp_completed{0};

  pool.start();

  std::vector<std::thread> submitters;
  for (int t = 0; t < kNumSubmitters; ++t) {
    submitters.emplace_back([&pool, &completed, &hp_completed, t] {
      for (int i = 0; i < kNumJobs / kNumSubmitters; ++i) {
        if ((i + t) % 50 == 0) {
          // Occasionally stall the taker so wakeups interleave with
          // workers transitioning between running and waiting.
          pool.submit([] { std::this_thread::sleep_for(std::chrono::milliseconds(2)); });
        }
        if ((i + t) % 7 == 0) {
          pool.submit(
              [&hp_completed, &completed] {
                hp_completed.fetch_add(1, std::memory_order_relaxed);
                completed.fetch_add(1, std::memory_order_relaxed);
              },
              /*high_priority=*/true);
        } else {
          pool.submit([&completed] {
            completed.fetch_add(1, std::memory_order_relaxed);
          });
        }
      }
    });
  }
  for (auto &th : submitters) th.join();

  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
  while (completed.load() < kNumJobs &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }

  pool.stop();
  pool.join();

  EXPECT_EQ(completed.load(), kNumJobs);
  EXPECT_GT(hp_completed.load(), 0);
}

// ---------------------------------------------------------------------------
// CPU pinning tests
// ---------------------------------------------------------------------------

namespace {

// Capture each worker's effective affinity from the on_start hook (which
// runs at the top of every worker loop, after pinning).
struct AffinityRecorder {
  std::mutex mu;
  std::vector<cpu_set_t> masks;

  void record() {
    cpu_set_t mask;
    CPU_ZERO(&mask);
    if (sched_getaffinity(0, sizeof(mask), &mask) == 0) {
      std::lock_guard<std::mutex> lock(mu);
      masks.push_back(mask);
    }
  }

  size_t count() {
    std::lock_guard<std::mutex> lock(mu);
    return masks.size();
  }
};

cpu_set_t process_allowed() {
  cpu_set_t allowed;
  CPU_ZERO(&allowed);
  if (sched_getaffinity(0, sizeof(allowed), &allowed) != 0) {
    CPU_SET(0, &allowed);  // fallback; assertion below still single-CPU
  }
  return allowed;
}

}  // namespace

// Every pinned worker must end up pinned to exactly one CPU that is part
// of the process's allowed set.  Regression test for the old
// cpu = cpu_offset + idx scheme, which silently failed (EINVAL) for
// workers with idx >= n_allowed on oversubscribed pools.
TEST(ExecutorPoolTest, PinnedWorkersStayInAllowedSet) {
  cpu_set_t allowed = process_allowed();
  int n_allowed = CPU_COUNT(&allowed);
  ASSERT_GE(n_allowed, 1);

  // Oversubscribe: more workers than allowed CPUs (like the 4× pool the
  // connection handler creates).  Round-robin placement must keep every
  // worker pinned instead of failing past n_allowed.
  constexpr size_t kNumWorkers = 32;
  ExecutorPool pool(kNumWorkers, 0);

  AffinityRecorder recorder;
  ExecutorPool::ThreadHooks hooks;
  hooks.on_start = [&recorder] { recorder.record(); };
  pool.set_thread_hooks(std::move(hooks));

  pool.start();
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (recorder.count() < kNumWorkers &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  pool.stop();
  pool.join();

  ASSERT_EQ(recorder.count(), kNumWorkers);
  for (const auto &mask : recorder.masks) {
    EXPECT_EQ(CPU_COUNT(&mask), 1) << "worker not pinned to a single CPU";
    for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
      if (CPU_ISSET(cpu, &mask)) {
        EXPECT_TRUE(CPU_ISSET(cpu, &allowed))
            << "worker pinned to CPU " << cpu << " outside allowed set";
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Submit → run latency histogram tests
// ---------------------------------------------------------------------------

TEST(ExecutorPoolTest, LatencyHistogramRecorded) {
  ExecutorPool pool(2, ExecutorPool::kNoPinning);
  std::atomic<int> counter{0};
  constexpr int kNumJobs = 50;

  pool.start();
  for (int i = 0; i < kNumJobs; ++i) {
    pool.submit([&counter] { counter.fetch_add(1, std::memory_order_relaxed); });
  }
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (counter.load(std::memory_order_relaxed) < kNumJobs &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  pool.stop();
  pool.join();

  EXPECT_EQ(counter.load(), kNumJobs);

  // Every completed job contributed exactly one histogram sample.
  EXPECT_EQ(pool.latency_count(), static_cast<uint64_t>(kNumJobs));
  uint64_t bucket_sum = 0;
  for (size_t i = 0; i < ExecutorPool::kNumLatencyBuckets; ++i) {
    bucket_sum += pool.latency_bucket_count(i);
  }
  EXPECT_EQ(bucket_sum, static_cast<uint64_t>(kNumJobs));
  // Total accumulated latency is consistent with the sample count.
  EXPECT_LE(pool.latency_total_us() / (kNumJobs ? kNumJobs : 1), 1000000u);
}

TEST(ExecutorPoolTest, LatencyHistogramEmptyPool) {
  ExecutorPool pool(1, ExecutorPool::kNoPinning);
  // No jobs submitted: counters stay at zero.
  EXPECT_EQ(pool.latency_count(), 0u);
  EXPECT_EQ(pool.latency_total_us(), 0u);
  for (size_t i = 0; i < ExecutorPool::kNumLatencyBuckets; ++i) {
    EXPECT_EQ(pool.latency_bucket_count(i), 0u);
  }
}

}  // namespace
}  // namespace coroutine