#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
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

}  // namespace
}  // namespace coroutine