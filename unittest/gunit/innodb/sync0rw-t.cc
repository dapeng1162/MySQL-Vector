/* Copyright (c) 2021, 2026, Oracle and/or its affiliates.

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
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/* See http://code.google.com/p/googletest/wiki/Primer */

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <fstream>
#include <iostream>
#include <iterator>
#include <thread>

#include "os0thread.h"
#include "sql/sched_ext_bridge.h"
#include "sync0arr_impl.h"

namespace innodb_sync0rw_unittest {

struct phase_task {
  size_t thread_idx;
  std::function<void(void)> action;
  bool blocking{true};
};
/** It runs a machinery to execute the phases supplied. It creates
 threads that move through phases to perform tasks needed to complete the
 algorithm specified by these phases.*/
void execute_multithreaded_phase_plan(const std::vector<phase_task> phases[],
                                      const size_t phase_count) {
  std::vector<std::atomic<size_t>> completed_tasks_in_phase(phase_count);
  std::vector<std::thread> threads;
  std::atomic<size_t> current_phase{0};
  size_t max_thread_idx_used = 0;

  /* Scan phase list to search for max thread index used to create that many
   threads. */
  for (size_t p = 0; p < phase_count; ++p) {
    completed_tasks_in_phase[p] = 0;
    for (const auto &t : phases[p]) {
      max_thread_idx_used = std::max(max_thread_idx_used, t.thread_idx);
    }
  }

  /* Specifies actions taken by a single thread to be created, depending on the
   thread index. It waits for each phase to be executed in chronological
   order, and for each phase it executes all tasks that are assigned for the
   thread index, as specified by the phases algorithm supplied. */
  auto thread_action = [&](auto my_thread_idx) {
    /* Run phases one by one in chronological order. */
    for (size_t p = 0; p < phase_count; ++p) {
      /* Wait for a specified phase to start. */
      while (current_phase < p) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
      /* Find all tasks in this phase that are assigned for current thread
       index. */
      for (const auto &t : phases[p]) {
        if (t.thread_idx == my_thread_idx) {
          /* If the action is blocking, execute it first and only then
           allow to move the phases forward. */
          if (t.blocking) t.action();
          /* Move phases forward - set this task as executed. */
          if (++completed_tasks_in_phase[p] == phases[p].size()) {
            /* If this task was last in the phase to execute, then move to
             the next phase. */
            current_phase++;
          }
          /* If the task was meant to not be blocking, execute it after
           moving phases forward. Note, that tasks in next phase may
           want to wait a bit for this thread to do its work. As this is
           blocking, there is no way to synchronize and tell other when
           to move on. */
          if (!t.blocking) t.action();
        }
      }
    }
  };
  /* Creates threads that execute the algorithm. */
  for (size_t thread_idx = 0; thread_idx <= max_thread_idx_used; ++thread_idx) {
    threads.emplace_back(thread_action, thread_idx);
  }
  /* Cleanup */
  for (auto &t : threads) {
    t.join();
  }
  EXPECT_EQ(current_phase.load(), phase_count);
}

TEST(sync0rw, rw_lock_reader_thread) {
  /* This test tests if the reader_thread is calculated correctly.
  We have three rw_locks:
  - lock 0 is S-latched by 3 threads in this order: t2, t1, t3. Then t2
  and t3 unlocks it, so only t1 have it locked.
  - lock 1 is S-latched by t1 and t2.
  - lock 2 is only X-latched by t1.
  Now we try to  X-latch locks 0, 1 and 2 with threads t2, t3 and t4.
  They will be waiting on rw_locks with different number of readers: 1, 2 and 0,
  respectively.
  We print cell info and let t1 to continue, which in turn lets all threads to
  finish. */

  os_event_global_init();
  sync_check_init(4);

  rw_lock_t *rw_locks[3];
  for (auto &rw_lock : rw_locks) {
    rw_lock = static_cast<rw_lock_t *>(malloc(sizeof(rw_lock_t)));
    rw_lock_create(PSI_NOT_INSTRUMENTED, rw_lock, LATCH_ID_BUF_BLOCK_LOCK);
  }

  std::atomic<std::thread::id> thread_1_id;

  auto check_reader_counts_action = [&] {
    /* Let all threads place their X-lock waits. */
    std::this_thread::sleep_for(std::chrono::seconds(2));

    EXPECT_EQ(rw_lock_get_reader_count(rw_locks[0]), 1);
    EXPECT_EQ(rw_lock_get_reader_count(rw_locks[1]), 2);
    EXPECT_EQ(rw_lock_get_reader_count(rw_locks[2]), 0);
    EXPECT_EQ(rw_locks[0]->reader_thread.recover_if_single(),
              thread_1_id.load());

    FILE *tmp_file = tmpfile();
    EXPECT_NE(tmp_file, nullptr);

    for (size_t i = 0; i < sync_array_size; ++i) {
      sync_array_t *arr = sync_wait_array[i];

      mutex_enter(&arr->mutex);

      for (size_t j = 0; j < arr->next_free_slot; j++) {
        sync_cell_t const &cell = arr->cells[j];

        void *latch = cell.latch.mutex;

        if (latch == nullptr || !cell.waiting) {
          continue;
        }

        sync_array_cell_print(tmp_file, &cell);
      }

      mutex_exit(&arr->mutex);
    }

    /* Print cells to temporary file, read all content and check it is correct.
     */
    const auto len = ftell(tmp_file);
    EXPECT_NE(-1, len);
    auto content = std::make_unique<char[]>(len);
    EXPECT_EQ(0, fseek(tmp_file, 0, SEEK_SET));
    EXPECT_EQ(len, fread(content.get(), 1, len, tmp_file));
    EXPECT_EQ(0, fclose(tmp_file));
    std::string const cell_print(content.get(), len);

    EXPECT_THAT(cell_print, testing::HasSubstr("number of readers 0, waiters"));
    /* Note that std::to_string(thread_id_to_uint64(thread_id) could be
     something different than to_string(thread_id). */
    EXPECT_THAT(cell_print, testing::HasSubstr(
                                "number of readers 1 (thread id " +
                                to_string(thread_1_id.load()) + "), waiters"));
    EXPECT_THAT(cell_print, testing::HasSubstr("number of readers 2, waiters"));
  };

  /* We proceed the test in the following phases: */
  std::vector<phase_task> phases[] = {
      /* Place all required S-latches and X-latches for locks 1 and 2.
      Place first S-latch for lock 0 (in thread 2) - other threads will
      place their lock 1 latches in next phases to synchronize order of
      S-latching.
    */
      {
          phase_task{1, [&] { thread_1_id = std::this_thread::get_id(); }},
          phase_task{1, [&] { rw_lock_s_lock(rw_locks[1], UT_LOCATION_HERE); }},
          phase_task{1, [&] { rw_lock_x_lock(rw_locks[2], UT_LOCATION_HERE); }},
          phase_task{2, [&] { rw_lock_s_lock(rw_locks[0], UT_LOCATION_HERE); }},
          phase_task{2, [&] { rw_lock_s_lock(rw_locks[1], UT_LOCATION_HERE); }},
      },
      /* Place second S-latch on lock 0, now from thread 1. */
      {
          phase_task{1, [&] { rw_lock_s_lock(rw_locks[0], UT_LOCATION_HERE); }},
      },
      /* Place third S-latch on lock 0, now from thread 3. */
      {
          phase_task{3, [&] { rw_lock_s_lock(rw_locks[0], UT_LOCATION_HERE); }},
      },
      /* Unlatch S-latches on lock 0 from threads 2 and 3. */
      {
          phase_task{2, [&] { rw_lock_s_unlock(rw_locks[0]); }},
          phase_task{3, [&] { rw_lock_s_unlock(rw_locks[0]); }},
      },
      /* Now place all blocking X-latches from threads 2, 3 and 4. */
      {
          phase_task{2, [&] { rw_lock_x_lock(rw_locks[0], UT_LOCATION_HERE); },
                     false},
          phase_task{3, [&] { rw_lock_x_lock(rw_locks[1], UT_LOCATION_HERE); },
                     false},
          phase_task{4, [&] { rw_lock_x_lock(rw_locks[2], UT_LOCATION_HERE); },
                     false},
      },
      /* Now run the rw_locks check. */
      {
          phase_task{0, check_reader_counts_action},
      },
      /* Unlock all remaining latches. */
      {
          phase_task{1, [&] { rw_lock_s_unlock(rw_locks[0]); }},
          phase_task{1, [&] { rw_lock_s_unlock(rw_locks[1]); }},

          /* Unlocking thread 1, it will allow next threads to be able to
          get their X-latches and then release them all. */
          phase_task{1, [&] { rw_lock_x_unlock(rw_locks[2]); }},
          phase_task{2, [&] { rw_lock_s_unlock(rw_locks[1]); }},
          phase_task{2, [&] { rw_lock_x_unlock(rw_locks[0]); }},
          phase_task{3, [&] { rw_lock_x_unlock(rw_locks[1]); }},
          phase_task{4, [&] { rw_lock_x_unlock(rw_locks[2]); }},
      }};

  execute_multithreaded_phase_plan(phases, UT_ARR_SIZE(phases));

  for (auto &rw_lock : rw_locks) {
    rw_lock_free(rw_lock);
    free(rw_lock);
  }

  sync_check_close();
  os_event_global_destroy();
}

/* Tests the DB-aware scheduler latch wait-edge instrumentation of the
rw-lock sync-array wait paths (sync0rw.cc).  Five locks, each driving a
different wait shape:

- lock A: X-locked by t1; t2's X-lock waits  -> edge t2 -> t1 (writer)
- lock B: single S-reader t3; t4's X-lock is the wait_ex thread waiting
  for readers to drain                                -> edge t4 -> t3
- lock C: two S-readers t5, t6; t7's X-lock waits for both -> no edge
  (multiple readers: no single holder to resolve)
- lock D: X-locked by t8; t9's S-lock waits             -> edge t9 -> t8
- lock E: X-locked by t10; t11's SX-lock waits         -> edge t11 -> t10

While the waiters are parked on their events, the userspace mirror in
the sched_ext bridge must show exactly these edges (it is always
compiled, no sched_ext scheduler required), the per-family counters
must have advanced, and after the holders release, the mirror must be
empty again (wait ends are paired with waits). */
TEST(sync0rw, rw_lock_latch_wait_edges) {
  os_event_global_init();
  /* Enough sync-array slots for the five concurrent waiters below (the
  existing test above uses 4 slots for three waiters). */
  sync_check_init(16);

  rw_lock_t *rw_locks[5];
  for (auto &rw_lock : rw_locks) {
    rw_lock = static_cast<rw_lock_t *>(malloc(sizeof(rw_lock_t)));
    rw_lock_create(PSI_NOT_INSTRUMENTED, rw_lock, LATCH_ID_BUF_BLOCK_LOCK);
  }
  rw_lock_t *const lock_a = rw_locks[0];
  rw_lock_t *const lock_b = rw_locks[1];
  rw_lock_t *const lock_c = rw_locks[2];
  rw_lock_t *const lock_d = rw_locks[3];
  rw_lock_t *const lock_e = rw_locks[4];

  std::atomic<std::thread::id> thread_id[12];
  for (auto &id : thread_id) {
    id = std::thread::id{};
  }

  uint64_t waits_before = 0;
  uint64_t edges_before = 0;

  auto record_id = [&](size_t i) {
    thread_id[i] = std::this_thread::get_id();
  };

  auto check_edges_action = [&] {
    /* Let all waiters park on their events. */
    std::this_thread::sleep_for(std::chrono::seconds(2));

    EXPECT_EQ(sched_ext::latch_wait_holder(thread_id[2].load()),
              thread_id[1].load());
    EXPECT_EQ(sched_ext::latch_wait_holder(thread_id[4].load()),
              thread_id[3].load());
    EXPECT_EQ(sched_ext::latch_wait_holder(thread_id[7].load()),
              std::thread::id{});
    EXPECT_EQ(sched_ext::latch_wait_holder(thread_id[9].load()),
              thread_id[8].load());
    EXPECT_EQ(sched_ext::latch_wait_holder(thread_id[11].load()),
              thread_id[10].load());

    /* Every blocked request must have reached an OS wait; all but the
    two-reader one must have produced an edge. */
    EXPECT_GE(sched_ext::latch_wait_count(sched_ext::Latch_kind::RwLock) -
                  waits_before,
              5);
    EXPECT_GE(sched_ext::latch_wait_edge_count(sched_ext::Latch_kind::RwLock) -
                  edges_before,
              4);
  };

  auto check_mirror_empty_action = [&] {
    /* Waiters are woken and return through latch_wait_end(). */
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    for (size_t i : {2, 4, 7, 9, 11}) {
      EXPECT_EQ(sched_ext::latch_wait_holder(thread_id[i].load()),
                std::thread::id{});
    }
  };

  std::vector<phase_task> phases[] = {
      /* Record thread ids; holders take their latches; baseline counters. */
      {
          phase_task{1, [&] { record_id(1); }},
          phase_task{1, [&] { rw_lock_x_lock(lock_a, UT_LOCATION_HERE); }},
          phase_task{2, [&] { record_id(2); }},
          phase_task{3, [&] { record_id(3); }},
          phase_task{3, [&] { rw_lock_s_lock(lock_b, UT_LOCATION_HERE); }},
          phase_task{4, [&] { record_id(4); }},
          phase_task{5, [&] { record_id(5); }},
          phase_task{5, [&] { rw_lock_s_lock(lock_c, UT_LOCATION_HERE); }},
          phase_task{6, [&] { record_id(6); }},
          phase_task{6, [&] { rw_lock_s_lock(lock_c, UT_LOCATION_HERE); }},
          phase_task{7, [&] { record_id(7); }},
          phase_task{8, [&] { record_id(8); }},
          phase_task{8, [&] { rw_lock_x_lock(lock_d, UT_LOCATION_HERE); }},
          phase_task{9, [&] { record_id(9); }},
          phase_task{10, [&] { record_id(10); }},
          phase_task{10, [&] { rw_lock_x_lock(lock_e, UT_LOCATION_HERE); }},
          phase_task{11, [&] { record_id(11); }},
          phase_task{
              0,
              [&] {
                waits_before = sched_ext::latch_wait_count(
                    sched_ext::Latch_kind::RwLock);
                edges_before = sched_ext::latch_wait_edge_count(
                    sched_ext::Latch_kind::RwLock);
              }},
      },
      /* All waiters block (non-blocking tasks: phases advance while
      the lock calls are still parked in their sync-array waits). */
      {
          phase_task{2, [&] { rw_lock_x_lock(lock_a, UT_LOCATION_HERE); },
                     false},
          phase_task{4, [&] { rw_lock_x_lock(lock_b, UT_LOCATION_HERE); },
                     false},
          phase_task{7, [&] { rw_lock_x_lock(lock_c, UT_LOCATION_HERE); },
                     false},
          phase_task{9, [&] { rw_lock_s_lock(lock_d, UT_LOCATION_HERE); },
                     false},
          phase_task{11, [&] { rw_lock_sx_lock(lock_e, UT_LOCATION_HERE); },
                     false},
      },
      /* Check the mirror while every waiter is parked. */
      {
          phase_task{0, check_edges_action},
      },
      /* Release the holders; waiters acquire and release in turn. */
      {
          phase_task{1, [&] { rw_lock_x_unlock(lock_a); }},
          phase_task{2, [&] { rw_lock_x_unlock(lock_a); }},
          phase_task{3, [&] { rw_lock_s_unlock(lock_b); }},
          phase_task{4, [&] { rw_lock_x_unlock(lock_b); }},
          phase_task{5, [&] { rw_lock_s_unlock(lock_c); }},
          phase_task{6, [&] { rw_lock_s_unlock(lock_c); }},
          phase_task{7, [&] { rw_lock_x_unlock(lock_c); }},
          phase_task{8, [&] { rw_lock_x_unlock(lock_d); }},
          phase_task{9, [&] { rw_lock_s_unlock(lock_d); }},
          phase_task{10, [&] { rw_lock_x_unlock(lock_e); }},
          phase_task{11, [&] { rw_lock_sx_unlock(lock_e); }},
      },
      /* The mirror must be empty again. */
      {
          phase_task{0, check_mirror_empty_action},
      }};

  execute_multithreaded_phase_plan(phases, UT_ARR_SIZE(phases));

  for (auto &rw_lock : rw_locks) {
    rw_lock_free(rw_lock);
    free(rw_lock);
  }

  sync_check_close();
  os_event_global_destroy();
}

}  // namespace innodb_sync0rw_unittest
