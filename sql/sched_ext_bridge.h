/*
   Copyright (c) 2026, Oracle and/or its affiliates.

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
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

/**
  @file sched_ext_bridge.h

  Userspace bridge between the MySQL server and the DB-aware sched_ext
  BPF scheduler (scx_db_sched, built under support-files/scx/).

  The server marks its threads' important events in a set of BPF maps that
  the scheduler reads:

    - commit_vip   : tid currently inside COMMIT / ROLLBACK
    - blocked_by   : waiter_tid -> holder_tid (lock wait relationship)
    - lock_holder  : tid -> number of threads waiting on it
    - rg_prio      : tid -> resource-group priority (nice-like)

  Graceful degradation is a hard requirement:

    * The THD-level lock-wait graph (lock_wait_begin/end, is_lock_holder,
      forget_thd) and the latch wait-edge mirror (latch_wait_begin/end,
      latch_wait_holder, the Coroutine_latch_* counters) are pure
      userspace state and are ALWAYS compiled, but are only ACTIVE while
      connected(): with no scheduler consuming the edges they are skipped
      entirely, so the no-scheduler path is zero-cost.

    * The BPF-facing half is a compile-time no-op when built without
      HAVE_SCX_SCHEDULER (the default): the server builds, links, starts and
      runs exactly as before, with zero overhead and no libbpf dependency.

    * When built with HAVE_SCX_SCHEDULER but no sched_ext scheduler is
      actually running (or the BPF maps are not pinned), init() fails to
      open the maps, connected() returns false, and every other function
      returns immediately without touching any BPF map.
*/

#ifndef SQL_SCHED_EXT_BRIDGE_INCLUDED
#define SQL_SCHED_EXT_BRIDGE_INCLUDED

#include "my_thread_local.h"  // my_thread_id
#include <thread>  // std::thread::id for latch wait edges

class THD;

/**
  DB-aware sched_ext bridge namespace.

  Unless HAVE_SCX_SCHEDULER is defined at compile time, the BPF-facing
  functions below are static inline no-ops, so including this header
  anywhere in the server is always safe and side-effect free.
*/
namespace sched_ext {

/**
  Whether the bridge is connected to live, pinned sched_ext maps.
  Probed once by init(); false until then, and always false when the
  scheduler is not running (or not built).
*/
bool connected();

/** Mark the current thread as being inside COMMIT / ROLLBACK. */
void commit_begin();
void commit_end();

/**
  Called when a server worker thread starts / stops.  The bridge uses this
  to build a pthread_t -> OS tid registry so that lock_wait_begin/end can
  translate a THD's owning thread into the numeric tid the BPF maps are
  keyed by.  thread_start() must be called from the new thread itself.
*/
void thread_start();
void thread_end();

/**
  Latch (InnoDB internal mutex / rw-lock) wait edges, published by the
  waiter around its OS wait (TTASEventMutex::wait() and the rw-lock
  sync-array waits).  Compiled always, active only while connected():
  every call then maintains the userspace waiter->holder mirror and
  the per-family wait/edge counters (SHOW GLOBAL STATUS
  Coroutine_latch_*); with no scheduler connected both the mirror and
  the counters are skipped and the path is zero-cost.

  owner is the latch holder's std::thread::id; pass the default
  (invalid) id when the holder cannot be identified — the wait is
  counted, but no edge is published.  Returns whether an edge to a
  resolved holder was recorded.  The holder id is inherently racy (the
  holder may release right after the caller read it): the bridge
  tolerates this because an edge lives only until the matching
  latch_wait_end().
*/
enum class Latch_kind { Mutex, RwLock };

bool latch_wait_begin(std::thread::id owner, Latch_kind kind);
void latch_wait_end();

/** Monotonic number of latch OS-waits instrumented, per family. */
uint64_t latch_wait_count(Latch_kind kind);

/** Monotonic number of waits that produced a wait edge (holder
resolved), per family. */
uint64_t latch_wait_edge_count(Latch_kind kind);

/**
  Holder recorded for @c waiter in the userspace latch-wait mirror, or
  the default (invalid) thread id if @c waiter is not currently blocked
  on a resolvable latch holder.  For tests and diagnostics.
*/
std::thread::id latch_wait_holder(std::thread::id waiter);

/**
  Called from the coroutine executor pool around each THD job
  (do_command / auth), on the executor thread running the job, after
  store_globals() / before restore_globals().

  In the coroutine architecture the OS thread executing a THD is not knowable
  at lock-wait time (the holder's job may not even be queued yet — explicit
  transactions hold locks *between* statements, while the THD is parked on
  the reactor).  So the bridge tracks the lock-wait graph at THD level, and
  only now — while the holder's job is actually running on a known OS thread
  — does it publish the tid-keyed lock_holder entry to the BPF scheduler.

  thread_job_begin(thd, lock_holder, time_sensitive): the reactor has already
  classified the job before queueing it.  Reuse that decision instead of
  locking the THD wait graph a second time on the worker.  If @c lock_holder
  is true, publish lock_holder[current_tid].  When @c time_sensitive is true
  (any high-priority queue job), additionally publish task_tier[current_tid] =
  TIER_TS for the duration of the job so the scheduler applies its bounded
  virtual-deadline bonus.
  thread_job_end(thd): remove whatever this thread published for @c thd.
*/
void thread_job_begin(THD *thd, bool lock_holder, bool time_sensitive);
void thread_job_end(THD *thd);

/**
  Whether any THD is currently recorded in the lock-wait graph as waiting
  on a lock held by another THD.  A single relaxed atomic load, maintained
  by lock_wait_begin/end/forget_thd: lets per-command hook sites skip the
  wait-graph mutex entirely in uncontended runs.
*/
bool any_lock_waits();

/**
  Per-command publication for the classic (one-thread-per-connection)
  architecture.  command_begin() is called from do_command_impl AFTER the
  command has been read from the socket and immediately before dispatch:

    - placement after the read is deliberate: a lock holder that sat idle
      between statements of an explicit transaction gets a fresh marker at
      wake-up, not one published before the (possibly long) idle read that
      the scheduler's staleness TTL would have expired by then;
    - the holder classification reuses is_lock_holder(), guarded by the
      any_lock_waits() fast path so the uncontended cost is one atomic
      load.

  No-op in the coroutine architecture (the executor pool already brackets
  each job with thread_job_begin/end, and double publication would
  unbalance the gate refcount) and in builds without HAVE_SCX_SCHEDULER.
*/
void command_begin(THD *thd);
void command_end(THD *thd);

/**
  Whether any other transaction is currently blocked on a lock held by
  @c thd.  Called by the reactor thread before submitting the THD's next
  job, to decide whether the job should go to the high-priority queue.
*/
bool is_lock_holder(THD *thd);

/**
  Defensive cleanup when a connection's THD is destroyed: removes any
  residual wait-graph entries (as waiter and as holder) so the wait counts
  cannot leak.  The THD pointer is only ever used as a map key.
*/
void forget_thd(THD *thd);

/**
  Record that @c waiter (a THD on this thread) is blocked on a lock held by
  @c holder.  Writes blocked_by[waiter_tid] = holder_tid and increments
  lock_holder[holder_tid].
*/
void lock_wait_begin(THD *waiter, THD *holder);

/** Clear the lock-wait record for @c waiter (blocked_by + lock_holder). */
void lock_wait_end(THD *waiter);

/** Adjust the wait-count of a lock holder (lock_holder map). */
void lock_holder_add(my_thread_id holder_tid);
void lock_holder_remove(my_thread_id holder_tid);

/**
  Set the current thread's scheduling tier in the task_tier BPF map.
  Tier values (must match scx_db_sched.bpf.c):
    0 = TIER_TS     time-sensitive: bounded virtual-deadline bonus
    1 = TIER_NORMAL default
    2 = TIER_BG     lower weight, subject to the same fairness accounting
  Called once from a thread's entry point (reactor, InnoDB background
  threads) or around a high-priority job's execution on an executor
  worker.  No-op when the bridge is not connected.
*/
void set_thread_tier(int tier);

/** Clear the current thread's tier entry (back to NORMAL). */
void clear_thread_tier();

/** Tier constants for set_thread_tier(). */
constexpr int kTierTS = 0;
constexpr int kTierNormal = 1;
constexpr int kTierBG = 2;

/** Set the current thread's resource-group priority (nice-like, lower =
 *  higher priority). */
void set_priority(int nice);

/** Probe and open the pinned BPF maps. Must be called once at startup. */
void init();

}  // namespace sched_ext

/*
 * -----------------------------------------------------------------------
 * No-op fallback (default build, no WITH_SCX_SCHEDULER).
 *
 * The THD-level lock-wait graph (is_lock_holder, forget_thd,
 * lock_wait_begin, lock_wait_end) is ALWAYS compiled in
 * sched_ext_bridge.cc: it is pure userspace state and drives the
 * coroutine handler's high-priority queue even when no sched_ext
 * scheduler is present.  The functions below are the kernel-facing half
 * (BPF map publication); when HAVE_SCX_SCHEDULER is not defined they are
 * compile-time no-ops, so the server has no libbpf dependency.
 * -----------------------------------------------------------------------
 */
#ifndef HAVE_SCX_SCHEDULER
namespace sched_ext {
inline bool connected() { return false; }
inline void commit_begin() {}
inline void commit_end() {}
inline void thread_start() {}
inline void thread_end() {}
inline void thread_job_begin(THD *, bool, bool) {}
inline void thread_job_end(THD *) {}
inline void lock_holder_add(my_thread_id) {}
inline void lock_holder_remove(my_thread_id) {}
inline void set_thread_tier(int) {}
inline void clear_thread_tier() {}
inline void set_priority(int) {}
inline void init() {}
inline void command_begin(THD *) {}
inline void command_end(THD *) {}
}  // namespace sched_ext
#endif

#endif /* SQL_SCHED_EXT_BRIDGE_INCLUDED */
