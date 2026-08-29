/*
   Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is governed by separate terms
   and described in the file to which this notice is attached, or in the
   license file that accompanies it. The authors of MySQL hereby grant you an
   additional permission to link the program and your derivative works with
   the separately licensed software (including but not limited to OpenSSL)
   that is governed by separate terms and described in the file to which this
   notice is attached, or in the license file that accompanies it.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

/**
  @file sched_ext_bridge.cc

  Implementation of the userspace bridge between the MySQL server and the
  DB-aware sched_ext scheduler.

  The file has two halves with different compile-time visibility:

  * The THD-level lock-wait graph is ALWAYS compiled (pure userspace
    state, no libbpf).  In the coroutine architecture the OS thread that
    will execute a lock holder's next statement is not knowable at
    lock-wait time — explicit transactions hold locks *between* statements
    while the THD is parked on the reactor with real_id == 0.  So we track
    the wait graph keyed by THD pointers: s_waiter_to_holder and
    s_holder_wait_count.  These drive the userspace high-priority queue
    (is_lock_holder) and the execution-time tid publication below.

  * The BPF map publication half is compiled only when
    HAVE_SCX_SCHEDULER is defined (WITH_SCX_SCHEDULER=ON in CMake).  When
    the option is OFF, every BPF-touching function is a no-op (see the
    static inline fallbacks in the header) and mysqld has no libbpf
    dependency.

  tid-level publication (BPF half, HAVE_SCX_SCHEDULER only):
     thread_job_begin(thd), called by the executor pool around each THD
     job after store_globals(), checks whether @c thd is a lock holder
     and, if so, writes lock_holder[current_tid] = wait_count into the
     BPF map.  The kernel scheduler then protects *this* thread from
     preemption while it drains the lock's critical section.  The entry
     is removed by thread_job_end().

     Compatibility path: when lock_wait_begin() runs and the holder's
     real_id is currently valid (autocommit: the holder is executing on
     this very moment on another thread), we also publish the tid-keyed
     blocked_by / lock_holder entries immediately, as v1 did.
*/

#include "sql/sched_ext_bridge.h"

#include <mutex>
#include <unordered_map>

#include "sql/sql_class.h"  // THD

namespace sched_ext {
namespace {

// -----------------------------------------------------------------------
// THD-level lock-wait graph (always compiled).
//
// m_waiter_to_holder[waiter]  = holder   (set on begin, removed on end)
// m_holder_wait_count[holder] = N        (number of waiters on this holder)
//
// Guarded by s_wait_mu, a leaf mutex: no callback or latch is ever taken
// while holding it, and it is never taken while holding anything else.
// THD pointers are only used as opaque keys, never dereferenced — a stale
// entry is cleaned up by forget_thd() at connection teardown.
// -----------------------------------------------------------------------
static std::mutex s_wait_mu;
static std::unordered_map<THD *, THD *> s_waiter_to_holder;
static std::unordered_map<THD *, uint32_t> s_holder_wait_count;

/** Current wait count for @c holder (THD level), 0 if none. */
static uint32_t holder_wait_count(THD *holder) {
  std::lock_guard<std::mutex> lock(s_wait_mu);
  auto it = s_holder_wait_count.find(holder);
  return it == s_holder_wait_count.end() ? 0 : it->second;
}

#ifdef HAVE_SCX_SCHEDULER
/* Defined in the BPF half below. */
void scx_publish_wait_edge(THD *waiter, THD *holder);
void scx_clear_wait_edge(THD *waiter, THD *holder);
#endif

}  // anonymous namespace

// -----------------------------------------------------------------------
// Lock-wait graph API (always compiled; drives the userspace HP queue)
// -----------------------------------------------------------------------

bool is_lock_holder(THD *thd) {
  if (thd == nullptr) return false;
  return holder_wait_count(thd) > 0;
}

void lock_wait_begin(THD *waiter, THD *holder) {
  if (waiter == nullptr || holder == nullptr) return;

  /* THD-level wait graph: always recorded, SCX or not.  The lock is scoped:
     scx_publish_wait_edge() below takes s_wait_mu again internally, and
     std::mutex is not recursive — holding it across the call would
     self-deadlock. */
  {
    std::lock_guard<std::mutex> lock(s_wait_mu);
    s_waiter_to_holder[waiter] = holder;
    s_holder_wait_count[holder]++;
  }

#ifdef HAVE_SCX_SCHEDULER
  /*
   * Compatibility path (v1 behaviour): when the holder is executing right
   * now (autocommit workloads — its real_id is valid), publish the
   * tid-keyed blocked_by / lock_holder entries immediately.  For explicit
   * transactions (holder parked on the reactor, real_id == 0) the tid
   * publication is deferred to thread_job_begin() when the holder's next
   * job starts running.
   */
  scx_publish_wait_edge(waiter, holder);
#endif
}

void lock_wait_end(THD *waiter) {
  if (waiter == nullptr) return;

  /* THD-level: drop the waiter's edge and decrement the holder count. */
  THD *holder = nullptr;
  {
    std::lock_guard<std::mutex> lock(s_wait_mu);
    auto w = s_waiter_to_holder.find(waiter);
    if (w != s_waiter_to_holder.end()) {
      holder = w->second;
      auto c = s_holder_wait_count.find(holder);
      if (c != s_holder_wait_count.end()) {
        if (c->second > 1) {
          c->second--;
        } else {
          s_holder_wait_count.erase(c);
        }
      }
      s_waiter_to_holder.erase(w);
    }
  }

#ifdef HAVE_SCX_SCHEDULER
  /* Compatibility path cleanup for the tid-keyed BPF entries. */
  scx_clear_wait_edge(waiter, holder);
#else
  (void)holder;  // unused in the default build
#endif
}

void forget_thd(THD *thd) {
  if (thd == nullptr) return;

  std::lock_guard<std::mutex> lock(s_wait_mu);
  // Remove as waiter: decrement the holder's wait count.
  auto w = s_waiter_to_holder.find(thd);
  if (w != s_waiter_to_holder.end()) {
    THD *holder = w->second;
    auto c = s_holder_wait_count.find(holder);
    if (c != s_holder_wait_count.end()) {
      if (c->second > 1) {
        c->second--;
      } else {
        s_holder_wait_count.erase(c);
      }
    }
    s_waiter_to_holder.erase(w);
  }
  // Remove as holder: any waiters still pointing at us keep their entries;
  // they will fail to resolve at end/forget time and be cleaned then.  Drop
  // our own count entry.
  s_holder_wait_count.erase(thd);
}

}  // namespace sched_ext

// -----------------------------------------------------------------------
// BPF map publication half (only when HAVE_SCX_SCHEDULER).  The functions
// below still touch the always-compiled THD wait graph above (anonymous
// namespace symbols with internal linkage are visible throughout the
// translation unit).
// -----------------------------------------------------------------------
#ifdef HAVE_SCX_SCHEDULER

#include <bpf/bpf.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <cstring>

#include "my_thread.h"  // my_thread_self

namespace sched_ext {
namespace {

// -----------------------------------------------------------------------
// Map pin paths – must match the scx_db_sched loader's pin directory.
// -----------------------------------------------------------------------
static constexpr const char *kMapCommitVIP  = "/sys/fs/bpf/scx_db_sched/commit_vip";
static constexpr const char *kMapBlockedBy  = "/sys/fs/bpf/scx_db_sched/blocked_by";
static constexpr const char *kMapLockHolder = "/sys/fs/bpf/scx_db_sched/lock_holder";
static constexpr const char *kMapRgPrio     = "/sys/fs/bpf/scx_db_sched/rg_prio";

// -----------------------------------------------------------------------
// Bridge state
// -----------------------------------------------------------------------
static bool s_connected = false;
static int s_fd_commit_vip  = -1;
static int s_fd_blocked_by  = -1;
static int s_fd_lock_holder = -1;
static int s_fd_rg_prio     = -1;

// -----------------------------------------------------------------------
// pthread_t -> OS tid registry.
//
// The BPF maps are keyed by OS tid (the kernel's per-thread pid, i.e. the
// value of gettid()).  A THD's owning thread is stored as a pthread_t
// (THD::real_id), which is opaque.  Executor/worker threads call
// thread_start() from themselves, registering pthread_t -> gettid(), so the
// bridge can translate a THD into the numeric tid the scheduler needs.
// -----------------------------------------------------------------------
static std::mutex s_registry_mu;
static std::unordered_map<my_thread_t, int32_t> s_tid_registry;

/** Cached OS tid of the current thread (lazily initialised). */
thread_local int32_t s_current_tid = -1;

/**
  Per-thread publication state for the lock_holder BPF map.
  thread_job_begin() remembers whether (and for which tid) it published an
  entry, so thread_job_end() can remove exactly that entry even if the
  wait count changed (or dropped to zero) in between.
*/
struct ThreadPub {
  bool published = false;
  int32_t tid = -1;
};
thread_local ThreadPub s_thread_pub;

// -----------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------
static inline int32_t current_tid() {
  if (s_current_tid < 0) {
    s_current_tid = static_cast<int32_t>(syscall(SYS_gettid));
  }
  return s_current_tid;
}

static int open_map(const char *path) {
  int fd = bpf_obj_get(path);
  if (fd < 0) {
    /* Log once per path; repeated calls are expected during probe. */
    return -1;
  }
  return fd;
}

static void close_map(int fd) {
  if (fd >= 0) ::close(fd);
}

/** OS tid of the thread currently running @c thd, or -1 if unknown. */
static int32_t thd_os_tid(THD *thd) {
  if (thd == nullptr) return -1;
  std::lock_guard<std::mutex> lock(s_registry_mu);
  auto it = s_tid_registry.find(thd->real_id);
  return it == s_tid_registry.end() ? -1 : it->second;
}

}  // anonymous namespace

namespace {

/*
 * tid-level compatibility path (v1 behaviour): when the holder is
 * executing right now (autocommit workloads — its real_id is valid),
 * publish the tid-keyed blocked_by / lock_holder entries immediately.
 * For explicit transactions (holder parked on the reactor, real_id == 0)
 * the tid publication is deferred to thread_job_begin() when the
 * holder's next job starts running.
 */
void scx_publish_wait_edge(THD *waiter, THD *holder) {
  if (!s_connected) return;

  int32_t waiter_tid = current_tid();
  int32_t holder_tid = thd_os_tid(holder);
  if (holder_tid < 0) return;

  uint32_t count = 0;
  {
    std::lock_guard<std::mutex> lock(s_wait_mu);
    auto it = s_holder_wait_count.find(holder);
    if (it != s_holder_wait_count.end()) count = it->second;
  }
  if (count == 0) return;

  /* blocked_by[waiter] = holder */
  bpf_map_update_elem(s_fd_blocked_by, &waiter_tid, &holder_tid, BPF_ANY);

  /* lock_holder[holder] = wait count */
  bpf_map_update_elem(s_fd_lock_holder, &holder_tid, &count, BPF_ANY);
}

/*
 * Compatibility path cleanup: remove the tid-keyed blocked_by entry for
 * the waiter, and refresh/remove the lock_holder entry for its holder.
 */
void scx_clear_wait_edge(THD *waiter, THD *holder) {
  if (!s_connected) return;

  int32_t waiter_tid = thd_os_tid(waiter);
  if (waiter_tid < 0) waiter_tid = current_tid();
  int32_t holder_tid = 0;
  if (!bpf_map_lookup_elem(s_fd_blocked_by, &waiter_tid, &holder_tid)) {
    if (holder_tid == 0 && holder != nullptr) {
      holder_tid = thd_os_tid(holder);
      if (holder_tid < 0) return;
    }
    uint32_t count = 0;
    if (bpf_map_lookup_elem(s_fd_lock_holder, &holder_tid, &count) == 0) {
      if (count > 1) {
        count--;
        bpf_map_update_elem(s_fd_lock_holder, &holder_tid, &count, BPF_EXIST);
      } else {
        bpf_map_delete_elem(s_fd_lock_holder, &holder_tid);
      }
    }
    bpf_map_delete_elem(s_fd_blocked_by, &waiter_tid);
  }
}

}  // namespace

// -----------------------------------------------------------------------
// Public API (BPF half)
// -----------------------------------------------------------------------

bool connected() { return s_connected; }

void thread_start() {
  std::lock_guard<std::mutex> lock(s_registry_mu);
  s_tid_registry[my_thread_self()] = current_tid();
}

void thread_end() {
  std::lock_guard<std::mutex> lock(s_registry_mu);
  s_tid_registry.erase(my_thread_self());
}

void thread_job_begin(THD *thd) {
  if (!s_connected || thd == nullptr) return;

  uint32_t count = holder_wait_count(thd);
  if (count == 0) return;

  int32_t tid = current_tid();
  bpf_map_update_elem(s_fd_lock_holder, &tid, &count, BPF_ANY);
  s_thread_pub.published = true;
  s_thread_pub.tid = tid;
}

void thread_job_end(THD *thd) {
  if (!s_connected || thd == nullptr) return;
  if (!s_thread_pub.published) return;

  int32_t tid = s_thread_pub.tid;
  bpf_map_delete_elem(s_fd_lock_holder, &tid);
  s_thread_pub.published = false;
  s_thread_pub.tid = -1;
}

void commit_begin() {
  if (!s_connected) return;
  int32_t tid = current_tid();
  uint8_t val = 1;
  bpf_map_update_elem(s_fd_commit_vip, &tid, &val, BPF_ANY);
}

void commit_end() {
  if (!s_connected) return;
  int32_t tid = current_tid();
  bpf_map_delete_elem(s_fd_commit_vip, &tid);
}

void lock_holder_add(my_thread_id holder_tid) {
  if (!s_connected) return;
  int32_t holder = static_cast<int32_t>(holder_tid);
  uint32_t count = 0;
  bpf_map_lookup_elem(s_fd_lock_holder, &holder, &count);
  count++;
  bpf_map_update_elem(s_fd_lock_holder, &holder, &count, BPF_ANY);
}

void lock_holder_remove(my_thread_id holder_tid) {
  if (!s_connected) return;
  int32_t holder = static_cast<int32_t>(holder_tid);
  uint32_t count = 1;
  if (bpf_map_lookup_elem(s_fd_lock_holder, &holder, &count) == 0) {
    if (count > 1) {
      count--;
      bpf_map_update_elem(s_fd_lock_holder, &holder, &count, BPF_EXIST);
    } else {
      bpf_map_delete_elem(s_fd_lock_holder, &holder);
    }
  }
}

void set_priority(int nice) {
  if (!s_connected) return;
  int32_t tid = current_tid();
  int32_t val = nice;
  bpf_map_update_elem(s_fd_rg_prio, &tid, &val, BPF_ANY);
}

void init() {
  /* Already connected or probe already failed — do nothing. */
  if (s_connected) return;

  s_fd_commit_vip  = open_map(kMapCommitVIP);
  s_fd_blocked_by  = open_map(kMapBlockedBy);
  s_fd_lock_holder = open_map(kMapLockHolder);
  s_fd_rg_prio     = open_map(kMapRgPrio);

  if (s_fd_commit_vip >= 0 && s_fd_blocked_by >= 0 &&
      s_fd_lock_holder >= 0 && s_fd_rg_prio >= 0) {
    s_connected = true;
  } else {
    /* Partial failure — close whatever we opened. */
    close_map(s_fd_commit_vip);
    close_map(s_fd_blocked_by);
    close_map(s_fd_lock_holder);
    close_map(s_fd_rg_prio);
    s_fd_commit_vip  = -1;
    s_fd_blocked_by  = -1;
    s_fd_lock_holder = -1;
    s_fd_rg_prio     = -1;
    s_connected = false;
  }
}

}  // namespace sched_ext

#endif  // HAVE_SCX_SCHEDULER
