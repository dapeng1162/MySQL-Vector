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

  * The THD-level lock-wait graph and the latch wait-edge mirror are
    ALWAYS compiled (pure userspace state, no libbpf).  In the coroutine
    architecture the OS thread that will execute a lock holder's next
    statement is not knowable at lock-wait time — explicit transactions
    hold locks *between* statements while the THD is parked on the
    reactor with real_id == 0.  So we track the wait graph keyed by THD
    pointers: s_waiter_to_holder and s_holder_wait_count.  These drive
    the userspace high-priority queue (is_lock_holder) and the
    execution-time tid publication below.  The latch mirror
    (s_latch_waiter_to_holder, keyed by std::thread::id) plus the
    Coroutine_latch_* counters serve the same purpose for InnoDB
    internal latches (mutex and rw-lock OS waits).

  * The BPF map publication half is compiled only when
    HAVE_SCX_SCHEDULER is defined (WITH_SCX_SCHEDULER=ON in CMake).  When
    the option is OFF, every BPF-touching function is a no-op (see the
    static inline fallbacks in the header) and mysqld has no libbpf
    dependency.

  tid-level publication (BPF half, HAVE_SCX_SCHEDULER only):
     thread_job_begin(thd, lock_holder, time_sensitive), called by the
     executor pool around each THD job after store_globals(), reuses the
     reactor's lock-holder classification and writes lock_holder[current_tid]
     when set.  The kernel scheduler then protects *this* thread from
     preemption while it drains the lock's critical section.  The entry is
     removed by thread_job_end().

     Compatibility path: when lock_wait_begin() runs and the holder's
     real_id is currently valid (autocommit: the holder is executing on
     this very moment on another thread), we also publish the tid-keyed
     blocked_by / lock_holder entries immediately, as v1 did.
*/

#include "sql/sched_ext_bridge.h"

#include <atomic>
#include <functional>
#include <mutex>
#include <unordered_map>

#include "mysql_sched_tier.h"  // mysql_sched_set_thread_tier (C hook)
#include "sql/sql_class.h"     // THD

namespace sched_ext {
namespace {

// -----------------------------------------------------------------------
// Sharded maps.
//
// v1 used one global mutex per structure (wait graph, gate refcount, tid
// registries, latch mirror).  In the classic architecture those locks are
// taken once per COMMAND (is_lock_holder read) and twice per COMMIT (gate
// refcount churn), from every connection thread: at 256 connections the
// single cacheline degenerated into futex traffic, measured as -8% TPS
// with +40% cpu-migrations at rw256.  Each logical map below is split
// into 256 independently-locked shards.  Shard locks are NEVER nested
// across maps (each helper locks one shard, releases, then takes the
// next), so there is no ordering hazard by construction.
// -----------------------------------------------------------------------
template <typename K, typename V>
struct Sharded_map {
  static constexpr size_t kCount = 256;
  static_assert((kCount & (kCount - 1)) == 0, "power of two");
  std::mutex mu[kCount];
  std::unordered_map<K, V> m[kCount];
  size_t idx(K k) const { return std::hash<K>()(k) & (kCount - 1); }
};

// -----------------------------------------------------------------------
// THD-level lock-wait graph (always compiled).
//
// s_waiter_to_holder[waiter]  = holder   (set on begin, removed on end)
// s_holder_wait_count[holder] = N        (number of waiters on this holder)
//
// THD pointers are only used as opaque keys, never dereferenced — a stale
// entry is cleaned up by forget_thd() at connection teardown.
// -----------------------------------------------------------------------
static Sharded_map<THD *, THD *> s_waiter_to_holder;
static Sharded_map<THD *, uint32_t> s_holder_wait_count;

/** Number of live waiter->holder edges (relaxed; advisory fast-path
 * counter for per-command hook sites, never used for correctness). */
static std::atomic<uint32_t> s_thd_wait_edges{0};

/** Current wait count for @c holder (THD level), 0 if none. */
static uint32_t holder_wait_count(THD *holder) {
  const size_t i = s_holder_wait_count.idx(holder);
  std::lock_guard<std::mutex> lock(s_holder_wait_count.mu[i]);
  auto it = s_holder_wait_count.m[i].find(holder);
  return it == s_holder_wait_count.m[i].end() ? 0 : it->second;
}

#ifdef HAVE_SCX_SCHEDULER
/* Defined in the BPF half below. */
void scx_publish_wait_edge(THD *waiter, THD *holder);
void scx_clear_wait_edge(THD *waiter, THD *holder);
void scx_publish_latch_edge(std::thread::id owner);
void scx_clear_latch_edge();
#endif

// -----------------------------------------------------------------------
// Latch-level wait mirror (always compiled).
//
// s_latch_waiter_to_holder[waiter] = holder   (set on begin, removed on
// end).  Keyed by std::thread::id — the raw id the instrumentation sites
// read from the latch — so no tid registry is needed for this userspace
// half.
// -----------------------------------------------------------------------
static Sharded_map<std::thread::id, std::thread::id>
    s_latch_waiter_to_holder;

static std::atomic<uint64_t> s_latch_waits[2] = {};
static std::atomic<uint64_t> s_latch_edges[2] = {};

}  // anonymous namespace

// -----------------------------------------------------------------------
// Lock-wait graph API (always compiled; drives the userspace HP queue)
// -----------------------------------------------------------------------

bool is_lock_holder(THD *thd) {
  if (thd == nullptr) return false;
  return holder_wait_count(thd) > 0;
}

bool any_lock_waits() {
  return s_thd_wait_edges.load(std::memory_order_relaxed) != 0;
}

void lock_wait_begin(THD *waiter, THD *holder) {
  if (waiter == nullptr || holder == nullptr) return;

  /* THD-level wait graph: always recorded, SCX or not.  Shard locks are
     scoped per map and never nested: scx_publish_wait_edge() below takes
     the holder-count shard again internally. */
  {
    const size_t i = s_waiter_to_holder.idx(waiter);
    std::lock_guard<std::mutex> lock(s_waiter_to_holder.mu[i]);
    s_waiter_to_holder.m[i][waiter] = holder;
  }
  {
    const size_t i = s_holder_wait_count.idx(holder);
    std::lock_guard<std::mutex> lock(s_holder_wait_count.mu[i]);
    s_holder_wait_count.m[i][holder]++;
  }
  s_thd_wait_edges.fetch_add(1, std::memory_order_relaxed);

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
    const size_t i = s_waiter_to_holder.idx(waiter);
    std::lock_guard<std::mutex> lock(s_waiter_to_holder.mu[i]);
    auto w = s_waiter_to_holder.m[i].find(waiter);
    if (w != s_waiter_to_holder.m[i].end()) {
      holder = w->second;
      s_waiter_to_holder.m[i].erase(w);
      s_thd_wait_edges.fetch_sub(1, std::memory_order_relaxed);
    }
  }
  if (holder != nullptr) {
    const size_t i = s_holder_wait_count.idx(holder);
    std::lock_guard<std::mutex> lock(s_holder_wait_count.mu[i]);
    auto c = s_holder_wait_count.m[i].find(holder);
    if (c != s_holder_wait_count.m[i].end()) {
      if (c->second > 1) {
        c->second--;
      } else {
        s_holder_wait_count.m[i].erase(c);
      }
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

  /* Remove as waiter: decrement the holder's wait count. */
  {
    THD *holder = nullptr;
    {
      const size_t i = s_waiter_to_holder.idx(thd);
      std::lock_guard<std::mutex> lock(s_waiter_to_holder.mu[i]);
      auto w = s_waiter_to_holder.m[i].find(thd);
      if (w != s_waiter_to_holder.m[i].end()) {
        holder = w->second;
        s_waiter_to_holder.m[i].erase(w);
        s_thd_wait_edges.fetch_sub(1, std::memory_order_relaxed);
      }
    }
    if (holder != nullptr) {
      const size_t i = s_holder_wait_count.idx(holder);
      std::lock_guard<std::mutex> lock(s_holder_wait_count.mu[i]);
      auto c = s_holder_wait_count.m[i].find(holder);
      if (c != s_holder_wait_count.m[i].end()) {
        if (c->second > 1) {
          c->second--;
        } else {
          s_holder_wait_count.m[i].erase(c);
        }
      }
    }
  }
  /* Remove as holder: any waiters still pointing at us keep their entries;
     they will fail to resolve at end/forget time and be cleaned then.  Drop
     our own count entry. */
  {
    const size_t i = s_holder_wait_count.idx(thd);
    std::lock_guard<std::mutex> lock(s_holder_wait_count.mu[i]);
    s_holder_wait_count.m[i].erase(thd);
  }
}

// -----------------------------------------------------------------------
// Latch wait-edge API (always compiled).
// -----------------------------------------------------------------------

bool latch_wait_begin(std::thread::id owner, Latch_kind kind) {
  const int k = static_cast<int>(kind);
  s_latch_waits[k].fetch_add(1, std::memory_order_relaxed);
  if (owner == std::thread::id{}) return false;

  {
    const std::thread::id waiter = std::this_thread::get_id();
    const size_t i = s_latch_waiter_to_holder.idx(waiter);
    std::lock_guard<std::mutex> lock(s_latch_waiter_to_holder.mu[i]);
    s_latch_waiter_to_holder.m[i][waiter] = owner;
  }
  s_latch_edges[k].fetch_add(1, std::memory_order_relaxed);

#ifdef HAVE_SCX_SCHEDULER
  /* Publish the tid-keyed BPF maps (no-op unless connected).  Scoped:
     scx_publish_latch_edge() must not take the latch-wait shard. */
  scx_publish_latch_edge(owner);
#endif
  return true;
}

void latch_wait_end() {
  {
    const std::thread::id waiter = std::this_thread::get_id();
    const size_t i = s_latch_waiter_to_holder.idx(waiter);
    std::lock_guard<std::mutex> lock(s_latch_waiter_to_holder.mu[i]);
    s_latch_waiter_to_holder.m[i].erase(waiter);
  }

#ifdef HAVE_SCX_SCHEDULER
  scx_clear_latch_edge();
#endif
}

uint64_t latch_wait_count(Latch_kind kind) {
  return s_latch_waits[static_cast<int>(kind)].load(std::memory_order_relaxed);
}

uint64_t latch_wait_edge_count(Latch_kind kind) {
  return s_latch_edges[static_cast<int>(kind)].load(std::memory_order_relaxed);
}

std::thread::id latch_wait_holder(std::thread::id waiter) {
  const size_t i = s_latch_waiter_to_holder.idx(waiter);
  std::lock_guard<std::mutex> lock(s_latch_waiter_to_holder.mu[i]);
  auto it = s_latch_waiter_to_holder.m[i].find(waiter);
  return it == s_latch_waiter_to_holder.m[i].end() ? std::thread::id{}
                                                   : it->second;
}

}  // namespace sched_ext

/* C hook for storage engines (InnoDB background threads) that cannot
 * include sql/ headers.  set_thread_tier() is a compile-time no-op in
 * default builds and a runtime no-op without a connected scheduler. */
extern "C" void mysql_sched_set_thread_tier(int tier) {
  sched_ext::set_thread_tier(tier);
}

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
#include <chrono>
#include <cstring>
#include <fstream>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "my_thread.h"  // my_thread_self
#include "sql/conn_handler/connection_handler_manager.h"  // thread_handling
#include "sql/resourcegroups/resource_group.h"  // Resource_group
#include "sql/resourcegroups/thread_resource_control.h"

namespace sched_ext {
namespace {

// -----------------------------------------------------------------------
// Map pin paths – must match the scx_db_sched loader's pin directory.
// -----------------------------------------------------------------------
static constexpr const char *kMapCommitVIP  = "/sys/fs/bpf/scx_db_sched/commit_vip";
static constexpr const char *kMapBlockedBy  = "/sys/fs/bpf/scx_db_sched/blocked_by";
static constexpr const char *kMapLockHolder = "/sys/fs/bpf/scx_db_sched/lock_holder";
static constexpr const char *kMapRgPrio     = "/sys/fs/bpf/scx_db_sched/rg_prio";
static constexpr const char *kMapTaskTier   = "/sys/fs/bpf/scx_db_sched/task_tier";
static constexpr const char *kMapSemAny     = "/sys/fs/bpf/scx_db_sched/sem_any";

// -----------------------------------------------------------------------
// Bridge state
// -----------------------------------------------------------------------
static bool s_connected = false;
static int s_fd_commit_vip  = -1;
static int s_fd_blocked_by  = -1;
static int s_fd_lock_holder = -1;
static int s_fd_rg_prio     = -1;
static int s_fd_task_tier   = -1;
static int s_fd_sem_any     = -1;

// -----------------------------------------------------------------------
// sem_any gate refcount.
//
// The BPF scheduler's enqueue/stopping fast path keys off the sem_any
// map: absent entry → per-CPU FIFO, no vtime, no global-DSQ lock.
// Userspace keeps that gate a conservative superset of the published
// semantic state by counting, per tid, how many semantic reasons are
// currently live (tier != NORMAL, commit_vip, blocked_by edge,
// lock_holder count).  add() publishes the gate BEFORE the semantic
// entry; remove() clears it AFTER the semantic entry is gone, so the
// scheduler never sees a gate entry with no state behind it (only the
// harmless reverse: state without a gate would be a bug, an extra gate
// just costs the slow path).
// -----------------------------------------------------------------------
static Sharded_map<int32_t, uint32_t> s_gate_refs;

static void gate_reason_add(int32_t tid) {
  if (tid < 0 || s_fd_sem_any < 0) return;
  const size_t i = s_gate_refs.idx(tid);
  bool first = false;
  {
    std::lock_guard<std::mutex> lock(s_gate_refs.mu[i]);
    first = ++s_gate_refs.m[i][tid] == 1;
  }
  if (first) {
    uint8_t one = 1;
    bpf_map_update_elem(s_fd_sem_any, &tid, &one, BPF_ANY);
  }
}

static void gate_reason_remove(int32_t tid) {
  if (tid < 0 || s_fd_sem_any < 0) return;
  const size_t i = s_gate_refs.idx(tid);
  bool last = false;
  {
    std::lock_guard<std::mutex> lock(s_gate_refs.mu[i]);
    auto it = s_gate_refs.m[i].find(tid);
    if (it == s_gate_refs.m[i].end()) return;
    if (it->second > 1) {
      it->second--;
    } else {
      s_gate_refs.m[i].erase(it);
      last = true;
    }
  }
  if (last) bpf_map_delete_elem(s_fd_sem_any, &tid);
}

// -----------------------------------------------------------------------
// lock_holder value: effectively boolean "this tid holds a contended
// lock/latch" with a freshness timestamp.  The BPF scheduler only checks
// count > 0 and HOLDER_TTL_NS freshness, so all publish sites write
// {1, now} idempotently and clear sites delete eagerly — no
// read-modify-write, hence no cross-thread count race.  A lost or
// duplicated update can only leave a stale entry behind, which the
// scheduler stops honouring after 1s and the gate sweeper (below)
// garbage-collects on its next pass.
// -----------------------------------------------------------------------
struct holder_entry_v {
  uint32_t count;
  uint64_t ts_ns;  // CLOCK_MONOTONIC ns; matches bpf_ktime_get_ns()
};

static inline uint64_t now_ns() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

/** Publish/refresh the holder marker for @c tid (idempotent). */
static void publish_holder(int32_t tid) {
  struct holder_entry_v v {1, now_ns()};
  gate_reason_add(tid);
  bpf_map_update_elem(s_fd_lock_holder, &tid, &v, BPF_ANY);
}

/** Drop the holder marker for @c tid (eager clear; refcounted gate). */
static void clear_holder(int32_t tid) {
  bpf_map_delete_elem(s_fd_lock_holder, &tid);
  gate_reason_remove(tid);
}

// -----------------------------------------------------------------------
// Gate sweeper (self-healing).
//
// The eager add/remove pairs above keep the gate tight in the common case,
// but the publication surface has many interleaved paths (THD lock waits
// cleared by the granter's thread, latch edges colliding with per-job
// lock_holder entries on the same tid, connections killed mid-wait), and
// every missed remove leaks a gate entry.  A leaked gate does not break
// correctness — it moves the tid to the slow path permanently.  Left
// alone, all worker tids end up gated within minutes under a contended
// load and the two-pool design degenerates to the single-pool behaviour.
//
// The sweeper is the final authority: every sweep period it rebuilds the
// wanted gate set as the union of keys of the four semantic maps (each
// entry == one reason, by construction), reconciles sem_any against it,
// and rebuilds s_gate_refs from the same ground truth.  Races with the
// eager paths are bounded: a gate removed while its state is being
// (re-)published is re-added by the next sweep, so worst case is one
// sweep period of slow-path scheduling for that tid.
// -----------------------------------------------------------------------
static std::atomic<bool> s_sweeper_started{false};

/** Collect the keys of a tid-keyed hash map into `out`, counting
 *  occurrences per tid in `refs` (used to rebuild the gate refcount). */
static void sweep_collect(int fd, std::unordered_set<int32_t> *out,
                          std::unordered_map<int32_t, uint32_t> *refs) {
  if (fd < 0) return;
  int32_t key = 0, next = 0;
  bool first = true;
  while (true) {
    int ret = bpf_map_get_next_key(fd, first ? nullptr : &key, &next);
    first = false;
    if (ret != 0) break;
    key = next;
    out->insert(key);
    ++(*refs)[key];
  }
}

static void gate_sweep_once() {
  /* GC: drop stale lock_holder entries first.  The value is boolean with
   * a freshness timestamp; a lost update or a missed clear would
   * otherwise leave a permanently-boosted "holder" behind.  The BPF side
   * already stops honouring entries older than 1s (HOLDER_TTL_NS); the
   * sweeper deletes them at 2s so a genuinely contended holder that is
   * being refreshed is never collected. */
  if (s_fd_lock_holder >= 0) {
    constexpr uint64_t kGcTtlNs = 2ull * 1000 * 1000 * 1000;
    uint64_t now = now_ns();
    int32_t key = 0, next = 0;
    bool first = true;
    while (bpf_map_get_next_key(s_fd_lock_holder, first ? nullptr : &key,
                                &next) == 0) {
      first = false;
      key = next;
      struct holder_entry_v v {};
      if (bpf_map_lookup_elem(s_fd_lock_holder, &key, &v) == 0 &&
          now - v.ts_ns > kGcTtlNs)
        bpf_map_delete_elem(s_fd_lock_holder, &key);
    }
  }

  std::unordered_set<int32_t> wanted;
  std::unordered_map<int32_t, uint32_t> refs;
  sweep_collect(s_fd_task_tier, &wanted, &refs);
  sweep_collect(s_fd_commit_vip, &wanted, &refs);
  sweep_collect(s_fd_blocked_by, &wanted, &refs);
  sweep_collect(s_fd_lock_holder, &wanted, &refs);

  /* Remove gates with no semantic state behind them. */
  {
    std::vector<int32_t> current;
    int32_t key = 0, next = 0;
    bool first = true;
    while (bpf_map_get_next_key(s_fd_sem_any, first ? nullptr : &key,
                                &next) == 0) {
      first = false;
      key = next;
      current.push_back(key);
    }
    for (int32_t tid : current)
      if (wanted.find(tid) == wanted.end())
        bpf_map_delete_elem(s_fd_sem_any, &tid);
  }
  /* Add gates that exist semantically but are missing (e.g. a racing
   * eager remove). */
  for (int32_t tid : wanted) {
    uint8_t val = 0;
    if (bpf_map_lookup_elem(s_fd_sem_any, &tid, &val) != 0) {
      uint8_t one = 1;
      bpf_map_update_elem(s_fd_sem_any, &tid, &one, BPF_ANY);
    }
  }

  /* Rebuild the refcount from ground truth so the eager paths stay
   * balanced from here on.  The collected refs are bucketed per shard
   * first so each shard lock is taken exactly once, with no map
   * operation held under it. */
  {
    std::vector<std::vector<std::pair<int32_t, uint32_t>>> per_shard(
        s_gate_refs.kCount);
    for (const auto &kv : refs)
      per_shard[s_gate_refs.idx(kv.first)].push_back(kv);
    for (size_t i = 0; i < s_gate_refs.kCount; ++i) {
      if (per_shard[i].empty()) continue;
      std::lock_guard<std::mutex> lock(s_gate_refs.mu[i]);
      auto &dst = s_gate_refs.m[i];
      dst.clear();
      for (const auto &kv : per_shard[i]) dst[kv.first] = kv.second;
    }
  }
}

static void gate_sweeper_main() {
  /* 2s: short enough to bound gate-leak erosion of the fast path, long
   * enough to be invisible in CPU cost (four map walks of <= 8k entries). */
  constexpr auto kSweepPeriod = std::chrono::seconds(2);
  while (true) {
    std::this_thread::sleep_for(kSweepPeriod);
    if (!s_connected || s_fd_sem_any < 0) continue;
    gate_sweep_once();
  }
}

// -----------------------------------------------------------------------
// pthread_t -> OS tid registry.
//
// The BPF maps are keyed by OS tid (the kernel's per-thread pid, i.e. the
// value of gettid()).  A THD's owning thread is stored as a pthread_t
// (THD::real_id), which is opaque.  Executor/worker threads call
// thread_start() from themselves, registering pthread_t -> gettid(), so the
// bridge can translate a THD into the numeric tid the scheduler needs.
// Shard-split like the wait graph: in the classic architecture every
// connection registers at connect and thread_end erases at disconnect.
// -----------------------------------------------------------------------
static Sharded_map<my_thread_t, int32_t> s_tid_registry;
/* Second registry for latch wait edges: std::thread::id -> OS tid. */
static Sharded_map<std::thread::id, int32_t> s_std_tid_registry;

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
  /** Whether thread_job_begin published a TIER_TS task_tier entry that
   *  thread_job_end must clear again. */
  bool published_ts_tier = false;
  /** Whether thread_job_begin published a resource-group-derived
   *  task_tier entry and rg_prio entry that thread_job_end must clear. */
  bool published_rg_tier = false;
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
  const size_t i = s_tid_registry.idx(thd->real_id);
  std::lock_guard<std::mutex> lock(s_tid_registry.mu[i]);
  auto it = s_tid_registry.m[i].find(thd->real_id);
  return it == s_tid_registry.m[i].end() ? -1 : it->second;
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
  /* The waiter's tid is the calling thread (lock_wait_begin runs on the
   * waiter); the THD itself is not needed here. */
  (void)waiter;

  int32_t waiter_tid = current_tid();
  int32_t holder_tid = thd_os_tid(holder);
  if (holder_tid < 0) return;

  uint32_t count = holder_wait_count(holder);
  if (count == 0) return;

  /* blocked_by[waiter] = holder */
  gate_reason_add(waiter_tid);
  bpf_map_update_elem(s_fd_blocked_by, &waiter_tid, &holder_tid, BPF_ANY);

  /* lock_holder[holder] = {1, now} (idempotent boolean marker) */
  publish_holder(holder_tid);
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
    /* The holder marker is boolean now: this waiter's clear drops the
     * entry even if other waiters remain; the boost has done its job by
     * the time a wait resolves, and any fresh wait re-publishes. */
    clear_holder(holder_tid);
    bpf_map_delete_elem(s_fd_blocked_by, &waiter_tid);
    gate_reason_remove(waiter_tid);
  }
}

/* Resolve a latch holder's std::thread::id to its OS tid, or -1. */
static int32_t latch_owner_tid(std::thread::id owner) {
  const size_t i = s_std_tid_registry.idx(owner);
  std::lock_guard<std::mutex> lock(s_std_tid_registry.mu[i]);
  auto it = s_std_tid_registry.m[i].find(owner);
  return it == s_std_tid_registry.m[i].end() ? -1 : it->second;
}

void scx_publish_latch_edge(std::thread::id owner) {
  if (!s_connected || owner == std::thread::id{}) return;

  int32_t holder_tid = latch_owner_tid(owner);
  if (holder_tid < 0) return;
  int32_t waiter_tid = current_tid();

  /* blocked_by[waiter] = holder */
  gate_reason_add(waiter_tid);
  bpf_map_update_elem(s_fd_blocked_by, &waiter_tid, &holder_tid, BPF_ANY);

  /* lock_holder[holder] = {1, now} (idempotent boolean marker) */
  publish_holder(holder_tid);
}

void scx_clear_latch_edge() {
  if (!s_connected) return;

  int32_t waiter_tid = current_tid();
  int32_t holder_tid = 0;
  if (bpf_map_lookup_elem(s_fd_blocked_by, &waiter_tid, &holder_tid) != 0)
    return;
  bpf_map_delete_elem(s_fd_blocked_by, &waiter_tid);
  gate_reason_remove(waiter_tid);
  clear_holder(holder_tid);
}

}  // namespace

// -----------------------------------------------------------------------
// Public API (BPF half)
// -----------------------------------------------------------------------

bool connected() { return s_connected; }

void thread_start() {
  const int32_t tid = current_tid();
  {
    const my_thread_t self = my_thread_self();
    const size_t i = s_tid_registry.idx(self);
    std::lock_guard<std::mutex> lock(s_tid_registry.mu[i]);
    s_tid_registry.m[i][self] = tid;
  }
  {
    const std::thread::id sid = std::this_thread::get_id();
    const size_t i = s_std_tid_registry.idx(sid);
    std::lock_guard<std::mutex> lock(s_std_tid_registry.mu[i]);
    s_std_tid_registry.m[i][sid] = tid;
  }
}

void thread_end() {
  {
    const my_thread_t self = my_thread_self();
    const size_t i = s_tid_registry.idx(self);
    std::lock_guard<std::mutex> lock(s_tid_registry.mu[i]);
    s_tid_registry.m[i].erase(self);
  }
  {
    const std::thread::id sid = std::this_thread::get_id();
    const size_t i = s_std_tid_registry.idx(sid);
    std::lock_guard<std::mutex> lock(s_std_tid_registry.mu[i]);
    s_std_tid_registry.m[i].erase(sid);
  }
}

void thread_job_begin(THD *thd, bool lock_holder, bool time_sensitive) {
  if (!s_connected || thd == nullptr) return;

  int32_t tid = current_tid();

  /* Resource-group tier mapping.  In the coroutine architecture the OS
   * thread changes between a session's statements, so a group's priority
   * (Linux: nice, 0..19 for user groups) is published per job:
   *   priority > 0  (low urgency, e.g. a batch/analytics group) → BG tier
   *   priority < 0  (high urgency, admin groups)               → TS tier
   * The nice value itself is also published to the scheduler's rg_prio
   * map, which weights vtime inside the normal tier. */
  {
    auto ctx = thd->resource_group_ctx();
    resourcegroups::Resource_group *rg =
        ctx ? ctx->m_cur_resource_group : nullptr;
    if (rg != nullptr && rg->enabled()) {
      int prio = rg->controller()->priority();
      if (prio != 0) {
        int32_t val = prio;
        bpf_map_update_elem(s_fd_rg_prio, &tid, &val, BPF_ANY);
        int32_t tier = (prio > 0) ? kTierBG : kTierTS;
        gate_reason_add(tid);
        bpf_map_update_elem(s_fd_task_tier, &tid, &tier, BPF_ANY);
        s_thread_pub.published_rg_tier = true;
      }
    }
  }

  if (lock_holder) {
    publish_holder(tid);
    s_thread_pub.published = true;
    s_thread_pub.tid = tid;
  }

  /* High-priority (lock-holder) jobs run at time-sensitive priority for
   * their duration: the DB-level HP queue decision is propagated to the
   * OS scheduler, so the job also preempts normal work on its CPU.
   * This overrides a resource-group BG tier: the holder's COMMIT must
   * drain so blocked waiters can proceed. */
  if (time_sensitive) {
    int32_t ts = kTierTS;
    /* rg-tier (if any) already registered a gate reason for the same
     * task_tier entry — do not double-count. */
    if (!s_thread_pub.published_rg_tier)
      gate_reason_add(tid);
    bpf_map_update_elem(s_fd_task_tier, &tid, &ts, BPF_ANY);
    s_thread_pub.published_ts_tier = true;
    s_thread_pub.tid = tid;
  }
}

void thread_job_end(THD *thd) {
  if (!s_connected || thd == nullptr) return;
  if (!s_thread_pub.published && !s_thread_pub.published_ts_tier &&
      !s_thread_pub.published_rg_tier)
    return;

  int32_t tid = s_thread_pub.tid;
  if (tid < 0) tid = current_tid();
  if (s_thread_pub.published) {
    clear_holder(tid);
    s_thread_pub.published = false;
  }
  if (s_thread_pub.published_rg_tier) {
    bpf_map_delete_elem(s_fd_rg_prio, &tid);
  }
  if (s_thread_pub.published_ts_tier || s_thread_pub.published_rg_tier) {
    bpf_map_delete_elem(s_fd_task_tier, &tid);
    gate_reason_remove(tid);
  }
  s_thread_pub.published_ts_tier = false;
  s_thread_pub.published_rg_tier = false;
  s_thread_pub.tid = -1;
}

void command_begin(THD *thd) {
  if (!s_connected || thd == nullptr) return;
  /* Classic (per-thread / no-threads / plugin) architectures only: the
   * coroutine executor brackets its jobs with thread_job_begin/end itself,
   * and a second publication here would unbalance the gate refcount. */
  if (Connection_handler_manager::thread_handling ==
      Connection_handler_manager::SCHEDULER_COROUTINE_EPOLL)
    return;
  thread_job_begin(thd, any_lock_waits() && is_lock_holder(thd), false);
}

void command_end(THD *thd) {
  if (!s_connected || thd == nullptr) return;
  if (Connection_handler_manager::thread_handling ==
      Connection_handler_manager::SCHEDULER_COROUTINE_EPOLL)
    return;
  thread_job_end(thd);
}

void set_thread_tier(int tier) {
  if (!s_connected) return;
  int32_t tid = current_tid();
  if (tier == kTierNormal) {
    bpf_map_delete_elem(s_fd_task_tier, &tid);
    gate_reason_remove(tid);
  } else {
    int32_t val = tier;
    gate_reason_add(tid);
    bpf_map_update_elem(s_fd_task_tier, &tid, &val, BPF_ANY);
  }
}

void clear_thread_tier() { set_thread_tier(kTierNormal); }

void commit_begin() {
  if (!s_connected) return;
  int32_t tid = current_tid();
  uint8_t val = 1;
  gate_reason_add(tid);
  bpf_map_update_elem(s_fd_commit_vip, &tid, &val, BPF_ANY);
}

void commit_end() {
  if (!s_connected) return;
  int32_t tid = current_tid();
  bpf_map_delete_elem(s_fd_commit_vip, &tid);
  gate_reason_remove(tid);
}

void lock_holder_add(my_thread_id holder_tid) {
  if (!s_connected) return;
  publish_holder(static_cast<int32_t>(holder_tid));
}

void lock_holder_remove(my_thread_id holder_tid) {
  if (!s_connected) return;
  clear_holder(static_cast<int32_t>(holder_tid));
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

  /* Stale-pin guard: the loader pins its maps under
   * /sys/fs/bpf/scx_db_sched and the pins survive the loader's exit even
   * though the scheduler behind them is gone.  Attaching to those dead
   * maps makes every hook site pay the publication cost for nothing
   * (observed as a connected bridge with sched_ext disabled).  Only
   * attach while a sched_ext scheduler is actually loaded. */
  {
    std::ifstream state("/sys/kernel/sched_ext/state");
    std::string v;
    if (!(state >> v) || v != "enabled") return;
  }

  s_fd_commit_vip  = open_map(kMapCommitVIP);
  s_fd_blocked_by  = open_map(kMapBlockedBy);
  s_fd_lock_holder = open_map(kMapLockHolder);
  s_fd_rg_prio     = open_map(kMapRgPrio);
  /* task_tier is new; tolerate its absence (older scheduler build). */
  s_fd_task_tier   = open_map(kMapTaskTier);
  /* sem_any gate (two-pool fast path); required — a v5 scheduler
   * without it would fast-path every task and drop all semantics. */
  s_fd_sem_any     = open_map(kMapSemAny);

  if (s_fd_commit_vip >= 0 && s_fd_blocked_by >= 0 &&
      s_fd_lock_holder >= 0 && s_fd_rg_prio >= 0 && s_fd_sem_any >= 0) {
    s_connected = true;
    /* Fresh scheduler session: any refcount state from a previous
     * session is meaningless (the BPF maps were recreated empty). */
    for (size_t i = 0; i < s_gate_refs.kCount; ++i) {
      std::lock_guard<std::mutex> lock(s_gate_refs.mu[i]);
      s_gate_refs.m[i].clear();
    }
    /* Start the gate sweeper once per process.  It is a detached thread
     * that only touches the BPF fds and leaky-static state, so process
     * exit needs no join. */
    if (!s_sweeper_started.exchange(true)) {
      std::thread(gate_sweeper_main).detach();
    }
  } else {
    /* Partial failure — close whatever we opened. */
    close_map(s_fd_commit_vip);
    close_map(s_fd_blocked_by);
    close_map(s_fd_lock_holder);
    close_map(s_fd_rg_prio);
    close_map(s_fd_task_tier);
    close_map(s_fd_sem_any);
    s_fd_commit_vip  = -1;
    s_fd_blocked_by  = -1;
    s_fd_lock_holder = -1;
    s_fd_rg_prio     = -1;
    s_fd_task_tier   = -1;
    s_fd_sem_any     = -1;
    s_connected = false;
  }
}

}  // namespace sched_ext

#endif  // HAVE_SCX_SCHEDULER
