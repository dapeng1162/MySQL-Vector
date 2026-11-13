/* SPDX-License-Identifier: GPL-2.0 */
/*
 * scx_db_sched (smart_sched): DB-aware sched_ext scheduler for MySQL 9.7.
 *
 * Design (see Docs/smart-sched-design.md).  Built on the v7-rc1 minimal
 * EEVDF base after the v6.4/v7-rc1 ablation results:
 *   - L0 base (always on): one queue family, per-CPU vtime DSQs, lag
 *     clamp, adaptive slice (pool-depth 20ms->2.5ms), a REAL CPU-bound
 *     detector whose cap bounds the head-of-line wait a hog imposes
 *     (the detector was dead code from v6.4 through v7-rc1 until the
 *     off-CPU-gap decay fix).  Exact on-CPU interval accounting.
 *   - L1 semantic tilts (switch-gated, off by default): bounded virtual
 *     -deadline bonuses for COMMIT/ROLLBACK windows (S2), lock/latch
 *     holders with waiters (S3), and priority-inheritance floors (S3).
 *     All tilted tasks land in the SAME per-CPU pool with a vd offset —
 *     never a separate queue (v6.4's global DSQ cost and its lock are
 *     gone).  Borrowed CPU is repaid as debt by idling (cpu burst).
 *   - L2 preemption: no kick by default (kick's IPI cost is linear in
 *     rate and made up ~11pp of v6.4's rw deficit).  R1 budgeted wakeup
 *     kick (granularity + per-CPU token bucket) for the noise tail;
 *     S4 semantic kick only for tilted wakeups, spending IPIs where
 *     semantic knowledge says they pay.
 *
 * Requires: Linux kernel with CONFIG_SCHED_CLASS_EXT (sched_ext).
 *
 * Copyright (c) 2026, Oracle and/or its affiliates.
 */

#include <scx/common.bpf.h>

char _license[] SEC("license") = "GPL";

UEI_DEFINE(uei);

/* Per-CPU vtime pools, id = CPU_DSQ_BASE + cpu.  The builtin
 * SCX_DSQ_LOCAL is deliberately unused: the core only calls dispatch()
 * when the builtin local DSQ is empty, so populating it would bypass
 * this scheduler's dispatch order. */
#define CPU_DSQ_BASE 0x1000
#define MAX_CPUS 512

/* Bounded wakeup credit: a task that slept keeps its lag (EEVDF burst
 * friendliness) but cannot hoard unbounded vruntime credit. */
#define MAX_LAG (100 * 1000 * 1000ULL)

/* CPU-bound containment.  A task whose undecayed on-CPU budget exceeds
 * CPUBOUND_BUDGET_NS (net on-CPU over the recent past) is treated as a
 * hog and its slice capped, bounding the head-of-line wait it can
 * impose on woken tasks even when a kick's preemption is delayed.
 * E1 (cap disabled) worsened the N1 noise tail 34.5ms -> 108ms. */
#define CPUBOUND_BUDGET_NS (20 * 1000 * 1000ULL)

/* L1 semantic tilts: bounded virtual-deadline bonuses (ns, vtime unit).
 * All three buy the same thing — shorten the critical path: commit
 * drains early, locks release early, latch holders leave the critical
 * section.  Borrowed CPU is repaid as debt (cgroup cpu burst idea), so
 * the tilt is free in steady state. */
#define B_COMMIT        SCX_SLICE_DFL       /* commit/rollback window */
#define B_HOLD          (SCX_SLICE_DFL >> 1) /* lock/latch holder */
#define B_MAX           (4 * SCX_SLICE_DFL)  /* per-window borrow cap */
#define D_MAX           (8 * SCX_SLICE_DFL)  /* outstanding debt cap */
#define PI_DEPTH_MAX    3
#define PI_STALE_NS     (1000 * 1000 * 1000ULL)

/* DB-internal background threads (purge, dict stats, fts optimize):
 * smaller weight => virtual deadline 4x later and vtime accumulates
 * 4x faster.  This is database self-knowledge, not business QoS. */
#define BG_WEIGHT       25
#define NORMAL_WEIGHT   100
#define TIER_BG         2

/* -----------------------------------------------------------------------
 * Runtime switches (loader-settable, same binary A/B).
 *   R1: wakeup preemption with granularity filter + per-CPU kick budget.
 *   R2/R3 knobs arrive with their stages; sem gate is counted, ignored.
 * ----------------------------------------------------------------------- */
const volatile bool scx_enable_wake_preempt = false; /* R1 */
const volatile u64 scx_wake_gran_ns = 1000 * 1000ULL; /* CFS-order granularity */
const volatile u32 scx_kick_budget_per_sec = 2048;    /* IPI/s per target CPU */
const volatile u64 scx_hog_slice_ns = 2500 * 1000ULL; /* 2.5ms default */
const volatile u32 scx_slice_shift = 0; /* adaptive slice ladder >>N (0=default) */
/* L1 semantic tilts (acceptance-gated, all off by default). */
const volatile bool scx_enable_commit_tilt = false;  /* S2 */
const volatile bool scx_enable_holder_tilt = false;  /* S3 lock/latch */
const volatile bool scx_enable_pi = false;           /* S3 priority inherit */
const volatile bool scx_enable_semantic_kick = false;/* S4 budgeted kick */

/* Global minimum-vtime tracker (lag clamp reference). */
static u64 vtime_now;

/* Number of per-CPU vtime DSQs actually created (set by init). */
static u32 nr_cpu_dsqs;

/* -----------------------------------------------------------------------
 * Userspace-published maps (pinned at /sys/fs/bpf/scx_db_sched/).
 * Kept for bridge ABI compatibility: the mysqld bridge (see
 * sql/sched_ext_bridge.cc) requires every map to be openable or it
 * disables itself — and the instrumentation cost must stay in the
 * measurement (C5n: -2.9% on CFS).  R0 reads only sem_any, and only to
 * count; the semantic state is ignored until R2/R3.
 * ----------------------------------------------------------------------- */
struct {
  __uint(type, BPF_MAP_TYPE_HASH);
  __uint(max_entries, 4096);
  __type(key, u32);
  __type(value, u8);
} commit_vip SEC(".maps");

struct {
  __uint(type, BPF_MAP_TYPE_HASH);
  __uint(max_entries, 4096);
  __type(key, u32);
  __type(value, u32);
} blocked_by SEC(".maps");

struct holder_entry {
  u32 count;
  u64 ts_ns;
};
#define HOLDER_TTL_NS (1000 * 1000 * 1000ULL)
struct {
  __uint(type, BPF_MAP_TYPE_HASH);
  __uint(max_entries, 4096);
  __type(key, u32);
  __type(value, struct holder_entry);
} lock_holder SEC(".maps");

struct {
  __uint(type, BPF_MAP_TYPE_HASH);
  __uint(max_entries, 4096);
  __type(key, u32);
  __type(value, s32);
} rg_prio SEC(".maps");

struct {
  __uint(type, BPF_MAP_TYPE_HASH);
  __uint(max_entries, 4096);
  __type(key, u32);
  __type(value, s32);
} task_tier SEC(".maps");

/* sem_any[tid] = 1  →  the bridge has some semantic state published for
 * this tid.  R0 counts hits and otherwise ignores the entry. */
struct {
  __uint(type, BPF_MAP_TYPE_HASH);
  __uint(max_entries, 8192);
  __type(key, u32);
  __type(value, u8);
} sem_any SEC(".maps");

/* L1 priority-inheritance state: holder_boost[holder_tid] = the most
 * urgent waiter's effective vd (floor), published by waiters through
 * blocked_by edges.  Deleted on use or when stale. */
struct boost_entry {
  u64 floor;
  u64 ts;
  u32 depth;
};

struct {
  __uint(type, BPF_MAP_TYPE_HASH);
  __uint(max_entries, 4096);
  __type(key, u32);
  __type(value, struct boost_entry);
} holder_boost SEC(".maps");

/* -----------------------------------------------------------------------
 * Per-task account: exact runtime charge + CPU-bound token bucket.
 * ----------------------------------------------------------------------- */
struct task_account {
  u64 slice_ns;      /* slice granted at last insert (fallback charge) */
  u64 start_ns;      /* on-CPU start timestamp, set in running() */
  u64 cpu_budget;    /* undecayed on-CPU credit (CPU-bound detector) */
  u64 last_stop_ns;  /* wall time of the last stop (decay reference) */
  u64 borrowed;      /* CPU consumed inside L1 tilt windows */
  u64 debt;          /* closed borrow, repaid by idle time */
};

struct {
  __uint(type, BPF_MAP_TYPE_HASH);
  __uint(max_entries, 8192);
  __type(key, u32);
  __type(value, struct task_account);
} task_account_map SEC(".maps");

/* R1 kick budget, one bucket per target CPU (indexed by cpu id). */
struct kick_bucket {
  u64 window_start_ns;
  s64 tokens;
};

struct {
  __uint(type, BPF_MAP_TYPE_ARRAY);
  __uint(max_entries, MAX_CPUS);
  __type(key, u32);
  __type(value, struct kick_bucket);
} kick_budget SEC(".maps");

/* Per-CPU statistics (aggregate across CPUs to read):
 *   [0] inserts, [1] sem gate hits, [2] cpu-bound cap applied,
 *   [3] kicks sent, [4] kicks below granularity, [5] kicks over budget,
 *   [6] dispatch steals, [7] commit tilt, [8] holder tilt,
 *   [9] PI floor publishes, [10] PI floor applies, [11] semantic kicks,
 *   [12] BG tier enqueues. */
struct {
  __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
  __uint(max_entries, 16);
  __type(key, u32);
  __type(value, u64);
} stats SEC(".maps");

static void stat_inc(u32 idx)
{
  u64 *cnt = bpf_map_lookup_elem(&stats, &idx);
  if (cnt)
    __sync_fetch_and_add(cnt, 1);
}

/* Fetch or create the per-task account. */
static struct task_account *account_of(u32 tid)
{
  struct task_account *st = bpf_map_lookup_elem(&task_account_map, &tid);
  if (!st) {
    struct task_account init = {};
    bpf_map_update_elem(&task_account_map, &tid, &init, BPF_NOEXIST);
    st = bpf_map_lookup_elem(&task_account_map, &tid);
  }
  return st;
}

/* L1 helpers. */
static inline s32 get_tier(u32 tid)
{
  s32 *t = bpf_map_lookup_elem(&task_tier, &tid);
  return t ? *t : 1;  /* TIER_NORMAL */
}

static bool kick_budget_allow(s32 cpu, u64 now);

/* Close an open borrow window: borrowed becomes debt (bounded). */
static void close_borrow_window(struct task_account *st)
{
  if (!st->borrowed)
    return;
  st->debt += st->borrowed;
  if (st->debt > D_MAX)
    st->debt = D_MAX;
  st->borrowed = 0;
}

/* L1 enqueue path, entered only when the bridge has published semantic
 * state for this tid AND at least one L1 switch is on.  Everything lands
 * in the SAME per-CPU vtime pool as the base path — the tilt is a vd
 * offset, never a different queue (v6.4's global DSQ is gone). */
static void enqueue_semantic(struct task_struct *p, u64 enq_flags,
                             u32 tid, s32 cpu, u64 vr, u64 slice,
                             struct task_account *st)
{
  u64 now = scx_bpf_now();
  u32 weight = NORMAL_WEIGHT;
  u64 vd;
  bool tilted = false;

  if (get_tier(tid) == TIER_BG) {
    weight = BG_WEIGHT;
    stat_inc(12);
  }
  vd = vr + SCX_SLICE_DFL * 100 / weight;

  /* Commit window: bounded vd bonus so COMMIT/ROLLBACK drains early. */
  if (scx_enable_commit_tilt) {
    u8 *vip = bpf_map_lookup_elem(&commit_vip, &tid);
    if (vip && *vip) {
      stat_inc(7);
      if (!st || st->borrowed < B_MAX)
        vd = vd > B_COMMIT ? vd - B_COMMIT : 0;
      tilted = true;
    }
  }

  /* Lock/latch holder with waiters: bounded vd bonus so the critical
   * section (and the lock release after it) finishes early.  A stale
   * entry (lost update / leaked clear) is ignored via HOLDER_TTL_NS. */
  if (scx_enable_holder_tilt) {
    struct holder_entry *he = bpf_map_lookup_elem(&lock_holder, &tid);
    if (he && he->count > 0 && now - he->ts_ns <= HOLDER_TTL_NS) {
      stat_inc(8);
      if (!st || st->borrowed < B_MAX)
        vd = vd > B_HOLD ? vd - B_HOLD : 0;
      tilted = true;
    }
  }

  /* Priority inheritance: if I am a waiter, publish my effective vd as
   * a floor for my holder; if a fresher floor was published for me,
   * apply it.  Depth-capped by the bridge's short InnoDB wait chains. */
  if (scx_enable_pi) {
    u32 *holder = bpf_map_lookup_elem(&blocked_by, &tid);
    if (holder && *holder) {
      struct boost_entry ne = { .floor = vd, .ts = now, .depth = 0 };
      bpf_map_update_elem(&holder_boost, holder, &ne, BPF_ANY);
      stat_inc(9);
    }
    {
      struct boost_entry *hb = bpf_map_lookup_elem(&holder_boost, &tid);
      if (hb) {
        if (now - hb->ts <= PI_STALE_NS && hb->floor < vd) {
          vd = hb->floor;
          tilted = true;
          stat_inc(10);
        }
        bpf_map_delete_elem(&holder_boost, &tid);
      }
    }
  }

  /* Debt from previous tilt windows: repaid by idling (running()), the
   * remainder defers this insert. */
  if (st)
    vd += st->debt;

  scx_bpf_dsq_insert_vtime(p, CPU_DSQ_BASE + (u64)cpu, slice, vd, enq_flags);

  /* S4 semantic kick: only a tilted wakeup is worth an IPI, and only
   * within the per-CPU budget.  This is where semantic knowledge buys
   * preemption efficiency the blind fast-path kick never had. */
  if (scx_enable_semantic_kick && tilted &&
      (enq_flags & SCX_ENQ_WAKEUP) && kick_budget_allow(cpu, now)) {
    stat_inc(11);
    scx_bpf_kick_cpu(cpu, SCX_KICK_PREEMPT);
  }
}

/* Adaptive slice, shrinking with the per-CPU pool depth: 20ms when the
 * pool is empty or shallow, halving at depth 2/4/8 down to 2.5ms —
 * CFS's saturation granularity.  A CPU-bound task can hold the CPU for
 * at most one adaptive slice before the pool's next task (possibly a
 * just-woken one at the vtime front) is dispatched, while an unloaded
 * machine keeps full slices and near-zero switch overhead. */
static __always_inline u64 fast_slice(s32 cpu)
{
  u32 shift = scx_slice_shift > 4 ? 4 : scx_slice_shift;
  s32 nr = scx_bpf_dsq_nr_queued(CPU_DSQ_BASE + (u64)cpu);
  if (nr < 0)
    nr = 0;
  if (nr >= 8)
    return SCX_SLICE_DFL >> (3 + shift > 5 ? 5 : 3 + shift);
  if (nr >= 4)
    return SCX_SLICE_DFL >> (2 + shift > 5 ? 5 : 2 + shift);
  if (nr >= 2)
    return SCX_SLICE_DFL >> (1 + shift > 5 ? 5 : 1 + shift);
  return SCX_SLICE_DFL >> shift;
}

/* R1: per-target-CPU token bucket.  Refills to scx_kick_budget_per_sec
 * once per second; a kick is allowed only while tokens remain, so an
 * extreme wakeup storm degrades to "wait for the current slice" instead
 * of unbounded IPI traffic. */
static bool kick_budget_allow(s32 cpu, u64 now)
{
  struct kick_bucket *kb;
  u32 key = (u32)cpu;

  if (cpu < 0 || cpu >= MAX_CPUS)
    return false;
  kb = bpf_map_lookup_elem(&kick_budget, &key);
  if (!kb)
    return false;
  if (kb->window_start_ns == 0 ||
      now - kb->window_start_ns >= 1000 * 1000 * 1000ULL) {
    kb->window_start_ns = now;
    kb->tokens = scx_kick_budget_per_sec;
  }
  if (kb->tokens > 0) {
    kb->tokens--;
    return true;
  }
  return false;
}

/* -----------------------------------------------------------------------
 * select_cpu: cache-affine default placement (prev_cpu + idle scan).
 * ----------------------------------------------------------------------- */
s32 BPF_STRUCT_OPS(db_sched_select_cpu, struct task_struct *p,
                   s32 prev_cpu, u64 wake_flags)
{
  bool is_idle = false;
  return scx_bpf_select_cpu_dfl(p, prev_cpu, wake_flags, &is_idle);
}

/* -----------------------------------------------------------------------
 * enqueue: single path for every task.
 * ----------------------------------------------------------------------- */
void BPF_STRUCT_OPS(db_sched_enqueue, struct task_struct *p, u64 enq_flags)
{
  u32 tid = p->pid;
  s32 cpu = scx_bpf_task_cpu(p);
  u64 vr, slice;
  struct task_account *st;
  u8 *gate;

  stat_inc(0);

  /* One semantic lookup per wakeup (the L1 hook point).  When the gate
   * misses (no semantic state published for this tid) the task takes
   * the plain base path below at zero extra cost. */
  gate = bpf_map_lookup_elem(&sem_any, &tid);
  if (gate) {
    stat_inc(1);
    if (scx_enable_commit_tilt || scx_enable_holder_tilt ||
        scx_enable_pi || scx_enable_semantic_kick) {
      u32 tid2 = tid;
      vr = p->scx.dsq_vtime;
      if (vr + MAX_LAG < vtime_now)
        vr = vtime_now - MAX_LAG;
      slice = fast_slice(cpu);
      st = account_of(tid2);
      if (st) {
        if (st->cpu_budget > CPUBOUND_BUDGET_NS &&
            slice > scx_hog_slice_ns) {
          slice = scx_hog_slice_ns;
          stat_inc(2);
        }
        st->slice_ns = slice;
      }
      enqueue_semantic(p, enq_flags, tid2, cpu, vr, slice, st);
      return;
    }
  }

  /* Lag-clamped vruntime: a task that slept keeps its credit, bounded. */
  vr = p->scx.dsq_vtime;
  if (vr + MAX_LAG < vtime_now)
    vr = vtime_now - MAX_LAG;

  /* Adaptive slice, capped once the task trips the CPU-bound detector.
   * The slice is recorded HERE (pool depth now, not at stop time) so
   * stopping() charges what was granted, not what an empty pool would
   * imply — recomputing at stop time overcharged sleepers (v6.2 N1
   * 21ms delay clusters). */
  slice = fast_slice(cpu);
  st = account_of(tid);
  if (st) {
    if (st->cpu_budget > CPUBOUND_BUDGET_NS &&
        slice > scx_hog_slice_ns) {
      slice = scx_hog_slice_ns;
      stat_inc(2);
    }
    st->slice_ns = slice;
  }

  /* R1 wakeup preemption: only wakers that genuinely slept (lag beyond
   * CFS-order granularity) and only within the per-CPU IPI budget.
   * v6.4 kicked on nearly every wake (100us threshold, no budget):
   * E2 showed that only accounts for a small part of the tail, and the
   * granularity + budget form is the CFS-shaped replacement. */
  if (scx_enable_wake_preempt && (enq_flags & SCX_ENQ_WAKEUP) &&
      vtime_now > p->scx.dsq_vtime) {
    if (vtime_now - p->scx.dsq_vtime <= scx_wake_gran_ns) {
      stat_inc(4);
    } else if (kick_budget_allow(cpu, scx_bpf_now())) {
      stat_inc(3);
      scx_bpf_kick_cpu(cpu, SCX_KICK_PREEMPT);
    } else {
      stat_inc(5);
    }
  }

  scx_bpf_dsq_insert_vtime(p, CPU_DSQ_BASE + (u64)cpu, slice, vr, enq_flags);
}

/* -----------------------------------------------------------------------
 * dispatch: local pool first, then round-robin neighbour steal (idle
 * path only — under saturation the local pool rarely runs dry).
 * ----------------------------------------------------------------------- */
void BPF_STRUCT_OPS(db_sched_dispatch, s32 cpu, struct task_struct *prev)
{
  u32 i, nr = nr_cpu_dsqs;

  if (scx_bpf_dsq_move_to_local(CPU_DSQ_BASE + (u64)cpu, 0))
    return;

  for (i = 1; i < MAX_CPUS; i++) {
    s32 victim;

    if (i >= nr)
      break;
    victim = (s32)((cpu + i) % nr);
    if (scx_bpf_dsq_move_to_local(CPU_DSQ_BASE + (u64)victim, 0)) {
      stat_inc(6);
      return;
    }
  }
}

/* -----------------------------------------------------------------------
 * running / stopping: exact on-CPU interval accounting.
 * ----------------------------------------------------------------------- */
void BPF_STRUCT_OPS(db_sched_running, struct task_struct *p)
{
  u32 pid = p->pid;
  u64 now = scx_bpf_now();
  struct task_account *st;

  if (time_before(vtime_now, p->scx.dsq_vtime))
    vtime_now = p->scx.dsq_vtime;

  st = bpf_map_lookup_elem(&task_account_map, &pid);
  if (st) {
    st->start_ns = now;
    /* Decay the CPU-bound budget by the task's OFF-CPU gap only (last
     * stop -> this start).  Decaying at stop time by (now - last stop)
     * includes the task's own runtime, and since runtime <= that gap
     * the budget could never grow: the hog detector was dead code from
     * v6.4 through v7-rc1 (N1: hogs ran full 10-20ms grants and every
     * wakeup waited half a grant; avg 5.2ms at noise@10k). */
    if (st->last_stop_ns && now > st->last_stop_ns) {
      u64 gap = now - st->last_stop_ns;
      st->cpu_budget = st->cpu_budget > gap ? st->cpu_budget - gap : 0;
      /* L1 debt is repaid by idling (cgroup cpu burst: borrowed CPU is
       * returned by waiting, not by running). */
      st->debt = st->debt > gap ? st->debt - gap : 0;
    }
  }
}

void BPF_STRUCT_OPS(db_sched_stopping, struct task_struct *p, bool runnable)
{
  u32 tid = p->pid;
  u64 now = scx_bpf_now();
  struct task_account *st = bpf_map_lookup_elem(&task_account_map, &tid);

  if (!st)
    return;

  /* Charge the runtime the task actually consumed — the
   * running() -> stopping() wall interval, which survives
   * scx_bpf_kick_cpu() zeroing p->scx.slice (v6.3 charged preempted
   * hogs their full grant and inflated vtime_now until the lag clamp
   * collapsed pool ordering).  Fallback: granted slice minus remaining
   * (account raced with disable/tid reuse). */
  if (st->start_ns && now > st->start_ns) {
    u64 rt = now - st->start_ns;

    /* CPU-bound detector: accrue on-CPU time here; the off-CPU gap is
     * decayed in running(), so a continuously-running hog's budget
     * grows to the cap while a sleeper's stays near zero. */
    st->cpu_budget += rt;
    st->last_stop_ns = now;
    st->start_ns = 0;

    /* L1 accounting: BG weight scales the vtime charge (4x slower
     * accumulation), and runtime inside a tilt window is borrowed CPU
     * to be repaid as debt once the window closes. */
    if (scx_enable_commit_tilt || scx_enable_holder_tilt ||
        scx_enable_pi || scx_enable_semantic_kick) {
      if (bpf_map_lookup_elem(&sem_any, &tid)) {
        u32 weight = get_tier(tid) == TIER_BG ? BG_WEIGHT : NORMAL_WEIGHT;
        bool tilted = false;

        p->scx.dsq_vtime += rt * 100 / weight;
        if (scx_enable_commit_tilt) {
          u8 *vip = bpf_map_lookup_elem(&commit_vip, &tid);
          if (vip && *vip)
            tilted = true;
        }
        if (scx_enable_holder_tilt) {
          struct holder_entry *he = bpf_map_lookup_elem(&lock_holder, &tid);
          if (he && he->count > 0 && now - he->ts_ns <= HOLDER_TTL_NS)
            tilted = true;
        }
        if (tilted) {
          st->borrowed += rt;
          if (st->borrowed >= B_MAX)
            close_borrow_window(st);
        } else if (st->borrowed) {
          close_borrow_window(st);
        }
        return;
      }
    }
    p->scx.dsq_vtime += rt;
  } else if (st->slice_ns >= p->scx.slice) {
    p->scx.dsq_vtime += st->slice_ns - p->scx.slice;
  }
}

void BPF_STRUCT_OPS(db_sched_enable, struct task_struct *p)
{
  p->scx.dsq_vtime = vtime_now;
}

void BPF_STRUCT_OPS(db_sched_disable, struct task_struct *p)
{
  u32 tid = p->pid;
  bpf_map_delete_elem(&task_account_map, &tid);
  bpf_map_delete_elem(&holder_boost, &tid);
  /* The bridge refcounts sem_any, but a tid leaving the scheduler may
   * still carry a stale gate entry (tid reuse); scrub as the final
   * authority. */
  bpf_map_delete_elem(&sem_any, &tid);
}

/* -----------------------------------------------------------------------
 * init / exit
 * ----------------------------------------------------------------------- */
s32 BPF_STRUCT_OPS_SLEEPABLE(db_sched_init)
{
  s32 cpu, ret, nr;

  /* Per-CPU vtime pools.  The node argument is only a NUMA allocation
   * hint and must be NUMA_NO_NODE — passing a cpu index is rejected
   * with -EINVAL once cpu >= nr_node_ids. */
  nr = scx_bpf_nr_cpu_ids();
  if (nr > MAX_CPUS)
    nr = MAX_CPUS;
  for (cpu = 0; cpu < nr; cpu++) {
    ret = scx_bpf_create_dsq(CPU_DSQ_BASE + cpu, -1);
    if (ret)
      return ret;
  }
  nr_cpu_dsqs = nr;

  bpf_printk("scx_db_sched v7-rc1 (R0): %d per-CPU vtime DSQs, wake_preempt=%d",
             nr, scx_enable_wake_preempt);
  return 0;
}

void BPF_STRUCT_OPS(db_sched_exit, struct scx_exit_info *ei)
{
  UEI_RECORD(uei, ei);
}

SCX_OPS_DEFINE(db_sched_ops,
               .select_cpu    = (void *)db_sched_select_cpu,
               .enqueue       = (void *)db_sched_enqueue,
               .dispatch      = (void *)db_sched_dispatch,
               .running       = (void *)db_sched_running,
               .stopping      = (void *)db_sched_stopping,
               .enable        = (void *)db_sched_enable,
               .disable       = (void *)db_sched_disable,
               .init          = (void *)db_sched_init,
               .exit          = (void *)db_sched_exit,
               .name          = "db_sched");
