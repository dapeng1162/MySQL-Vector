/* SPDX-License-Identifier: GPL-2.0 */
/*
 * EEVDF-Sem: DB-aware sched_ext BPF scheduler for MySQL / InnoDB (v4).
 *
 * Design (see Docs/coroutine-sched-resilience-design.md):
 *   - Two pools.  Gated tasks (semantic state published by the DB bridge:
 *     tier != NORMAL, COMMIT/ROLLBACK, lock/latch edges) are ordered by
 *       vd = vruntime + slice * 100 / weight + debt - bonus
 *     in one global vtime DSQ (EEVDF "earliest eligible" idea).  Ungated
 *     tasks — the vast majority of wakeups — take a per-CPU FIFO DSQ
 *     fast path with no vtime math and no global-DSQ lock, so the
 *     single-lock bottleneck of the v4 single-pool design is gone.
 *     dispatch() drains the semantic pool first, then the CPU's FIFO
 *     pool, preserving commit/latch priority over fast-path work.
 *   - No strict-priority FIFO bypass, no SCX_SLICE_INF.
 *   - Lag clamp on enqueue: a task that slept keeps its lag (EEVDF's
 *     burst-friendliness) but cannot hoard unbounded credit.
 *   - COMMIT/ROLLBACK: bounded vd bonus (B_COMMIT), no infinite slice.
 *   - lock_holder: bounded slice + vd inheritance from the most urgent
 *     waiter (priority inheritance), with depth cap and timestamp sweep.
 *   - Borrow-and-repay: CPU consumed inside a boost window is charged as
 *     `borrowed`; when the window closes it becomes `debt`, repaid by
 *     deferring the task's future deadlines.  debt is independent of
 *     dsq_vtime and is NOT lag-clamped (no sleep-laundering).
 *   - rg_prio: map kept for userspace ABI compatibility but IGNORED by
 *     the scheduler (business QoS moved to middleware).
 *
 * Requires: Linux kernel with CONFIG_SCHED_CLASS_EXT (sched_ext).
 *
 * Copyright (c) 2026, Oracle and/or its affiliates.
 */

#include <scx/common.bpf.h>

char _license[] SEC("license") = "GPL";

UEI_DEFINE(uei);

/* Two user-DSQ pools.
 *   EEVDF_DSQ             — global vtime-ordered pool for gated (semantic)
 *                           tasks: COMMIT/ROLLBACK, lock holders, waiters,
 *                           BG-tier.  Only these pay the rbtree + single
 *                           global lock, and only dispatch consumes it.
 *   CPU_DSQ_BASE + cpu    — per-CPU FIFO pool for everything else (the
 *                           vast majority of wakeups).  Per-CPU lock, list
 *                           insert: no cross-CPU contention, no vtime
 *                           math.  dispatch() drains the semantic pool
 *                           first, then this CPU's FIFO pool.
 * The builtin SCX_DSQ_LOCAL is deliberately unused for the fast path:
 * the scheduler core only calls ops.dispatch() when the builtin local
 * DSQ is empty, so populating it would starve the semantic pool. */
#define EEVDF_DSQ 1
#define CPU_DSQ_BASE 0x1000
#define MAX_CPUS 512

static __always_inline u64 cpu_dsq_id(s32 cpu)
{
  return CPU_DSQ_BASE + (u64)cpu;
}

#define TIER_TS     0
#define TIER_NORMAL 1
#define TIER_BG     2

/* Bounded semantic adjustments (all in ns, same unit as dsq_vtime). */
#define B_COMMIT        SCX_SLICE_DFL   /* vd bonus while in COMMIT/ROLLBACK */
#define HOLDER_SLICE    (2 * SCX_SLICE_DFL) /* bounded slice for lock holders */
#define MAX_LAG         (100 * 1000 * 1000ULL) /* 100ms wakeup credit cap */
#define B_MAX           (4 * SCX_SLICE_DFL)  /* per-window borrow cap */
#define D_MAX           (8 * SCX_SLICE_DFL)  /* outstanding debt cap */
#define PI_DEPTH_MAX    8
#define PI_STALE_NS     (1000 * 1000 * 1000ULL) /* 1s wait-edge freshness */

/* Background tasks (purge & friends) get a smaller weight: their virtual
 * deadline lands 4x later and their vruntime accumulates 4x faster, so
 * they receive ~1/4 share under contention and full speed when idle. */
#define BG_WEIGHT       25
#define NORMAL_WEIGHT   100

/* Global minimum-vtime tracker (EEVDF lag reference). */
static u64 vtime_now;

/* Number of per-CPU FIFO DSQs actually created (set by init). */
static u32 nr_cpu_dsqs;

/* -----------------------------------------------------------------------
 * Userspace-published maps (pinned at /sys/fs/bpf/scx_db_sched/).
 * ----------------------------------------------------------------------- */

/* commit_vip[tid] = 1  →  this thread is inside COMMIT / ROLLBACK */
struct {
  __uint(type, BPF_MAP_TYPE_HASH);
  __uint(max_entries, 4096);
  __type(key, u32);
  __type(value, u8);
} commit_vip SEC(".maps");

/* blocked_by[waiter_tid] = holder_tid */
struct {
  __uint(type, BPF_MAP_TYPE_HASH);
  __uint(max_entries, 4096);
  __type(key, u32);
  __type(value, u32);
} blocked_by SEC(".maps");

/* lock_holder[tid] = {count, ts_ns}  →  this tid holds a lock/latch that
 * other threads wait on.  The value is effectively boolean (count is kept
 * for observability only; the scheduler checks count > 0); publishers
 * write {1, now} idempotently and clears delete eagerly.  There is no
 * read-modify-write anywhere: a lost update can only leave a stale entry,
 * which the scheduler stops honouring after HOLDER_TTL_NS and the bridge
 * sweeper garbage-collects a period later. */
struct holder_entry {
  u32 count;
  u64 ts_ns;  /* CLOCK_MONOTONIC ns of the last publish (bridge writes) */
};
#define HOLDER_TTL_NS (1000 * 1000 * 1000ULL) /* 1s staleness bound */
struct {
  __uint(type, BPF_MAP_TYPE_HASH);
  __uint(max_entries, 4096);
  __type(key, u32);
  __type(value, struct holder_entry);
} lock_holder SEC(".maps");

/* Kept for userspace bridge ABI compatibility; ignored by the scheduler. */
struct {
  __uint(type, BPF_MAP_TYPE_HASH);
  __uint(max_entries, 4096);
  __type(key, u32);
  __type(value, s32);
} rg_prio SEC(".maps");

/* task_tier[tid] = tier (TS=0 / NORMAL=1 / BG=2).  Absent = NORMAL. */
struct {
  __uint(type, BPF_MAP_TYPE_HASH);
  __uint(max_entries, 4096);
  __type(key, u32);
  __type(value, s32);
} task_tier SEC(".maps");

/* sem_any[tid] = 1  →  the DB bridge currently has some semantic state
 * published for this tid (tier != NORMAL, commit_vip, lock edges, or a
 * lock_holder count).  Absent  →  pure normal task: the enqueue and
 * stopping fast paths put it on the per-CPU FIFO local DSQ and skip all
 * vtime math, accounts and the global DSQ lock.  The bridge maintains
 * this gate as a refcount over its published entries, so the gate is a
 * conservative superset of the real state: a stale entry only costs the
 * slow path, never correctness. */
struct {
  __uint(type, BPF_MAP_TYPE_HASH);
  __uint(max_entries, 8192);
  __type(key, u32);
  __type(value, u8);
} sem_any SEC(".maps");

/* -----------------------------------------------------------------------
 * Priority-inheritance state.
 * ----------------------------------------------------------------------- */
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
 * Per-task borrow-and-repay account (kernel-internal, not pinned).
 * ----------------------------------------------------------------------- */
struct task_account {
  u64 borrowed;
  u64 debt;
  u64 last_stop_ns;
  u64 slice_ns;   /* slice granted at last insert (for exact runtime) */
};

struct {
  __uint(type, BPF_MAP_TYPE_HASH);
  __uint(max_entries, 8192);
  __type(key, u32);
  __type(value, struct task_account);
} task_account_map SEC(".maps");

/* Per-CPU statistics: [0]=commit bonus, [1]=normal (gated), [2]=holder,
 * [3]=ts, [4]=bg, [5]=fast-path (ungated, per-CPU FIFO) */
struct {
  __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
  __uint(max_entries, 6);
  __type(key, u32);
  __type(value, u64);
} stats SEC(".maps");

static void stat_inc(u32 idx)
{
  u64 *cnt = bpf_map_lookup_elem(&stats, &idx);
  if (cnt)
    __sync_fetch_and_add(cnt, 1);
}

static inline s32 get_tier(u32 tid)
{
  s32 *t = bpf_map_lookup_elem(&task_tier, &tid);
  return t ? *t : TIER_NORMAL;
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

/* Repay debt with idle time: waiting, not running, is what repays. */
static void repay_debt_by_idle(struct task_account *st, u64 now)
{
  u64 idle;

  if (!st->debt || !st->last_stop_ns || now <= st->last_stop_ns)
    return;
  idle = now - st->last_stop_ns;
  st->debt = (st->debt > idle) ? st->debt - idle : 0;
}

/* Close an open borrow window: move borrowed into debt (bounded by D_MAX). */
static void close_borrow_window(struct task_account *st)
{
  if (!st->borrowed)
    return;
  st->debt += st->borrowed;
  if (st->debt > D_MAX)
    st->debt = D_MAX;
  st->borrowed = 0;
}

/* Insert with the effective vd and record the granted slice. */
static void insert_task(struct task_struct *p, u64 slice, u64 vd,
                        u64 enq_flags, u32 tid)
{
  struct task_account *st = account_of(tid);
  if (st)
    st->slice_ns = slice;
  scx_bpf_dsq_insert_vtime(p, EEVDF_DSQ, slice, vd, enq_flags);
}

/* -----------------------------------------------------------------------
 * select_cpu
 *   Cache-affine default placement ONLY.  No direct DSQ insert from here:
 *   that fast path bypasses enqueue and would silently skip the sem_any
 *   gate, vd bonuses, inheritance and accounting (v3 bug).  Ungated
 *   tasks reach their per-CPU FIFO DSQ through enqueue's fast path,
 *   which has already verified they carry no semantic state.
 * ----------------------------------------------------------------------- */
s32 BPF_STRUCT_OPS(db_sched_select_cpu, struct task_struct *p,
                   s32 prev_cpu, u64 wake_flags)
{
  bool is_idle = false;
  return scx_bpf_select_cpu_dfl(p, prev_cpu, wake_flags, &is_idle);
}

/* -----------------------------------------------------------------------
 * enqueue
 * ----------------------------------------------------------------------- */
void BPF_STRUCT_OPS(db_sched_enqueue, struct task_struct *p, u64 enq_flags)
{
  u32 tid = p->pid;
  u64 now = scx_bpf_now();
  u64 vr, vd, slice = SCX_SLICE_DFL;
  s32 tier;
  u32 weight;
  struct task_account *st;
  u8 *vip;
  u8 *gate;

  /* Fast path: no semantic state published for this tid.  Per-CPU FIFO
   * user DSQ of the select_cpu() target — no vtime arithmetic, no
   * account, no rbtree insert under the global DSQ lock.  This is where
   * the overwhelming majority of wakeups (idle workers, kernel threads,
   * non-DB tasks) must land. */
  gate = bpf_map_lookup_elem(&sem_any, &tid);
  if (!gate) {
    stat_inc(5);
    scx_bpf_dsq_insert(p, cpu_dsq_id(scx_bpf_task_cpu(p)),
                       SCX_SLICE_DFL, enq_flags);
    return;
  }

  tier = get_tier(tid);
  weight = (tier == TIER_BG) ? BG_WEIGHT : NORMAL_WEIGHT;

  /* Base vruntime with lag clamp (bounded wakeup credit). */
  vr = p->scx.dsq_vtime;
  if (vr + MAX_LAG < vtime_now)
    vr = vtime_now - MAX_LAG;

  vd = vr + SCX_SLICE_DFL * 100 / weight;

  /* Borrow-and-repay account: repay by idling, then charge remaining debt. */
  st = account_of(tid);
  if (st) {
    repay_debt_by_idle(st, now);
    vd += st->debt;
  }

  /* COMMIT/ROLLBACK: bounded vd bonus (no infinite slice). */
  vip = bpf_map_lookup_elem(&commit_vip, &tid);
  if (vip && *vip) {
    stat_inc(0);
    if (!st || st->borrowed < B_MAX)
      vd = (vd > B_COMMIT) ? vd - B_COMMIT : 0;
    insert_task(p, SCX_SLICE_DFL, vd, enq_flags, tid);
    goto inherit_publish;
  }

  /* Time-sensitive role (reactor, VIP workers).  Head of this CPU's
   * FIFO pool plus a preempt kick: the kick makes the target CPU
   * re-enter dispatch immediately, and the head insert wins over the
   * fast-path FIFO entries (the semantic vtime pool is still drained
   * first — a waiting COMMIT outranks a reactor for one slice, which
   * matches the v4 vd-bonus equivalence of B_TS and B_COMMIT). */
  if (tier == TIER_TS) {
    stat_inc(3);
    scx_bpf_dsq_insert(p, cpu_dsq_id(scx_bpf_task_cpu(p)), SCX_SLICE_DFL,
                       enq_flags | SCX_ENQ_HEAD);
    scx_bpf_kick_cpu(scx_bpf_task_cpu(p), SCX_KICK_PREEMPT);
    goto inherit_publish;
  }

  /* Lock holder: bounded slice + inheritance from most urgent waiter.
   * A stale entry (lost update / leaked clear) is ignored: publishers
   * refresh ts_ns, so anything older than HOLDER_TTL_NS is dead state. */
  {
    struct holder_entry *he = bpf_map_lookup_elem(&lock_holder, &tid);
    if (he && he->count > 0 && now - he->ts_ns <= HOLDER_TTL_NS) {
      struct boost_entry *hb;

      stat_inc(2);
      slice = HOLDER_SLICE;
      if (!st || st->borrowed < B_MAX) {
        hb = bpf_map_lookup_elem(&holder_boost, &tid);
        if (hb) {
          if (now - hb->ts <= PI_STALE_NS && hb->floor < vd)
            vd = hb->floor;
          /* One-shot: delete on use or when stale; waiters re-publish. */
          bpf_map_delete_elem(&holder_boost, &tid);
        }
      }
    }
  }

  if (tier == TIER_BG)
    stat_inc(4);
  else
    stat_inc(1);

  insert_task(p, slice, vd, enq_flags, tid);

inherit_publish:
  /* If this task is itself a waiter, publish its effective vd as a floor
   * for its holder, then propagate up the wait-for chain (depth-capped). */
  {
    u32 *holder = bpf_map_lookup_elem(&blocked_by, &tid);
    u64 floor = vd;
    u32 depth = 0;
    u32 h = holder ? *holder : 0;

    while (h && depth < PI_DEPTH_MAX) {
      struct boost_entry *hb = bpf_map_lookup_elem(&holder_boost, &h);
      if (hb && now - hb->ts <= PI_STALE_NS && hb->floor <= floor)
        break;  /* Fresher-or-equal boost already present. */
      {
        struct boost_entry ne = { .floor = floor, .ts = now, .depth = depth };
        bpf_map_update_elem(&holder_boost, &h, &ne, BPF_ANY);
      }
      {
        u32 *next = bpf_map_lookup_elem(&blocked_by, &h);
        h = next ? *next : 0;
      }
      depth++;
    }
  }
}

/* -----------------------------------------------------------------------
 * dispatch: single fair pool.  BG starvation is bounded by EEVDF lag
 * semantics (a starved task's clamped lag caps its wait).
 * ----------------------------------------------------------------------- */
void BPF_STRUCT_OPS(db_sched_dispatch, s32 cpu, struct task_struct *prev)
{
  u32 i, nr = nr_cpu_dsqs;

  /* Semantic pool first (commit / lock holders / BG): the global
   * vtime-ordered DSQ holds only gated tasks, so its lock is no longer
   * hammered by every wakeup.  Then this CPU's FIFO fast-path pool.
   * enq_flags 0: plain move; the compat macro resolves to
   * scx_bpf_consume() on pre-6.12 kernels. */
  if (scx_bpf_dsq_move_to_local(EEVDF_DSQ, 0))
    return;
  if (scx_bpf_dsq_move_to_local(cpu_dsq_id(cpu), 0))
    return;

  /* Work conservation: this CPU would otherwise go idle while another
   * CPU's FIFO holds fast-path tasks.  Per-CPU user DSQs have no builtin
   * stealing, so scan neighbours round-robin from cpu+1.  Idle path only
   * — under saturation this loop is rarely reached. */
  for (i = 1; i < MAX_CPUS; i++) {
    s32 victim;

    if (i >= nr)
      break;
    victim = (s32)((cpu + i) % nr);
    if (scx_bpf_dsq_move_to_local(cpu_dsq_id(victim), 0))
      return;
  }
}

/* -----------------------------------------------------------------------
 * running / stopping / enable / disable
 * ----------------------------------------------------------------------- */
void BPF_STRUCT_OPS(db_sched_running, struct task_struct *p)
{
  if (time_before(vtime_now, p->scx.dsq_vtime))
    vtime_now = p->scx.dsq_vtime;
}

void BPF_STRUCT_OPS(db_sched_stopping, struct task_struct *p, bool runnable)
{
  u32 tid = p->pid;
  u8 *gate;
  s32 tier;
  u32 weight;
  u64 runtime;
  u8 *vip;
  struct holder_entry *he;
  struct task_account *st;

  gate = bpf_map_lookup_elem(&sem_any, &tid);
  if (!gate) {
    /* Fast path: keep dsq_vtime current at normal weight so a later
     * transition into the semantic pool (this tid gets a gate entry)
     * starts from a sane vruntime instead of a stale one.  No maps are
     * written: the FIFO local pool ignores vtime entirely. */
    if (p->scx.slice <= SCX_SLICE_DFL)
      p->scx.dsq_vtime += SCX_SLICE_DFL - p->scx.slice;
    return;
  }

  tier = get_tier(tid);
  weight = (tier == TIER_BG) ? BG_WEIGHT : NORMAL_WEIGHT;
  st = account_of(tid);

  /* Exact runtime from the slice granted at insert time. */
  if (st && st->slice_ns >= p->scx.slice)
    runtime = st->slice_ns - p->scx.slice;
  else if (p->scx.slice <= SCX_SLICE_DFL)
    runtime = SCX_SLICE_DFL - p->scx.slice;
  else
    runtime = 0;

  p->scx.dsq_vtime += runtime * 100 / weight;

  vip = bpf_map_lookup_elem(&commit_vip, &tid);
  he = bpf_map_lookup_elem(&lock_holder, &tid);
  if ((vip && *vip) ||
      (he && he->count > 0 &&
       scx_bpf_now() - he->ts_ns <= HOLDER_TTL_NS)) {
    if (st) {
      st->borrowed += runtime;
      if (st->borrowed >= B_MAX)
        close_borrow_window(st);
    }
  } else if (st) {
    /* Window closed (lock released / commit done): settle the account. */
    close_borrow_window(st);
    st->last_stop_ns = scx_bpf_now();
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
  /* The gate is refcounted by the bridge; a tid leaving the scheduler
   * may still carry a stale gate entry if the bridge missed a clear
   * (tid reuse), so scrub it here as the final authority. */
  bpf_map_delete_elem(&sem_any, &tid);
}

/* -----------------------------------------------------------------------
 * init / exit
 * ----------------------------------------------------------------------- */
s32 BPF_STRUCT_OPS_SLEEPABLE(db_sched_init)
{
  s32 cpu, ret, nr;

  ret = scx_bpf_create_dsq(EEVDF_DSQ, -1);
  if (ret)
    return ret;

  /* Per-CPU FIFO pools for the ungated fast path.  The "per-CPU" aspect
   * is purely conventional (id = CPU_DSQ_BASE + cpu); the node argument
   * is only a NUMA allocation hint and must be NUMA_NO_NODE — passing the
   * cpu index would be rejected with -EINVAL once cpu >= nr_node_ids. */
  nr = scx_bpf_nr_cpu_ids();
  if (nr > MAX_CPUS)
    nr = MAX_CPUS;
  for (cpu = 0; cpu < nr; cpu++) {
    ret = scx_bpf_create_dsq(CPU_DSQ_BASE + cpu, -1);
    if (ret)
      return ret;
  }
  nr_cpu_dsqs = nr;

  bpf_printk("EEVDF-Sem: registered (semantic vtime DSQ=%d + %d per-CPU FIFO DSQs)",
             EEVDF_DSQ, nr);
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
