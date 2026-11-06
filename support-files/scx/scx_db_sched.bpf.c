/* SPDX-License-Identifier: GPL-2.0 */
/*
 * EEVDF-Sem: DB-aware sched_ext BPF scheduler for MySQL / InnoDB (v4).
 *
 * Design (see Docs/coroutine-sched-resilience-design.md):
 *   - Single fair pool: every task is scheduled by a virtual deadline
 *       vd = vruntime + slice * 100 / weight + debt - bonus
 *     ordered in one vtime DSQ (EEVDF "earliest eligible" idea).  No
 *     strict-priority FIFO bypass, no SCX_SLICE_INF.
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

/* Single fair-pool DSQ.  All tasks (commits, TS, normal, BG) are ordered
 * by their effective virtual deadline in this one queue. */
#define EEVDF_DSQ 1

#define TIER_TS     0
#define TIER_NORMAL 1
#define TIER_BG     2

/* Bounded semantic adjustments (all in ns, same unit as dsq_vtime). */
#define B_COMMIT        SCX_SLICE_DFL   /* vd bonus while in COMMIT/ROLLBACK */
#define B_TS            SCX_SLICE_DFL   /* vd bonus for reactor / VIP workers */
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

/* lock_holder[tid] = N  →  N threads are waiting for this tid */
struct {
  __uint(type, BPF_MAP_TYPE_HASH);
  __uint(max_entries, 4096);
  __type(key, u32);
  __type(value, u32);
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

/* Per-CPU statistics: [0]=commit bonus, [1]=normal, [2]=holder, [3]=ts, [4]=bg */
struct {
  __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
  __uint(max_entries, 5);
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
 *   Cache-affine default placement ONLY.  No direct SCX_DSQ_LOCAL insert:
 *   that fast path bypasses enqueue and would silently skip vd bonuses,
 *   inheritance and accounting (v3 bug).  Everything goes through enqueue.
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
  s32 tier = get_tier(tid);
  u32 weight = (tier == TIER_BG) ? BG_WEIGHT : NORMAL_WEIGHT;
  struct task_account *st;
  u8 *vip;
  u32 *wcnt;

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

  /* Time-sensitive role (reactor, VIP workers): bonus + preempt kick. */
  if (tier == TIER_TS) {
    stat_inc(3);
    vd = (vd > B_TS) ? vd - B_TS : 0;
    insert_task(p, SCX_SLICE_DFL, vd, enq_flags, tid);
    scx_bpf_kick_cpu(scx_bpf_task_cpu(p), SCX_KICK_PREEMPT);
    goto inherit_publish;
  }

  /* Lock holder: bounded slice + inheritance from most urgent waiter. */
  wcnt = bpf_map_lookup_elem(&lock_holder, &tid);
  if (wcnt && *wcnt > 0) {
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
  scx_bpf_dsq_move_to_local(EEVDF_DSQ);
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
  s32 tier = get_tier(tid);
  u32 weight = (tier == TIER_BG) ? BG_WEIGHT : NORMAL_WEIGHT;
  u64 runtime;
  u8 *vip;
  u32 *wcnt;
  struct task_account *st = account_of(tid);

  /* Exact runtime from the slice granted at insert time. */
  if (st && st->slice_ns >= p->scx.slice)
    runtime = st->slice_ns - p->scx.slice;
  else if (p->scx.slice <= SCX_SLICE_DFL)
    runtime = SCX_SLICE_DFL - p->scx.slice;
  else
    runtime = 0;

  p->scx.dsq_vtime += runtime * 100 / weight;

  vip = bpf_map_lookup_elem(&commit_vip, &tid);
  wcnt = bpf_map_lookup_elem(&lock_holder, &tid);
  if ((vip && *vip) || (wcnt && *wcnt > 0)) {
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
}

/* -----------------------------------------------------------------------
 * init / exit
 * ----------------------------------------------------------------------- */
s32 BPF_STRUCT_OPS_SLEEPABLE(db_sched_init)
{
  s32 ret = scx_bpf_create_dsq(EEVDF_DSQ, -1);
  if (ret)
    return ret;

  bpf_printk("EEVDF-Sem: registered (single fair pool, DSQ=%d)", EEVDF_DSQ);
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
