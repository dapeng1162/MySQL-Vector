/* SPDX-License-Identifier: GPL-2.0 */
/*
 * DB-aware sched_ext BPF scheduler for MySQL / InnoDB.
 *
 * Purpose:
 *   - commit_vip:  threads performing COMMIT / ROLLBACK are dispatched to a
 *                   dedicated VIP DSQ (strict FIFO, higher priority than all
 *                   normal tasks), reducing transaction latency.
 *   - lock_holder: threads that hold a row lock waited on by others receive
 *                   SCX_SLICE_INF (cannot be preempted) PLUS a vtime boost
 *                   so they drain quickly → anti-LHP (Lock Holder Priority).
 *   - blocked_by:  waiter→holder relationship used for priority inheritance.
 *                   When a waiter with a high priority (rg_prio) is blocked on
 *                   a holder, the holder inherits the waiter's urgency.
 *   - rg_prio:     per-thread resource-group priority (nice-like, lower =
 *                   higher priority).  Defaults to 0 for all tasks.
 *
 * Requires: Linux kernel with CONFIG_SCHED_CLASS_EXT (sched_ext).
 *           See https://github.com/sched-ext/sched_ext.
 *
 * Copyright (c) 2026, Oracle and/or its affiliates.
 */

#include <scx/common.bpf.h>

char _license[] SEC("license") = "GPL";

/* -----------------------------------------------------------------------
 * User-exit info (for clean shutdown / error reporting to userspace)
 * ----------------------------------------------------------------------- */
UEI_DEFINE(uei);

/* -----------------------------------------------------------------------
 * Custom DSQ IDs
 * -----------------------------------------------------------------------
 *
 * SCX_DSQ_GLOBAL   (built-in) – fallback, not used directly.
 * COMMIT_VIP_DSQ   (custom)   – FIFO queue for commit/rollback tasks.
 * SHARED_DSQ       (custom)   – vtime-sorted queue for all normal tasks.
 *
 * dispatch() first tries to consume from COMMIT_VIP_DSQ; only if empty
 * does it consume from SHARED_DSQ.  This provides strict priority of
 * commit-vip tasks over normal tasks.
 */
#define COMMIT_VIP_DSQ  0
#define SHARED_DSQ      1

/* -----------------------------------------------------------------------
 * BPF maps (pinned at /sys/fs/bpf/scx_db_sched/ on the host).
 *
 * These are the four maps that the userspace bridge
 * (sql/sched_ext_bridge.cc) writes to.
 * ----------------------------------------------------------------------- */

/* commit_vip[tid] = 1  →  this thread is inside COMMIT / ROLLBACK */
struct {
  __uint(type, BPF_MAP_TYPE_HASH);
  __uint(max_entries, 4096);
  __type(key, u32);
  __type(value, u8);
} commit_vip SEC(".maps");

/* blocked_by[waiter_tid] = holder_tid  →  waiter is blocked on holder */
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

/* rg_prio[tid] = nice  →  resource-group priority (nice-like, lower = higher) */
struct {
  __uint(type, BPF_MAP_TYPE_HASH);
  __uint(max_entries, 4096);
  __type(key, u32);
  __type(value, s32);
} rg_prio SEC(".maps");

/* -----------------------------------------------------------------------
 * Internal helper map for priority inheritance.
 *
 * holder_boost[holder_tid] = vtime_floor
 *   – The minimum dsq_vtime the holder should be scheduled at, inherited
 *     from the most urgent waiter.  Updated in enqueue when the waiter is
 *     enqueued; cleared when the holder itself is enqueued (done once).
 *   – This implements true priority inheritance: the holder is scheduled
 *     no later than its most urgent blocked waiter.
 * ----------------------------------------------------------------------- */
struct {
  __uint(type, BPF_MAP_TYPE_HASH);
  __uint(max_entries, 4096);
  __type(key, u32);
  __type(value, u64);
} holder_boost SEC(".maps");

/* -----------------------------------------------------------------------
 * Per-CPU statistics (for introspection / debug).
 * ----------------------------------------------------------------------- */
struct {
  __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
  __uint(max_entries, 3); /* [0]=vip, [1]=normal, [2]=holder */
  __type(key, u32);
  __type(value, u64);
} stats SEC(".maps");

static void stat_inc(u32 idx)
{
  u64 *cnt = bpf_map_lookup_elem(&stats, &idx);
  if (cnt)
    __sync_fetch_and_add(cnt, 1);
}

/* -----------------------------------------------------------------------
 * Helper: read rg_prio for a task, defaulting to 0 (normal priority).
 * ----------------------------------------------------------------------- */
static inline s32 get_prio(u32 tid)
{
  s32 *p = bpf_map_lookup_elem(&rg_prio, &tid);
  return p ? *p : 0;
}

/* -----------------------------------------------------------------------
 * select_cpu
 *   Return the previous CPU to preserve cache affinity.  If the CPU is
 *   idle, dispatch directly to the local DSQ (bypass the global DSQ).
 * ----------------------------------------------------------------------- */
s32 BPF_STRUCT_OPS(db_sched_select_cpu, struct task_struct *p,
                   s32 prev_cpu, u64 wake_flags)
{
  bool is_idle = false;
  s32 cpu;

  cpu = scx_bpf_select_cpu_dfl(p, prev_cpu, wake_flags, &is_idle);
  if (is_idle) {
    /* Idle CPU → enqueue to local DSQ for immediate execution. */
    scx_bpf_dsq_insert(p, SCX_DSQ_LOCAL, SCX_SLICE_DFL, 0);
  }

  return cpu;
}

/* -----------------------------------------------------------------------
 * enqueue
 *   The main scheduling decision point.
 *
 *   Priority order (checked in sequence):
 *     1. commit_vip  →  COMMIT_VIP_DSQ (FIFO, SCX_SLICE_MAX)
 *     2. lock_holder > 0  →  SHARED_DSQ (vtime, SCX_SLICE_MAX, boosted)
 *     3. normal task  →  SHARED_DSQ (vtime, rg_prio-weighted)
 *
 *   Priority inheritance (blocked_by):
 *     If the task being enqueued is a waiter (i.e. it is blocked on a
 *     holder), we record the waiter's dsq_vtime as a floor for the holder.
 *     When the holder is later enqueued, its dsq_vtime is clamped to the
 *     minimum of its own and the most-urgent waiter's, so the holder runs
 *     at least as soon as the waiter that needs it.
 * ----------------------------------------------------------------------- */
void BPF_STRUCT_OPS(db_sched_enqueue, struct task_struct *p, u64 enq_flags)
{
  u32 tid = p->pid;
  u64 vtime;
  u64 slice;
  s32 prio;

  skip_other:
  /* ── 1. commit_vip ──────────────────────────────────────────────────── */
  {
    u8 *vip = bpf_map_lookup_elem(&commit_vip, &tid);
    if (vip && *vip) {
      stat_inc(0);
      /* VIP tasks are dispatched to the separate FIFO DSQ with max slice.
       * They are not subject to vtime accounting. */
      scx_bpf_dsq_insert(p, COMMIT_VIP_DSQ, SCX_SLICE_INF, enq_flags);
      return;
    }
  }

  /* ── Priority inheritance: if this task is a waiter, boost its holder ─ */
  {
    u32 *holder_tid = bpf_map_lookup_elem(&blocked_by, &tid);
    if (holder_tid) {
      /* The waiter's vtime is the earliest time the holder should run.
       * Record this as a floor for the holder.  If the holder hasn't been
       * boosted yet, or the waiter's vtime is lower than the current
       * boost, update. */
      u64 *current_boost = bpf_map_lookup_elem(&holder_boost, holder_tid);
      u64 waiter_vtime = p->scx.dsq_vtime;

      if (!current_boost || waiter_vtime < *current_boost) {
        bpf_map_update_elem(&holder_boost, holder_tid, &waiter_vtime,
                            BPF_ANY);
      }
    }
  }

  /* ── 2. lock_holder > 0 (anti-LHP) or normal task ──────────────────── */

  vtime = p->scx.dsq_vtime;
  slice = SCX_SLICE_DFL;

  u32 *wcnt = bpf_map_lookup_elem(&lock_holder, &tid);
  if (wcnt && *wcnt > 0) {
    stat_inc(2);
    /* Lock holder: grant max slice so it cannot be preempted, and boost
     * its vtime (subtract a fixed quantum) so it runs sooner.  The
     * combination of SCX_SLICE_MAX + vtime boost is the classic anti-LHP
     * measure. */
    slice = SCX_SLICE_INF;

    /* Apply a vtime boost equal to 2 full slices – this ensures the
     * holder is scheduled ahead of normal tasks of the same priority. */
    if (vtime >= SCX_SLICE_DFL * 2)
      vtime -= SCX_SLICE_DFL * 2;
    else
      vtime = 0;
  }

  /* ── 3. Apply priority inheritance boost for holder ────────────────── */
  {
    u64 *inherit = bpf_map_lookup_elem(&holder_boost, &tid);
    if (inherit && *inherit < vtime) {
      /* The holder inherits its waiters' urgency: clamp vtime to the
       * minimum of its own and the most-urgent waiter's. */
      vtime = *inherit;
      /* Clear the boost to avoid permanent accumulation (the waiter
       * will re-set it if still blocked). */
      bpf_map_delete_elem(&holder_boost, &tid);
    }
  }

  /* ── 4. Apply rg_prio ──────────────────────────────────────────────── */
  prio = get_prio(tid);
  if (prio < 0) {
    /* Higher priority (nice < 0): schedule sooner by reducing vtime.
     * The multiplier is 1 ms per nice level. */
    u64 bonus = (-prio) * (SCX_SLICE_DFL / 2);
    if (vtime >= bonus)
      vtime -= bonus;
    else
      vtime = 0;
  } else if (prio > 0) {
    /* Lower priority (nice > 0): reduce slice. */
    u64 reduction = slice * (u64)prio / 20;
    if (reduction > 0) {
      if (slice > reduction)
        slice -= reduction;
      else
        slice = SCX_SLICE_DFL / 4; /* minimum slice floor */
    }
  }

  /* ── 5. Dispatch to SHARED_DSQ with vtime ───────────────────────────── */
  stat_inc(1);
  scx_bpf_dsq_insert_vtime(p, SHARED_DSQ, slice, vtime, enq_flags);
}

/* -----------------------------------------------------------------------
 * dispatch
 *   Called when a CPU needs a new task.  Priority: consume from the VIP
 *   DSQ first; only if it is empty, consume from the shared DSQ.
 *   scx_bpf_dsq_move_to_local() returns true if a task was moved.
 * ----------------------------------------------------------------------- */
void BPF_STRUCT_OPS(db_sched_dispatch, s32 cpu, struct task_struct *prev)
{
  if (scx_bpf_dsq_move_to_local(COMMIT_VIP_DSQ, 0))
    return;
  scx_bpf_dsq_move_to_local(SHARED_DSQ, 0);
}

/* -----------------------------------------------------------------------
 * running
 *   Advance global vtime (used by SHARED_DSQ weight-based scheduling).
 * ----------------------------------------------------------------------- */
static u64 vtime_now;

void BPF_STRUCT_OPS(db_sched_running, struct task_struct *p)
{
  if (time_before(vtime_now, p->scx.dsq_vtime))
    vtime_now = p->scx.dsq_vtime;
}

/* -----------------------------------------------------------------------
 * stopping
 *   Charge the task's execution time against its dsq_vtime.
 * ----------------------------------------------------------------------- */
void BPF_STRUCT_OPS(db_sched_stopping, struct task_struct *p, bool runnable)
{
  s32 prio = get_prio(p->pid);
  /*
   * Scale by the inverse of weight.  The default weight is 100
   * (SCX_SLICE_DFL is 20ms).  Higher priority tasks accumulate vtime
   * more slowly, so they are scheduled more often.
   *
   * weight = 100 - prio * 5  (clamped to [10, 200])
   *   prio < 0  → weight > 100  → slower vtime accumulation
   *   prio > 0  → weight < 100  → faster vtime accumulation
   */
  u64 weight = 100;
  if (prio < 0)
    weight = 100 + (-prio) * 5;
  else if (prio > 0)
    weight = (prio * 5 < 90) ? 100 - prio * 5 : 10;

  p->scx.dsq_vtime += (SCX_SLICE_DFL - p->scx.slice) * 100 / weight;
}

/* -----------------------------------------------------------------------
 * enable
 *   Initialize per-task vtime when a task first enters the scheduler.
 * ----------------------------------------------------------------------- */
void BPF_STRUCT_OPS(db_sched_enable, struct task_struct *p)
{
  p->scx.dsq_vtime = vtime_now;
}

/* -----------------------------------------------------------------------
 * init / exit
 * ----------------------------------------------------------------------- */
s32 BPF_STRUCT_OPS_SLEEPABLE(db_sched_init)
{
  s32 ret;

  ret = scx_bpf_create_dsq(COMMIT_VIP_DSQ, -1);
  if (ret)
    return ret;

  ret = scx_bpf_create_dsq(SHARED_DSQ, -1);
  if (ret)
    return ret;

  bpf_printk("scx_db_sched: registered (COMMIT_VIP_DSQ=%d, "
             "SHARED_DSQ=%d)", COMMIT_VIP_DSQ, SHARED_DSQ);
  return 0;
}

void BPF_STRUCT_OPS(db_sched_exit, struct scx_exit_info *ei)
{
  UEI_RECORD(uei, ei);
}

/* -----------------------------------------------------------------------
 * Ops definition
 * ----------------------------------------------------------------------- */
SCX_OPS_DEFINE(db_sched_ops,
               .select_cpu    = (void *)db_sched_select_cpu,
               .enqueue       = (void *)db_sched_enqueue,
               .dispatch      = (void *)db_sched_dispatch,
               .running       = (void *)db_sched_running,
               .stopping      = (void *)db_sched_stopping,
               .enable        = (void *)db_sched_enable,
               .init          = (void *)db_sched_init,
               .exit          = (void *)db_sched_exit,
               .name          = "db_sched");