/* SPDX-License-Identifier: GPL-2.0 */
/*
 * scx_db_sched v7-rc1 (R0): minimal EEVDF base for MySQL / InnoDB.
 *
 * Design (see Docs/sched-v7-redesign.md).  v6.4 attribution experiments
 * (E1-E4 + C5n, rw256, redo 4G) showed the semantic machinery — global
 * vtime DSQ, unconditional commit/urgent kicks, vtime tilts, CPU-bound
 * cap — accounts for only ~3% of the -21.8% gap to CFS, the mysqld-side
 * publication for ~3%, and the scx base path itself for -16.7%.  v7
 * therefore starts from a minimal, faithful EEVDF base (R0) and re-adds
 * mechanisms one per stage behind runtime switches, each with its own
 * acceptance gate (R1: budgeted wakeup preemption, R2: commit tilt,
 * R3: latch tilt + PI).
 *
 * R0 scheduler shape:
 *   - One queue family: per-CPU vtime DSQs.  No global DSQ.  Every task,
 *     gated or not, takes the same single path: clamp vruntime against a
 *     global lag bound, adaptive slice, insert into the select_cpu()
 *     target's pool.
 *   - The DB bridge maps (sem_any &c.) stay pinned for ABI compatibility
 *     so the mysqld instrumentation cost is present in every
 *     measurement, but R0 only counts gate hits and ignores them — R2/R3
 *     re-attach meaning behind switches.
 *   - Adaptive slice (20ms -> 2.5ms by pool depth) plus a CPU-bound
 *     detector whose cap bounds the head-of-line wait a hog can impose
 *     when preemption is delayed (E1: removing it worsened the N1 tail).
 *   - Exact runtime accounting from the running()->stopping() on-CPU
 *     interval, immune to kick slice-zeroing (v6.2/v6.3 lesson).
 *   - R1 wakeup preemption (off by default): CFS-granularity lag filter
 *     plus a per-target-CPU token bucket bounding kick IPIs per second.
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

/* -----------------------------------------------------------------------
 * Runtime switches (loader-settable, same binary A/B).
 *   R1: wakeup preemption with granularity filter + per-CPU kick budget.
 *   R2/R3 knobs arrive with their stages; sem gate is counted, ignored.
 * ----------------------------------------------------------------------- */
const volatile bool scx_enable_wake_preempt = false; /* R1 */
const volatile u64 scx_wake_gran_ns = 1000 * 1000ULL; /* CFS-order granularity */
const volatile u32 scx_kick_budget_per_sec = 2048;    /* IPI/s per target CPU */
const volatile u64 scx_hog_slice_ns = 2500 * 1000ULL; /* 2.5ms default */

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

/* -----------------------------------------------------------------------
 * Per-task account: exact runtime charge + CPU-bound token bucket.
 * ----------------------------------------------------------------------- */
struct task_account {
  u64 slice_ns;      /* slice granted at last insert (fallback charge) */
  u64 start_ns;      /* on-CPU start timestamp, set in running() */
  u64 cpu_budget;    /* undecayed on-CPU credit (CPU-bound detector) */
  u64 last_stop_ns;  /* wall time of the last stop (decay reference) */
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
 *   [0] inserts, [1] sem gate hits (ignored), [2] cpu-bound cap applied,
 *   [3] kicks sent, [4] kicks below granularity, [5] kicks over budget,
 *   [6] dispatch steals. */
struct {
  __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
  __uint(max_entries, 8);
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

/* Adaptive slice, shrinking with the per-CPU pool depth: 20ms when the
 * pool is empty or shallow, halving at depth 2/4/8 down to 2.5ms —
 * CFS's saturation granularity.  A CPU-bound task can hold the CPU for
 * at most one adaptive slice before the pool's next task (possibly a
 * just-woken one at the vtime front) is dispatched, while an unloaded
 * machine keeps full slices and near-zero switch overhead. */
static __always_inline u64 fast_slice(s32 cpu)
{
  s32 nr = scx_bpf_dsq_nr_queued(CPU_DSQ_BASE + (u64)cpu);
  if (nr < 0)
    nr = 0;
  if (nr >= 8)
    return SCX_SLICE_DFL >> 3;
  if (nr >= 4)
    return SCX_SLICE_DFL >> 2;
  if (nr >= 2)
    return SCX_SLICE_DFL >> 1;
  return SCX_SLICE_DFL;
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

  stat_inc(0);

  /* One semantic lookup per wakeup (the R2/R3 hook point).  Counted,
   * ignored in R0. */
  if (bpf_map_lookup_elem(&sem_any, &tid))
    stat_inc(1);

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

    p->scx.dsq_vtime += rt;
    /* CPU-bound detector: accrue on-CPU time here; the off-CPU gap is
     * decayed in running(), so a continuously-running hog's budget
     * grows to the cap while a sleeper's stays near zero. */
    st->cpu_budget += rt;
    st->last_stop_ns = now;
    st->start_ns = 0;
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
