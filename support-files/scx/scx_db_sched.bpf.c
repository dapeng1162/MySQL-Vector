/* SPDX-License-Identifier: GPL-2.0 */
/*
 * scx_db_sched v2: native-first, full-system sched_ext scheduler.
 *
 * See Docs/smart-sched-design.md.  This scheduler is attached only for a
 * GUARDED window; the default database path is the kernel's own EEVDF.
 *
 * Base policy:
 *   - per-CPU vtime DSQs with weight-scaled runtime accounting;
 *   - canonical base_vruntime is kept in task_account and restored before
 *     each run, so temporary semantic bonuses never pollute long-term
 *     fairness;
 *   - EEVDF-style vlag accounting: the lag (unfairness suffered while
 *     runnable, i.e. cv - base) is frozen when a task sleeps and re-applied
 *     at wake (base = cv_now - vlag), so sleep duration is forgiven while
 *     runnable-time entitlement is honored; scx_lag_carry_ns bounds vlag;
 *   - deadline-ordered wakeup preemption via SCX_KICK_PREEMPT, admitted
 *     against detected CPU hogs (streak classifier) or victims past the
 *     wakeup granularity, mirroring how EEVDF protects interactive
 *     workloads from CPU hogs;
 *   - semantic boosts are repaid as bounded debt;
 *   - wakeup kick is on by default (ablation: --no-kick).
 */

#include <scx/common.bpf.h>

char _license[] SEC("license") = "GPL";

UEI_DEFINE(uei);

#define CPU_DSQ_BASE 0x1000
#define MAX_CPUS 512

#define B_COMMIT_NS (1000 * 1000ULL)
#define B_HOLD_NS (500 * 1000ULL)
#define B_MAX_NS (4 * 1000 * 1000ULL)
#define D_MAX_NS (8 * 1000 * 1000ULL)
#define SEM_TTL_NS (5 * 1000 * 1000 * 1000ULL)

const volatile bool scx_enable_semantic = false;
/* Wakeup preemption: on SCX_ENQ_WAKEUP, kick the target CPU when the
   waker's virtual deadline is earlier than the running task's. This is
   the EEVDF wakeup-preemption equivalent; without it a 6ms hog slice
   gates every foreground wakeup (N1 p99 7.2ms, N2 p99 123ms on
   2026-11-13). Ablation: --no-kick. */
const volatile bool scx_enable_kick = true;
/* Kick admission gate: a wakeup kick may preempt the running task only
   when the victim is a detected CPU hog or has already consumed half of
   its granted slice. Ungated kicks fire on every eligible wakeup — under
   clean OLTP load the victims are mysqld workers and the preemption
   churn costs throughput (lag50 ungated r1 measured ~-7% vs lag0 on
   2026-11-17). Hogs are still admitted immediately, which is what the
   N1 noise scenario needs. Ablation: --no-kick-gate. */
const volatile bool scx_kick_gate = true;
/* True EEVDF vlag semantics (D7): freeze the runnable-time lag at sleep,
   forgive the sleep duration at wake (base = cv - vlag). The legacy
   approximation (--no-vlag) instead froze the sleeper's vruntime for the
   whole sleep and clamped it lag_carry behind the CPU floor — under OLTP
   wake storms that turned the accumulated sleep time into phantom credit,
   so vruntime stopped tracking delivered service and the vtime ordering
   degenerated toward insertion order (the D5/D6-era -5% clean-load and
   3x mixed-tail deficit). */
const volatile bool scx_enable_vlag = true;
/* Wakeup-kick admission granularity: a non-hog victim becomes preemptible
   after min(slice/2, gran) of runtime. The old pure half-slice rule waited
   up to 10ms under the 20ms SCX_SLICE_DFL slice (rw t32, the shallowest
   queue, measured -7.2% vs V97); 1ms aligns with native EEVDF's short
   natural preemption cadence. 0 restores pure half-slice. */
const volatile u64 scx_wakeup_gran_ns = 1000 * 1000ULL;
/* Consecutive fully-burned slices (>= 90% of the granted slice) before a
   task is classified a CPU hog (slice cap + immediate kick admission).
   Replaces the old cpu_budget accumulator, which misclassified OLTP
   workers whose in-transaction microbursts summed past the budget
   (~5M cpubound_cap events with only 8 true hogs in the N2 matrix).
   0 disables hog classification. */
const volatile u32 scx_hog_streak_n = 2;
/* Upper bound on the frozen vlag applied at wake, in virtual time.
   50ms comfortably covers real runnable-time unfairness while bounding
   placement against per-CPU cv floor skew. */
const volatile u64 scx_lag_carry_ns = 50000 * 1000ULL;
/* 6ms: measured on 16-core c8i (2026-11-13) — 2.5ms shredded busy
   connection threads in closed-loop OLTP (-3.6% rw64), while 20ms (cap
   disabled) recovered only the same amount; the remaining gap is the
   base policy itself, not the cap.  6ms keeps true CPU hogs bounded
   for the mixed-workload scenario without hurting foreground OLTP. */
const volatile u64 scx_hog_slice_ns = 6000 * 1000ULL;

struct sem_state {
  u32 flags;         /* 1=commit, 2=holder */
  u32 holder_count;
  u64 ts_ns;
};

struct wait_edge_key {
  u32 waiter_tid;
  u32 seq;
};

struct wait_edge_value {
  u32 holder_tid;
  u32 kind;
  u64 ts_ns;
};

struct task_account {
  u64 base_vruntime;
  u64 start_ns;
  u64 debt;
  u64 granted_ns;
  s64 vlag_v;
  u32 hog_streak;
  bool boost_pending;
  bool boosted;
  bool vlag_valid;
};

struct {
  __uint(type, BPF_MAP_TYPE_HASH);
  __uint(max_entries, 8192);
  __type(key, u32);
  __type(value, struct sem_state);
} task_sem SEC(".maps");

struct {
  __uint(type, BPF_MAP_TYPE_HASH);
  __uint(max_entries, 8192);
  __type(key, struct wait_edge_key);
  __type(value, struct wait_edge_value);
} wait_edges SEC(".maps");

struct {
  __uint(type, BPF_MAP_TYPE_HASH);
  __uint(max_entries, 8192);
  __type(key, u32);
  __type(value, struct task_account);
} task_account_map SEC(".maps");

struct {
  __uint(type, BPF_MAP_TYPE_ARRAY);
  __uint(max_entries, MAX_CPUS);
  __type(key, u32);
  __type(value, u64);
} vtime_cpu SEC(".maps");

/* Order key of the task currently running on each CPU (0 = none). The
   wakeup path compares against it to decide preemption. */
struct {
  __uint(type, BPF_MAP_TYPE_ARRAY);
  __uint(max_entries, MAX_CPUS);
  __type(key, u32);
  __type(value, u64);
} cpu_cur_order SEC(".maps");

/* Admission state for the wakeup kick, snapshotted in running(): the
   wall-clock point at which the running task has consumed half of its
   granted slice (0 = no victim) and whether it entered the run as a
   detected CPU hog. */
struct cpu_victim {
  u64 half_ns;
  u64 hog;
};

struct {
  __uint(type, BPF_MAP_TYPE_ARRAY);
  __uint(max_entries, MAX_CPUS);
  __type(key, u32);
  __type(value, struct cpu_victim);
} cpu_cur_victim SEC(".maps");

struct {
  __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
  __uint(max_entries, 16);
  __type(key, u32);
  __type(value, u64);
} stats SEC(".maps");

static u32 nr_cpu_dsqs;

static void stat_inc(u32 idx) {
  u64 *cnt = bpf_map_lookup_elem(&stats, &idx);
  if (cnt) __sync_fetch_and_add(cnt, 1);
}

static struct task_account *account_of(u32 tid) {
  struct task_account *st = bpf_map_lookup_elem(&task_account_map, &tid);
  if (!st) {
    struct task_account init = {};
    bpf_map_update_elem(&task_account_map, &tid, &init, BPF_NOEXIST);
    st = bpf_map_lookup_elem(&task_account_map, &tid);
  }
  return st;
}

static u64 cpu_vtime(s32 cpu) {
  u64 *v;
  u32 key = (u32)cpu;
  if (cpu < 0 || cpu >= MAX_CPUS) return 0;
  v = bpf_map_lookup_elem(&vtime_cpu, &key);
  return v ? *v : 0;
}

static void cpu_vtime_advance(s32 cpu, u64 val) {
  u64 *v;
  u32 key = (u32)cpu;
  if (cpu < 0 || cpu >= MAX_CPUS) return;
  v = bpf_map_lookup_elem(&vtime_cpu, &key);
  if (!v) return;
  if (time_before(*v, val)) *v = val;
}

static u64 cur_order_of(s32 cpu) {
  u64 *v;
  u32 key = (u32)cpu;
  if (cpu < 0 || cpu >= MAX_CPUS) return 0;
  v = bpf_map_lookup_elem(&cpu_cur_order, &key);
  return v ? *v : 0;
}

static void cur_order_set(s32 cpu, u64 val) {
  u64 *v;
  u32 key = (u32)cpu;
  if (cpu < 0 || cpu >= MAX_CPUS) return;
  v = bpf_map_lookup_elem(&cpu_cur_order, &key);
  if (v) *v = val;
}

static struct cpu_victim *victim_of(s32 cpu) {
  u32 key = (u32)cpu;
  if (cpu < 0 || cpu >= MAX_CPUS) return NULL;
  return bpf_map_lookup_elem(&cpu_cur_victim, &key);
}

static void victim_set(s32 cpu, u64 hog, u64 half_ns) {
  struct cpu_victim *v = victim_of(cpu);
  if (v) {
    v->hog = hog;
    v->half_ns = half_ns;
  }
}

static void victim_clear(s32 cpu) {
  struct cpu_victim *v = victim_of(cpu);
  if (v) {
    v->hog = 0;
    v->half_ns = 0;
  }
}

/* Canonical order key used for wakeup-preemption comparisons: the
   virtual deadline of the next slice at the SAME slice scale the task
   will actually be enqueued with (slice_ns is the fast_slice in effect).
   Transient semantic bonuses are deliberately excluded — preemption
   must not be driven by them. */
static u64 order_key(const struct task_account *st, u32 weight, u64 slice_ns) {
  u64 slice_v = slice_ns * 100 / (weight ? weight : 100);
  return st->base_vruntime + slice_v + st->debt;
}

static u32 weight_of(struct task_struct *p) {
  u32 w = p->scx.weight;
  return w == 0 ? 100 : w;
}

static bool is_hog(const struct task_account *st) {
  return scx_hog_streak_n && st->hog_streak >= scx_hog_streak_n;
}

static __always_inline u64 fast_slice(s32 cpu) {
  s32 nr = scx_bpf_dsq_nr_queued(CPU_DSQ_BASE + (u64)cpu);
  if (nr < 0) nr = 0;
  if (nr >= 8) return SCX_SLICE_DFL >> 3;
  if (nr >= 4) return SCX_SLICE_DFL >> 2;
  if (nr >= 2) return SCX_SLICE_DFL >> 1;
  return SCX_SLICE_DFL;
}

static u64 semantic_bonus(u32 tid, u64 now) {
  struct sem_state *sem;
  u64 bonus = 0;

  if (!scx_enable_semantic) return 0;
  sem = bpf_map_lookup_elem(&task_sem, &tid);
  if (!sem || now - sem->ts_ns > SEM_TTL_NS) return 0;

  stat_inc(1);
  if (sem->flags & 1) {
    bonus += B_COMMIT_NS;
    stat_inc(2);
  }
  if ((sem->flags & 2) && sem->holder_count > 0) {
    bonus += B_HOLD_NS;
    stat_inc(3);
  }
  return bonus;
}

s32 BPF_STRUCT_OPS(db_sched_select_cpu, struct task_struct *p, s32 prev_cpu,
                   u64 wake_flags) {
  bool is_idle = false;
  s32 cpu = scx_bpf_select_cpu_dfl(p, prev_cpu, wake_flags, &is_idle);
  u64 slice;
  u32 tid = p->pid;
  struct task_account *st;

  if (!is_idle) return cpu;

  slice = fast_slice(cpu);
  st = account_of(tid);
  if (st) {
    if (is_hog(st) && slice > scx_hog_slice_ns) {
      slice = scx_hog_slice_ns;
      stat_inc(6);
    }
    if (semantic_bonus(tid, scx_bpf_now()) > 0) st->boost_pending = true;
  }
  scx_bpf_dsq_insert(p, SCX_DSQ_LOCAL, slice, 0);
  stat_inc(8);
  return cpu;
}

void BPF_STRUCT_OPS(db_sched_enqueue, struct task_struct *p, u64 enq_flags) {
  u32 tid = p->pid;
  s32 cpu = scx_bpf_task_cpu(p);
  u64 base = p->scx.dsq_vtime;
  u64 cv = cpu_vtime(cpu);
  u64 slice = fast_slice(cpu);
  u64 weight = weight_of(p);
  u64 order;
  u64 bonus;
  struct task_account *st = account_of(tid);

  stat_inc(0);

  /* The canonical vruntime lives in task_account (maintained by
     running/stopping).  p->scx.dsq_vtime is only the transport that
     carries the last temporary order key into the vtime DSQ, so it must
     never feed back into base — otherwise a re-enqueue without an
     intervening run would make a semantic bonus permanent. */
  if (st && st->base_vruntime != 0) base = st->base_vruntime;

  if (st && (enq_flags & SCX_ENQ_WAKEUP)) {
    if (scx_enable_vlag && st->vlag_valid) {
      /* EEVDF wakeup placement: forgive the sleep, keep only the lag
         frozen when the task went to sleep (unfairness suffered while
         runnable). Freezing the whole sleep as phantom credit (the
         legacy clamp) let vruntime drift arbitrarily far behind cv, so
         ordering stopped tracking delivered service. */
      s64 lag = st->vlag_v;
      st->vlag_valid = false;
      if (lag >= 0)
        base = (u64)lag < cv ? cv - (u64)lag : 0;
      else
        base = cv + (u64)(-lag);
    } else if (!scx_enable_vlag) {
      /* Legacy --no-vlag semantics: the sleeper keeps its frozen base,
         bounded lag_carry behind the CPU floor. */
      if (base + scx_lag_carry_ns < cv) base = cv - scx_lag_carry_ns;
    }
  }

  if (st) {
    st->base_vruntime = base;
    if (is_hog(st) && slice > scx_hog_slice_ns) {
      slice = scx_hog_slice_ns;
      stat_inc(6);
    }
  }

  /* Wakeup preemption (EEVDF-style): if this is a real wakeup and the
     woken task's virtual deadline beats what is running on the target
     CPU, kick that CPU now. Re-enqueues from preemption or slice expiry
     do not carry SCX_ENQ_WAKEUP and never kick, which bounds storms. */
  if (scx_enable_kick && st && (enq_flags & SCX_ENQ_WAKEUP)) {
    u64 cur = cur_order_of(cpu);
    if (cur != 0 && order_key(st, weight_of(p), slice) < cur) {
      const struct cpu_victim *v = victim_of(cpu);
      bool admit = true;
      if (scx_kick_gate) {
        u64 now = scx_bpf_now();
        admit = v && (v->hog || (v->half_ns && now > v->half_ns));
      }
      if (admit) {
        scx_bpf_kick_cpu(cpu, SCX_KICK_PREEMPT);
        stat_inc(9);
      } else {
        stat_inc(10);
      }
    }
  }

  bonus = semantic_bonus(tid, scx_bpf_now());
  if (st) {
    st->boost_pending = bonus > 0;
  }

  order = base + slice * 100 / weight;
  if (order > bonus) order -= bonus;
  else order = 0;
  if (st) order += st->debt;

  scx_bpf_dsq_insert_vtime(p, CPU_DSQ_BASE + (u64)cpu, slice, order,
                           enq_flags);
}

void BPF_STRUCT_OPS(db_sched_dispatch, s32 cpu, struct task_struct *prev) {
  u32 i, nr = nr_cpu_dsqs;

  /* Single-argument form: the running kernel's kfunc (7.0.0-38) is
     scx_bpf_dsq_move_to_local(u64 dsq_id); the enq_flags variant exists
     only in newer trees' compat headers, and we never pass flags here. */
  if (scx_bpf_dsq_move_to_local(CPU_DSQ_BASE + (u64)cpu)) return;

  for (i = 1; i < MAX_CPUS; i++) {
    s32 victim;
    if (i >= nr) break;
    victim = (s32)((cpu + i) % nr);
    if (scx_bpf_dsq_move_to_local(CPU_DSQ_BASE + (u64)victim)) {
      stat_inc(7);
      return;
    }
  }
}

void BPF_STRUCT_OPS(db_sched_running, struct task_struct *p) {
  u32 tid = p->pid;
  s32 cpu = scx_bpf_task_cpu(p);
  u64 now = scx_bpf_now();
  struct task_account *st = account_of(tid);

  if (!st) return;
  p->scx.dsq_vtime = st->base_vruntime;
  cpu_vtime_advance(cpu, st->base_vruntime);
  cur_order_set(cpu, order_key(st, weight_of(p), p->scx.slice));
  st->start_ns = now;
  st->granted_ns = p->scx.slice;
  if (st->boost_pending) {
    st->boosted = true;
    st->boost_pending = false;
    stat_inc(4);
  }
  {
    u64 gate_ns = p->scx.slice / 2;
    if (scx_wakeup_gran_ns && gate_ns > scx_wakeup_gran_ns)
      gate_ns = scx_wakeup_gran_ns;
    victim_set(cpu, is_hog(st), now + gate_ns);
  }
}

void BPF_STRUCT_OPS(db_sched_stopping, struct task_struct *p, bool runnable) {
  u32 tid = p->pid;
  u64 now = scx_bpf_now();
  u32 weight = weight_of(p);
  struct task_account *st = bpf_map_lookup_elem(&task_account_map, &tid);

  if (!st) return;
  if (!st->start_ns || now <= st->start_ns) return;

  u64 rt = now - st->start_ns;
  st->base_vruntime += rt * 100 / weight;

  /* Advance the CPU vtime floor with executed service: cv must track the
     maximum vruntime actually reached, like EEVDF's avg_vruntime advances
     with load. Advancing only at running() (to the base at start) let cv
     freeze at its historical max once wakeups place every sleeper below
     it — ordering then degenerated to insertion order and deadline
     comparisons became noise (23M gated kicks with no selectivity in the
     2026-11-17 D7 rw run). */
  cpu_vtime_advance(scx_bpf_task_cpu(p), st->base_vruntime);

  if (st->boosted) {
    st->debt += rt;
    if (st->debt > D_MAX_NS) st->debt = D_MAX_NS;
    st->boosted = false;
  } else if (st->debt) {
    u64 repay = rt < st->debt ? rt : st->debt;
    st->base_vruntime += repay * 100 / weight;
    st->debt -= repay;
    stat_inc(5);
  }

  /* Streak hog classifier: a run that burned >= 90% of its granted slice
     extends the streak, any short run resets it. Unlike the old
     cpu_budget accumulator this cannot sum up OLTP in-transaction
     microbursts into a hog verdict. */
  if (st->granted_ns && rt >= st->granted_ns * 9 / 10)
    st->hog_streak++;
  else
    st->hog_streak = 0;

  /* Freeze the EEVDF vlag when the task sleeps: the credit kept is the
     unfairness suffered while runnable (cv - base), never the sleep
     duration itself. Applied at the next SCX_ENQ_WAKEUP enqueue. */
  if (!runnable) {
    if (scx_enable_vlag) {
      s64 cv = (s64)cpu_vtime(scx_bpf_task_cpu(p));
      s64 lag = cv - (s64)st->base_vruntime;
      if (lag > (s64)scx_lag_carry_ns) lag = (s64)scx_lag_carry_ns;
      if (lag < -(s64)scx_lag_carry_ns) lag = -(s64)scx_lag_carry_ns;
      st->vlag_v = lag;
      st->vlag_valid = true;
    } else {
      st->vlag_valid = false;
    }
  }

  st->start_ns = 0;
  p->scx.dsq_vtime = st->base_vruntime;
  cur_order_set(scx_bpf_task_cpu(p), 0);
  victim_clear(scx_bpf_task_cpu(p));
}

void BPF_STRUCT_OPS(db_sched_enable, struct task_struct *p) {
  u32 tid = p->pid;
  s32 cpu = scx_bpf_task_cpu(p);
  struct task_account *st = account_of(tid);

  if (st) {
    st->base_vruntime = cpu_vtime(cpu);
    p->scx.dsq_vtime = st->base_vruntime;
  } else {
    p->scx.dsq_vtime = cpu_vtime(cpu);
  }
}

void BPF_STRUCT_OPS(db_sched_disable, struct task_struct *p) {
  u32 tid = p->pid;
  bpf_map_delete_elem(&task_account_map, &tid);
}

s32 BPF_STRUCT_OPS_SLEEPABLE(db_sched_init) {
  s32 cpu, ret, nr;

  nr = scx_bpf_nr_cpu_ids();
  if (nr > MAX_CPUS) nr = MAX_CPUS;
  for (cpu = 0; cpu < nr; cpu++) {
    ret = scx_bpf_create_dsq(CPU_DSQ_BASE + cpu, -1);
    if (ret) return ret;
  }
  nr_cpu_dsqs = nr;
  bpf_printk("scx_db_sched v2: %d per-CPU vtime DSQs, semantic=%d "
             "slice_dfl=%lluus",
             nr, scx_enable_semantic,
             (unsigned long long)SCX_SLICE_DFL / 1000);
  return 0;
}

void BPF_STRUCT_OPS(db_sched_exit, struct scx_exit_info *ei) {
  UEI_RECORD(uei, ei);
}

SCX_OPS_DEFINE(db_sched_ops,
               .select_cpu = (void *)db_sched_select_cpu,
               .enqueue = (void *)db_sched_enqueue,
               .dispatch = (void *)db_sched_dispatch,
               .running = (void *)db_sched_running,
               .stopping = (void *)db_sched_stopping,
               .enable = (void *)db_sched_enable,
               .disable = (void *)db_sched_disable,
               .init = (void *)db_sched_init,
               .exit = (void *)db_sched_exit,
               .name = "db_sched");
