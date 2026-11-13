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
 *   - semantic boosts are repaid as bounded debt;
 *   - no wakeup kick by default.
 */

#include <scx/common.bpf.h>

char _license[] SEC("license") = "GPL";

UEI_DEFINE(uei);

#define CPU_DSQ_BASE 0x1000
#define MAX_CPUS 512
#define MAX_LAG (100 * 1000 * 1000ULL)
#define CPUBOUND_BUDGET_NS (20 * 1000 * 1000ULL)

#define B_COMMIT_NS (1000 * 1000ULL)
#define B_HOLD_NS (500 * 1000ULL)
#define B_MAX_NS (4 * 1000 * 1000ULL)
#define D_MAX_NS (8 * 1000 * 1000ULL)
#define SEM_TTL_NS (5 * 1000 * 1000 * 1000ULL)

const volatile bool scx_enable_semantic = false;
const volatile u64 scx_hog_slice_ns = 2500 * 1000ULL;

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
  u64 cpu_budget;
  u64 last_stop_ns;
  u64 debt;
  bool boost_pending;
  bool boosted;
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

static u32 weight_of(struct task_struct *p) {
  u32 w = p->scx.weight;
  return w == 0 ? 100 : w;
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
    if (st->cpu_budget > CPUBOUND_BUDGET_NS && slice > scx_hog_slice_ns) {
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

  if (base + MAX_LAG < cv) base = cv - MAX_LAG;

  if (st) {
    st->base_vruntime = base;
    if (st->cpu_budget > CPUBOUND_BUDGET_NS && slice > scx_hog_slice_ns) {
      slice = scx_hog_slice_ns;
      stat_inc(6);
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

  if (scx_bpf_dsq_move_to_local(CPU_DSQ_BASE + (u64)cpu, 0)) return;

  for (i = 1; i < MAX_CPUS; i++) {
    s32 victim;
    if (i >= nr) break;
    victim = (s32)((cpu + i) % nr);
    if (scx_bpf_dsq_move_to_local(CPU_DSQ_BASE + (u64)victim, 0)) {
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
  st->start_ns = now;
  if (st->boost_pending) {
    st->boosted = true;
    st->boost_pending = false;
    stat_inc(4);
  }
  if (st->last_stop_ns && now > st->last_stop_ns) {
    u64 gap = now - st->last_stop_ns;
    st->cpu_budget = st->cpu_budget > gap ? st->cpu_budget - gap : 0;
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

  st->cpu_budget += rt;
  st->last_stop_ns = now;
  st->start_ns = 0;
  p->scx.dsq_vtime = st->base_vruntime;
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
  bpf_printk("scx_db_sched v2: %d per-CPU vtime DSQs, semantic=%d", nr,
             scx_enable_semantic);
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
