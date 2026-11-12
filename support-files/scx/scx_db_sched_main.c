/* SPDX-License-Identifier: GPL-2.0 */
/*
 * Userspace loader for the DB-aware sched_ext scheduler (scx_db_sched).
 *
 * Loads scx_db_sched.bpf.o (built from scx_db_sched.bpf.c), attaches it
 * as the system scheduler, and optionally dumps the contents of the
 * DB-aware maps (commit_vip, blocked_by, lock_holder, rg_prio, task_tier).
 *
 * Requires:
 *   - Linux kernel with CONFIG_SCHED_CLASS_EXT (sched_ext), checked at
 *     load time by verifying /sys/kernel/sched_ext exists.
 *   - libbpf.
 *
 * Usage:
 *   scx_db_sched [--dump]
 *
 * Copyright (c) 2026, Oracle and/or its affiliates.
 */

#include <errno.h>
#include <getopt.h>
#include <libgen.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

/*
 * Skeleton layout compat: bpftool >= 7.4 emits struct_ops maps as a
 * dedicated skel->struct_ops.<name> section pointing at the ops value,
 * which the SCX_OPS_OPEN()/SCX_OPS_LOAD() macros expect.  Older
 * bpftool emits struct_ops maps as plain maps, and the ops struct is
 * not reachable in a layout-portable way.  On that path we open/load
 * the skeleton directly: ops->hotplug_seq stays 0 (the kernel
 * documents "if 0, no detection occurs") and the UEI dump keeps its
 * default size.  SCX_SKEL_HAS_STRUCT_OPS is defined by the CMake
 * bpftool version probe.
 */
#include <bpf/bpf.h>
#include <scx/common.h>

#include "scx_db_sched.bpf.skel.h"

static const char help_fmt[] =
"A DB-aware sched_ext scheduler for MySQL / InnoDB (v7-rc1, R0 base).\n"
"\n"
"Usage: %s [OPTIONS]\n"
"\n"
"  --r1-kick          Enable R1 wakeup preemption (lag granularity filter\n"
"                     + per-CPU kick budget).  Off by default (R0).\n"
"  --wake-gran-us=N   Wakeup-preemption lag granularity in microseconds\n"
"                     (default 1000, CFS order).\n"
"  --kick-budget=N    Kick IPI budget per target CPU per second when R1 is\n"
"                     enabled (default 2048).\n"
"  --stats            Periodically print aggregated scheduler counters\n"
"                     (every 10s) while attached.\n"
"  --dump             After attaching, dump the current contents of the\n"
"                     DB-aware maps (commit_vip, blocked_by, lock_holder,\n"
"                     rg_prio) and then keep running.\n"
"  -v                 Print libbpf debug messages\n"
"  -h                 Display this help and exit\n";

static bool verbose;
static bool dump_maps;
static bool print_stats;
static volatile int exit_req = 0;

/*
 * Where the DB-aware maps are pinned so that a running mysqld (via
 * sql/sched_ext_bridge.cc) can open them with bpf_obj_get().  All maps
 * share a single directory under /sys/fs/bpf.
 */
#define SCX_MAP_DIR "/sys/fs/bpf/scx_db_sched"
#define SCX_MAP_PIN_PREFIX SCX_MAP_DIR "/"

static void sigint_handler(int signo) { exit_req = 1; }

static int libbpf_print_fn(enum libbpf_print_level level, const char *format,
                           va_list args) {
  if (level == LIBBPF_DEBUG && !verbose) return 0;
  return vfprintf(stderr, format, args);
}

/*
 * Check that sched_ext is available on this system.  Without
 * CONFIG_SCHED_CLASS_EXT there is no /sys/kernel/sched_ext and the
 * scheduler cannot be loaded.
 */
static int check_sched_ext_available(void) {
  struct stat st;

  if (stat("/sys/kernel/sched_ext", &st) != 0) {
    fprintf(stderr,
            "ERROR: sched_ext is not available on this system.\n"
            "  /sys/kernel/sched_ext does not exist.\n"
            "  This scheduler requires a kernel built with "
            "CONFIG_SCHED_CLASS_EXT (Linux >= 6.12).\n");
    return -1;
  }
  return 0;
}

/*
 * Dump the four DB-aware maps.  Each map is a HASH keyed by tid (u32).
 */
static void dump_map_entries(const char *name, struct bpf_map *map,
                             int value_size, const char *value_fmt) {
  int fd = bpf_map__fd(map);
  __u32 next_key, key = 0;
  int ret;
  /* Large enough for the widest value (lock_holder's holder_entry). */
  char value[16] = {0};

  printf("%s:\n", name);
  while ((ret = bpf_map_get_next_key(fd, &key, &next_key)) == 0) {
    __u32 k = next_key;
    if (bpf_map_lookup_elem(fd, &k, value) != 0) {
      printf("  tid=%u (lookup failed: %s)\n", k, strerror(errno));
    } else {
      if (value_size == 1)
        printf("  tid=%u value=%u\n", k, *(u8 *)value);
      else if (value_size == 4)
        printf("  tid=%u value=%d\n", k, *(int *)value);
      else if (value_size == 12) /* lock_holder: {count, ts_ns} */
        printf("  tid=%u count=%u ts_ns=%llu\n", k, *(u32 *)value,
               *(unsigned long long *)(value + 4));
      else
        printf("  tid=%u value=%s\n", k, value_fmt);
    }
    key = next_key;
  }
  if (ret < 0 && errno != ENOENT)
    printf("  (iteration error: %s)\n", strerror(errno));
}

/*
 * Pin the DB-aware maps so that the running mysqld can open them
 * via bpf_obj_get() from sql/sched_ext_bridge.cc.
 *
 * The maps are pinned under /sys/fs/bpf/scx_db_sched/.
 * Existing pins are removed first to avoid stale entries.
 */
static int pin_maps(struct scx_db_sched *skel) {
  struct {
    const char *name;
    struct bpf_map *map;
  } maps[] = {
      {"commit_vip", skel->maps.commit_vip},
      {"blocked_by", skel->maps.blocked_by},
      {"lock_holder", skel->maps.lock_holder},
      {"rg_prio", skel->maps.rg_prio},
      {"task_tier", skel->maps.task_tier},
      {"sem_any", skel->maps.sem_any},
  };
  size_t i;
  char path[256];

  /* Create the directory.  On a fresh start it may already exist from a
   * previous instance; ignore the EEXIST error. */
  if (mkdir(SCX_MAP_DIR, 0755) != 0 && errno != EEXIST) {
    fprintf(stderr, "Failed to create %s: %s\n", SCX_MAP_DIR,
            strerror(errno));
    return -1;
  }

  for (i = 0; i < sizeof(maps) / sizeof(maps[0]); i++) {
    int ret;

    snprintf(path, sizeof(path), SCX_MAP_PIN_PREFIX "%s", maps[i].name);
    /* Remove any stale pin first. */
    unlink(path);
    ret = bpf_map__pin(maps[i].map, path);
    if (ret != 0) {
      fprintf(stderr, "Failed to pin %s -> %s: %s\n", maps[i].name, path,
              strerror(errno));
      return ret;
    }
  }
  fprintf(stderr, "scx_db_sched: maps pinned under %s\n", SCX_MAP_DIR);
  return 0;
}

static void dump_maps_all(struct scx_db_sched *skel) {
  printf("=== scx_db_sched map dump ===\n");
  dump_map_entries("commit_vip", skel->maps.commit_vip, 1, "u8");
  dump_map_entries("blocked_by", skel->maps.blocked_by, 4, "u32");
  dump_map_entries("lock_holder", skel->maps.lock_holder, 12, "");
  dump_map_entries("rg_prio", skel->maps.rg_prio, 4, "i32");
  dump_map_entries("task_tier", skel->maps.task_tier, 4, "i32");
  printf("=== end dump ===\n");
  fflush(stdout);
}

/*
 * Aggregate the per-CPU scheduler counters across all CPUs and print
 * them with the counter names from scx_db_sched.bpf.c.  The stats map
 * is a BPF_MAP_TYPE_PERCPU_ARRAY of u64 with 8 slots, so one lookup
 * returns one u64 per possible CPU.
 */
static const char *const stat_names[] = {
    "inserts",        "sem_gate_hits", "cpubound_cap",
    "kicks_sent",     "kick_gran_skip", "kick_budget_skip",
    "dispatch_steals", NULL,
};

static void print_stats_once(struct scx_db_sched *skel) {
  int fd = bpf_map__fd(skel->maps.stats);
  int nr_cpus = libbpf_num_possible_cpus();
  __u32 key = 0;
  __u64 *values;
  int i, slot;

  if (nr_cpus < 0)
    nr_cpus = 1;
  values = calloc(nr_cpus, sizeof(__u64));
  if (!values)
    return;
  printf("=== scx_db_sched stats ===\n");
  for (slot = 0; stat_names[slot] != NULL; slot++) {
    __u64 sum = 0;

    key = (__u32)slot;
    if (bpf_map_lookup_elem(fd, &key, values) != 0) {
      printf("  %-17s (lookup failed: %s)\n", stat_names[slot],
             strerror(errno));
      continue;
    }
    for (i = 0; i < nr_cpus; i++)
      sum += values[i];
    printf("  %-17s %llu\n", stat_names[slot],
           (unsigned long long)sum);
  }
  printf("=== end stats ===\n");
  fflush(stdout);
  free(values);
}

int main(int argc, char **argv) {
  struct scx_db_sched *skel;
  struct bpf_link *link;
  bool opt_r1_kick = false;
  long opt_wake_gran_us = 0;
  long opt_kick_budget = 0;
  static const struct option long_options[] = {
      {"dump", no_argument, NULL, 'd'},
      {"stats", no_argument, NULL, 's'},
      {"verbose", no_argument, NULL, 'v'},
      {"help", no_argument, NULL, 'h'},
      {"r1-kick", no_argument, NULL, 'K'},
      {"wake-gran-us", required_argument, NULL, 'g'},
      {"kick-budget", required_argument, NULL, 'b'},
      {NULL, 0, NULL, 0},
  };
  int opt;
  __u64 ecode;

  libbpf_set_print(libbpf_print_fn);
  signal(SIGINT, sigint_handler);
  signal(SIGTERM, sigint_handler);

  if (check_sched_ext_available() != 0) return 1;

restart:
#ifdef SCX_SKEL_HAS_STRUCT_OPS
  skel = SCX_OPS_OPEN(db_sched_ops, scx_db_sched);
#else
  skel = scx_db_sched__open();
  SCX_BUG_ON(!skel, "Could not open scx_db_sched");
#endif

  while ((opt = getopt_long(argc, argv, "dsvh", long_options, NULL)) != -1) {
    switch (opt) {
      case 'd':
        dump_maps = true;
        break;
      case 's':
        print_stats = true;
        break;
      case 'v':
        verbose = true;
        break;
      case 'K':
        opt_r1_kick = true;
        break;
      case 'g':
        opt_wake_gran_us = atol(optarg);
        if (opt_wake_gran_us <= 0) {
          fprintf(stderr, "--wake-gran-us must be positive\n");
          return 1;
        }
        break;
      case 'b':
        opt_kick_budget = atol(optarg);
        if (opt_kick_budget <= 0) {
          fprintf(stderr, "--kick-budget must be positive\n");
          return 1;
        }
        break;
      default:
        fprintf(stderr, help_fmt, basename(argv[0]));
        return opt != 'h';
    }
  }

  /* Runtime switches must be applied after open() and before load(). */
  if (opt_r1_kick)
    skel->rodata->scx_enable_wake_preempt = true;
  if (opt_wake_gran_us > 0)
    skel->rodata->scx_wake_gran_ns = (__u64)opt_wake_gran_us * 1000ULL;
  if (opt_kick_budget > 0)
    skel->rodata->scx_kick_budget_per_sec = (__u32)opt_kick_budget;

  fprintf(stderr,
          "scx_db_sched v7-rc1 (R0): wake_preempt=%s gran=%lluus budget=%u/s\n",
          opt_r1_kick ? "on" : "off",
          (unsigned long long)skel->rodata->scx_wake_gran_ns / 1000,
          skel->rodata->scx_kick_budget_per_sec);

#ifdef SCX_SKEL_HAS_STRUCT_OPS
  SCX_OPS_LOAD(skel, db_sched_ops, scx_db_sched, uei);
#else
  /* UEI_SET_SIZE() reaches ops->exit_dump_len through the struct_ops
   * skeleton section; on the old layout it is 0 anyway, so apply the
   * default dump size directly. */
  skel->rodata->uei_dump_len = UEI_DUMP_DFL_LEN;
  RESIZE_ARRAY(skel, data, uei_dump, UEI_DUMP_DFL_LEN);
  SCX_BUG_ON(scx_db_sched__load(skel), "Failed to load skel");
#endif

  if (pin_maps(skel) != 0) {
    fprintf(stderr, "scx_db_sched: map pinning failed, continuing anyway\n");
  }

  link = SCX_OPS_ATTACH(skel, db_sched_ops, scx_db_sched);

  fprintf(stderr, "scx_db_sched: attached as the system scheduler.\n");

  if (dump_maps) dump_maps_all(skel);

  while (!exit_req && !UEI_EXITED(skel, uei)) {
    if (print_stats) {
      sleep(10);
      print_stats_once(skel);
    } else if (dump_maps) {
      /* Print a periodic dump every 5s so the user can observe changes. */
      sleep(5);
      dump_maps_all(skel);
    } else {
      sleep(1);
    }
  }

  bpf_link__destroy(link);
  ecode = UEI_REPORT(skel, uei);
  scx_db_sched__destroy(skel);

  if (UEI_ECODE_RESTART(ecode)) goto restart;
  return 0;
}
