/* SPDX-License-Identifier: GPL-2.0 */
/*
 * Userspace loader for scx_db_sched v2.
 *
 * The scheduler is intended for a GUARDED window: attach explicitly, run the
 * window, then exit to restore the kernel's native EEVDF scheduler.
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

#include <bpf/bpf.h>
#include <scx/common.h>

#include "scx_db_sched.bpf.skel.h"

static const char help_fmt[] =
"A native-first DB-aware sched_ext scheduler for MySQL (v2).\n"
"\n"
"Usage: %s [OPTIONS]\n"
"\n"
"  --semantic         Enable commit/holder semantic boosts (default off).\n"
"  --hog-slice-us=N   Slice cap for CPU-bound tasks (default 2500).\n"
"  --stats            Print aggregated counters every 10s.\n"
"  --dump             Dump the DB-aware maps after attaching.\n"
"  -v                 Print libbpf debug messages.\n"
"  -h                 Display this help and exit.\n";

static bool verbose;
static bool dump_maps;
static bool print_stats;
static volatile int exit_req = 0;

#define SCX_MAP_DIR "/sys/fs/bpf/scx_db_sched"
#define SCX_MAP_PIN_PREFIX SCX_MAP_DIR "/"

static void sigint_handler(int signo) { exit_req = 1; }

static int libbpf_print_fn(enum libbpf_print_level level, const char *format,
                           va_list args) {
  if (level == LIBBPF_DEBUG && !verbose) return 0;
  return vfprintf(stderr, format, args);
}

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

static void dump_task_sem(struct bpf_map *map) {
  int fd = bpf_map__fd(map);
  u32 key = 0, next = 0;
  struct {
    u32 flags;
    u32 holder_count;
    u64 ts_ns;
  } value;

  printf("task_sem:\n");
  while (bpf_map_get_next_key(fd, &key, &next) == 0) {
    key = next;
    if (bpf_map_lookup_elem(fd, &key, &value) == 0)
      printf("  tid=%u flags=%u holders=%u ts=%llu\n", key, value.flags,
             value.holder_count,
             (unsigned long long)value.ts_ns);
  }
}

static void dump_wait_edges(struct bpf_map *map) {
  int fd = bpf_map__fd(map);
  struct {
    u32 waiter_tid;
    u32 seq;
  } key = {}, next = {};
  struct {
    u32 holder_tid;
    u32 kind;
    u64 ts_ns;
  } value;

  printf("wait_edges:\n");
  while (bpf_map_get_next_key(fd, &key, &next) == 0) {
    key = next;
    if (bpf_map_lookup_elem(fd, &key, &value) == 0)
      printf("  waiter=%u seq=%u holder=%u kind=%u ts=%llu\n",
             key.waiter_tid, key.seq, value.holder_tid, value.kind,
             (unsigned long long)value.ts_ns);
  }
}

static int pin_maps(struct scx_db_sched *skel) {
  char path[256];

  if (mkdir(SCX_MAP_DIR, 0755) != 0 && errno != EEXIST) {
    fprintf(stderr, "Failed to create %s: %s\n", SCX_MAP_DIR, strerror(errno));
    return -1;
  }

  snprintf(path, sizeof(path), SCX_MAP_PIN_PREFIX "task_sem");
  unlink(path);
  if (bpf_map__pin(skel->maps.task_sem, path) != 0) {
    fprintf(stderr, "Failed to pin task_sem: %s\n", strerror(errno));
    return -1;
  }

  snprintf(path, sizeof(path), SCX_MAP_PIN_PREFIX "wait_edges");
  unlink(path);
  if (bpf_map__pin(skel->maps.wait_edges, path) != 0) {
    fprintf(stderr, "Failed to pin wait_edges: %s\n", strerror(errno));
    return -1;
  }

  fprintf(stderr, "scx_db_sched: maps pinned under %s\n", SCX_MAP_DIR);
  return 0;
}

static const char *const stat_names[] = {
    "inserts",       "semantic_hits", "commit_boost",  "holder_boost",
    "boosted_runs",  "debt_repay",    "cpubound_cap",  "dispatch_steals",
    "direct_idle",   NULL,
};

static void print_stats_once(struct scx_db_sched *skel) {
  int fd = bpf_map__fd(skel->maps.stats);
  int nr_cpus = libbpf_num_possible_cpus();
  u32 key = 0;
  u64 *values;
  int i, slot;

  if (nr_cpus < 0) nr_cpus = 1;
  values = calloc(nr_cpus, sizeof(u64));
  if (!values) return;

  printf("=== scx_db_sched stats ===\n");
  for (slot = 0; stat_names[slot]; slot++) {
    u64 sum = 0;
    key = (u32)slot;
    if (bpf_map_lookup_elem(fd, &key, values) != 0) continue;
    for (i = 0; i < nr_cpus; i++) sum += values[i];
    printf("  %-17s %llu\n", stat_names[slot], (unsigned long long)sum);
  }
  printf("=== end stats ===\n");
  fflush(stdout);
  free(values);
}

int main(int argc, char **argv) {
  struct scx_db_sched *skel;
  struct bpf_link *link;
  bool opt_semantic = false;
  long opt_hog_slice_us = 0;
  static const struct option long_options[] = {
      {"dump", no_argument, NULL, 'd'},
      {"stats", no_argument, NULL, 's'},
      {"verbose", no_argument, NULL, 'v'},
      {"help", no_argument, NULL, 'h'},
      {"semantic", no_argument, NULL, 'S'},
      {"hog-slice-us", required_argument, NULL, 'H'},
      {NULL, 0, NULL, 0},
  };
  int opt;
  u64 ecode;

  libbpf_set_print(libbpf_print_fn);
  signal(SIGINT, sigint_handler);
  signal(SIGTERM, sigint_handler);

  if (check_sched_ext_available() != 0) return 1;

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
      case 'S':
        opt_semantic = true;
        break;
      case 'H':
        opt_hog_slice_us = atol(optarg);
        if (opt_hog_slice_us <= 0) {
          fprintf(stderr, "--hog-slice-us must be positive\n");
          return 1;
        }
        break;
      default:
        fprintf(stderr, help_fmt, basename(argv[0]));
        return opt != 'h';
    }
  }

  if (opt_semantic) skel->rodata->scx_enable_semantic = true;
  if (opt_hog_slice_us > 0)
    skel->rodata->scx_hog_slice_ns = (u64)opt_hog_slice_us * 1000ULL;

  fprintf(stderr,
          "scx_db_sched v2: semantic=%s hog_slice=%lluus\n",
          opt_semantic ? "on" : "off",
          (unsigned long long)skel->rodata->scx_hog_slice_ns / 1000);

#ifdef SCX_SKEL_HAS_STRUCT_OPS
  SCX_OPS_LOAD(skel, db_sched_ops, scx_db_sched, uei);
#else
  skel->rodata->uei_dump_len = UEI_DUMP_DFL_LEN;
  RESIZE_ARRAY(skel, data, uei_dump, UEI_DUMP_DFL_LEN);
  SCX_BUG_ON(scx_db_sched__load(skel), "Failed to load skel");
#endif

  if (pin_maps(skel) != 0) {
    fprintf(stderr, "scx_db_sched: map pinning failed, continuing anyway\n");
  }

  link = SCX_OPS_ATTACH(skel, db_sched_ops, scx_db_sched);
  fprintf(stderr, "scx_db_sched: attached as the system scheduler.\n");

  if (dump_maps) {
    dump_task_sem(skel->maps.task_sem);
    dump_wait_edges(skel->maps.wait_edges);
  }

  while (!exit_req && !UEI_EXITED(skel, uei)) {
    if (print_stats) {
      sleep(10);
      print_stats_once(skel);
    } else if (dump_maps) {
      sleep(5);
      dump_task_sem(skel->maps.task_sem);
      dump_wait_edges(skel->maps.wait_edges);
    } else {
      sleep(1);
    }
  }

  bpf_link__destroy(link);
  ecode = UEI_REPORT(skel, uei);
  scx_db_sched__destroy(skel);
  return 0;
}
