/* SPDX-License-Identifier: GPL-2.0 */
/*
 * Userspace loader for scx_db_sched v2.
 *
 * The scheduler is intended for a GUARDED window: attach explicitly, run the
 * window, then exit to restore the kernel's native EEVDF scheduler.
 */

#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <libgen.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include <bpf/bpf.h>
#include <scx/common.h>

#include "scx_db_sched.bpf.skel.h"

static const char help_fmt[] =
"A native-first DB-aware sched_ext scheduler for MySQL (v2).\n"
"\n"
"Usage: %s [OPTIONS]\n"
"\n"
"  --semantic         Enable commit/holder semantic boosts (default off).\n"
"  --hog-slice-us=N   Slice cap for CPU-bound tasks (default 6000).\n"
"  --lag-carry-us=N   Max vtime credit kept across a sleep (default 50000).\n"
"  --no-vlag          Legacy lag semantics: freeze vruntime across sleep\n"
"                     with the lag-carry clamp instead of EEVDF vlag.\n"
"  --wakeup-gran-us=N Wakeup-kick admission granularity vs non-hog\n"
"                     victims: min(slice/2, N). 0 = pure half-slice\n"
"                     (default 1000).\n"
"  --hog-streak=N     Consecutive full slices before hog classification\n"
"                     (default 2, 0 disables).\n"
"  --no-kick          Disable deadline-ordered wakeup preemption.\n"
"  --no-kick-gate     Admit every eligible wakeup kick (skip the\n"
"                     hog/half-slice victim gate).\n"
"  --watchdog-cmd=CMD End-to-end liveness probe run via /bin/sh (e.g.\n"
"                     \"mysql --socket=... -u root -e 'SELECT 1'\").\n"
"  --watchdog-interval-sec=N   Probe every N seconds (watchdog on when a\n"
"                     probe command is also given).\n"
"  --watchdog-timeout-sec=N    Kill a probe after N seconds (default 5).\n"
"  --watchdog-max-fail=N       Detach after N consecutive failures (default 2).\n"
"  --stats            Print aggregated counters every 10s.\n"
"  --dump             Dump the DB-aware maps after attaching.\n"
"  -v                 Print libbpf debug messages.\n"
"  -h                 Display this help and exit.\n"
"\n"
"On watchdog trigger the scheduler detaches (kernel native EEVDF is\n"
"restored for all tasks) and the loader exits with status 3.\n";

static bool verbose;
static bool dump_maps;
static bool print_stats;
static volatile int exit_req = 0;

#define SCX_MAP_DIR "/sys/fs/bpf/scx_db_sched"
#define SCX_MAP_PIN_PREFIX SCX_MAP_DIR "/"

static void sigint_handler(int signo) { exit_req = 1; }

/* ------------------------------------------------------------------ */
/* Detach watchdog.                                                    */
/*                                                                     */
/* The GUARDED contract is: if scheduling quality collapses under the  */
/* BPF scheduler, fall back to native EEVDF. Scheduler crashes already  */
/* self-detach (UEI_EXITED tears down the link). The watchdog covers    */
/* the silent wedge case (e.g. the lost-wakeup stall seen at 300k qps: */
/* loader alive, counters frozen, connections stuck) with an            */
/* end-to-end liveness probe the operator supplies: if it fails or      */
/* hangs for K consecutive probes, the main loop destroys the link.    */
/* ------------------------------------------------------------------ */

static const char *wd_cmd;
static int wd_interval_sec;
static int wd_timeout_sec = 5;
static int wd_max_fail = 2;
static volatile int watchdog_detach_req = 0;

/* Run wd_cmd through /bin/sh; return 0 iff it exits 0 within the
   timeout, else nonzero. The probe child is killed and reaped either
   way, so a hung probe cannot leak processes. */
static int watchdog_probe_once(void) {
  pid_t pid;
  int status = 0;
  int waited_ms = 0;

  pid = fork();
  if (pid < 0) return -1;
  if (pid == 0) {
    int devnull = open("/dev/null", O_WRONLY);
    if (devnull >= 0) {
      dup2(devnull, STDOUT_FILENO);
      dup2(devnull, STDERR_FILENO);
    }
    execl("/bin/sh", "sh", "-c", wd_cmd, (char *)NULL);
    _exit(127);
  }

  while (waited_ms < wd_timeout_sec * 1000) {
    pid_t r = waitpid(pid, &status, WNOHANG);
    if (r == pid) return WIFEXITED(status) && WEXITSTATUS(status) == 0 ? 0 : -1;
    if (r < 0 && errno != EINTR) return -1;
    usleep(100 * 1000);
    waited_ms += 100;
  }
  kill(pid, SIGKILL);
  waitpid(pid, &status, 0);
  return -1;
}

static void *watchdog_thread(void *unused) {
  int fails = 0;
  (void)unused;

  while (!exit_req && !watchdog_detach_req) {
    int i;
    for (i = 0; i < wd_interval_sec && !exit_req && !watchdog_detach_req;
         i++)
      sleep(1);
    if (exit_req || watchdog_detach_req) break;

    if (watchdog_probe_once() != 0) {
      fails++;
      fprintf(stderr,
             "scx_db_sched watchdog: probe failed or timed out (%d/%d)\n",
             fails, wd_max_fail);
      if (fails >= wd_max_fail) {
        fprintf(stderr,
                "scx_db_sched watchdog: %d consecutive probe failures — "
                "requesting detach to native EEVDF\n",
                fails);
        watchdog_detach_req = 1;
        break;
      }
    } else {
      if (fails) fprintf(stderr, "scx_db_sched watchdog: probe recovered\n");
      fails = 0;
    }
  }
  return NULL;
}

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

  /* Pin the accounting maps too: they carry per-task vruntime/debt state and
     are the primary forensic evidence when a stall is reported, and bpftool
     cannot reliably locate unpinned maps by name on a busy system. */
  snprintf(path, sizeof(path), SCX_MAP_PIN_PREFIX "task_account_map");
  unlink(path);
  if (bpf_map__pin(skel->maps.task_account_map, path) != 0)
    fprintf(stderr, "Failed to pin task_account_map: %s\n", strerror(errno));

  snprintf(path, sizeof(path), SCX_MAP_PIN_PREFIX "vtime_cpu");
  unlink(path);
  if (bpf_map__pin(skel->maps.vtime_cpu, path) != 0)
    fprintf(stderr, "Failed to pin vtime_cpu: %s\n", strerror(errno));

  snprintf(path, sizeof(path), SCX_MAP_PIN_PREFIX "cpu_cur_order");
  unlink(path);
  if (bpf_map__pin(skel->maps.cpu_cur_order, path) != 0)
    fprintf(stderr, "Failed to pin cpu_cur_order: %s\n", strerror(errno));

  fprintf(stderr, "scx_db_sched: maps pinned under %s\n", SCX_MAP_DIR);
  return 0;
}

static const char *const stat_names[] = {
    "inserts",       "semantic_hits", "commit_boost",  "holder_boost",
    "boosted_runs",  "debt_repay",    "cpubound_cap",  "dispatch_steals",
    "direct_idle",   "wakeup_kicks",  "kick_gated",   NULL,
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
  long opt_lag_carry_us = -1;  /* -1: not specified, keep BPF rodata default */
  bool opt_no_kick = false;
  bool opt_no_kick_gate = false;
  bool opt_no_vlag = false;
  long opt_wakeup_gran_us = -1;  /* -1: not specified, keep BPF rodata default */
  long opt_hog_streak = -1;       /* -1: not specified */
  long opt_wd_interval = 0;
  long opt_wd_timeout = 5;
  long opt_wd_max_fail = 2;
  static const struct option long_options[] = {
      {"dump", no_argument, NULL, 'd'},
      {"stats", no_argument, NULL, 's'},
      {"verbose", no_argument, NULL, 'v'},
      {"help", no_argument, NULL, 'h'},
      {"semantic", no_argument, NULL, 'S'},
      {"hog-slice-us", required_argument, NULL, 'H'},
      {"lag-carry-us", required_argument, NULL, 'L'},
      {"no-kick", no_argument, NULL, 'K'},
    {"no-kick-gate", no_argument, NULL, 'G'},
    {"no-vlag", no_argument, NULL, 'V'},
    {"wakeup-gran-us", required_argument, NULL, 'A'},
    {"hog-streak", required_argument, NULL, 'B'},
      {"watchdog-cmd", required_argument, NULL, 'w'},
      {"watchdog-interval-sec", required_argument, NULL, 'W'},
      {"watchdog-timeout-sec", required_argument, NULL, 'T'},
      {"watchdog-max-fail", required_argument, NULL, 'F'},
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
      case 'w':
        wd_cmd = optarg;
        break;
      case 'W':
        opt_wd_interval = atol(optarg);
        break;
      case 'T':
        opt_wd_timeout = atol(optarg);
        break;
      case 'F':
        opt_wd_max_fail = atol(optarg);
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
      case 'L':
        opt_lag_carry_us = atol(optarg);
        if (opt_lag_carry_us < 0) {
          fprintf(stderr, "--lag-carry-us must be >= 0\n");
          return 1;
        }
        break;
      case 'K':
        opt_no_kick = true;
        break;
      case 'G':
        opt_no_kick_gate = true;
        break;
      case 'V':
        opt_no_vlag = true;
        break;
      case 'A':
        opt_wakeup_gran_us = atol(optarg);
        if (opt_wakeup_gran_us < 0) {
          fprintf(stderr, "--wakeup-gran-us must be >= 0\n");
          return 1;
        }
        break;
      case 'B':
        opt_hog_streak = atol(optarg);
        if (opt_hog_streak < 0) {
          fprintf(stderr, "--hog-streak must be >= 0\n");
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
  if (opt_lag_carry_us >= 0)
    skel->rodata->scx_lag_carry_ns = (u64)opt_lag_carry_us * 1000ULL;
  if (opt_no_kick) skel->rodata->scx_enable_kick = false;
  if (opt_no_kick_gate) skel->rodata->scx_kick_gate = false;
  if (opt_no_vlag) skel->rodata->scx_enable_vlag = false;
  if (opt_wakeup_gran_us >= 0)
    skel->rodata->scx_wakeup_gran_ns = (u64)opt_wakeup_gran_us * 1000ULL;
  if (opt_hog_streak >= 0) skel->rodata->scx_hog_streak_n = (u32)opt_hog_streak;

  fprintf(stderr,
          "scx_db_sched v2: semantic=%s hog_slice=%lluus lag_carry=%lluus "
          "kick=%s gate=%s vlag=%s gran=%lluus streak=%u slice_dfl=%lluus\n",
          opt_semantic ? "on" : "off",
          (unsigned long long)skel->rodata->scx_hog_slice_ns / 1000,
          (unsigned long long)skel->rodata->scx_lag_carry_ns / 1000,
          opt_no_kick ? "off" : "on",
          opt_no_kick_gate ? "off" : "on",
          opt_no_vlag ? "off" : "on",
          (unsigned long long)skel->rodata->scx_wakeup_gran_ns / 1000,
          skel->rodata->scx_hog_streak_n,
          (unsigned long long)SCX_SLICE_DFL / 1000);

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

  if (wd_cmd) {
    if (opt_wd_interval <= 0) {
      fprintf(stderr,
              "--watchdog-cmd given without a positive "
              "--watchdog-interval-sec; watchdog disabled\n");
    } else if (opt_wd_timeout <= 0 || opt_wd_max_fail <= 0) {
      fprintf(stderr,
              "--watchdog-timeout-sec and --watchdog-max-fail must be "
              "positive; watchdog disabled\n");
    } else {
      pthread_t wd_tid;
      wd_interval_sec = (int)opt_wd_interval;
      wd_timeout_sec = (int)opt_wd_timeout;
      wd_max_fail = (int)opt_wd_max_fail;
      if (pthread_create(&wd_tid, NULL, watchdog_thread, NULL) == 0) {
        fprintf(stderr,
                "scx_db_sched watchdog: probe every %ds, timeout %ds, "
                "detach after %d consecutive failures\n",
                wd_interval_sec, wd_timeout_sec, wd_max_fail);
      } else {
        fprintf(stderr, "scx_db_sched watchdog: pthread_create failed (%s); "
                        "continuing without watchdog\n",
                strerror(errno));
      }
    }
  }

  if (dump_maps) {
    dump_task_sem(skel->maps.task_sem);
    dump_wait_edges(skel->maps.wait_edges);
  }

  int tick = 0;
  while (!exit_req && !watchdog_detach_req && !UEI_EXITED(skel, uei)) {
    sleep(1);
    if (++tick >= 10 && print_stats) {
      tick = 0;
      print_stats_once(skel);
    } else if (tick >= 5 && dump_maps) {
      tick = 0;
      dump_task_sem(skel->maps.task_sem);
      dump_wait_edges(skel->maps.wait_edges);
    }
  }

  bpf_link__destroy(link);
  ecode = UEI_REPORT(skel, uei);
  scx_db_sched__destroy(skel);

  if (watchdog_detach_req) {
    fprintf(stderr,
            "scx_db_sched: watchdog detached the scheduler — native EEVDF "
            "restored for all tasks.\n");
    return 3;
  }
  return ecode == 0 ? 0 : (ecode < 256 ? (int)ecode : 1);
}
