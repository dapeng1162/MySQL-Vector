/* SPDX-License-Identifier: GPL-2.0 */
/*
 * Userspace loader for the DB-aware sched_ext scheduler (scx_db_sched).
 *
 * Loads scx_db_sched.bpf.o (built from scx_db_sched.bpf.c), attaches it
 * as the system scheduler, and optionally dumps the contents of the four
 * DB-aware maps (commit_vip, blocked_by, lock_holder, rg_prio).
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

#include <bpf/bpf.h>
#include <scx/common.h>

#include "scx_db_sched.bpf.skel.h"

static const char help_fmt[] =
"A DB-aware sched_ext scheduler for MySQL / InnoDB.\n"
"\n"
"Usage: %s [--dump] [-v]\n"
"\n"
"  --dump         After attaching, dump the current contents of the\n"
"                 DB-aware maps (commit_vip, blocked_by, lock_holder,\n"
"                 rg_prio) and then keep running.\n"
"  -v             Print libbpf debug messages\n"
"  -h             Display this help and exit\n";

static bool verbose;
static bool dump_maps;
static volatile int exit_req;

/*
 * Where the DB-aware maps are pinned so that a running mysqld (via
 * sql/sched_ext_bridge.cc) can open them with bpf_obj_get().  All four
 * maps share a single directory under /sys/fs/bpf.
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
  char value[8] = {0};

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
      else
        printf("  tid=%u value=%s\n", k, value_fmt);
    }
    key = next_key;
  }
  if (ret < 0 && errno != ENOENT)
    printf("  (iteration error: %s)\n", strerror(errno));
}

/*
 * Pin the four DB-aware maps so that the running mysqld can open them
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
  dump_map_entries("lock_holder", skel->maps.lock_holder, 4, "u32");
  dump_map_entries("rg_prio", skel->maps.rg_prio, 4, "i32");
  printf("=== end dump ===\n");
  fflush(stdout);
}

int main(int argc, char **argv) {
  struct scx_db_sched *skel;
  struct bpf_link *link;
  static const struct option long_options[] = {
      {"dump", no_argument, NULL, 'd'},
      {"verbose", no_argument, NULL, 'v'},
      {"help", no_argument, NULL, 'h'},
      {NULL, 0, NULL, 0},
  };
  int opt;
  __u64 ecode;

  libbpf_set_print(libbpf_print_fn);
  signal(SIGINT, sigint_handler);
  signal(SIGTERM, sigint_handler);

  if (check_sched_ext_available() != 0) return 1;

restart:
  skel = SCX_OPS_OPEN(db_sched_ops, scx_db_sched);

  while ((opt = getopt_long(argc, argv, "dvh", long_options, NULL)) != -1) {
    switch (opt) {
      case 'd':
        dump_maps = true;
        break;
      case 'v':
        verbose = true;
        break;
      default:
        fprintf(stderr, help_fmt, basename(argv[0]));
        return opt != 'h';
    }
  }

  SCX_OPS_LOAD(skel, db_sched_ops, scx_db_sched, uei);

  if (pin_maps(skel) != 0) {
    fprintf(stderr, "scx_db_sched: map pinning failed, continuing anyway\n");
  }

  link = SCX_OPS_ATTACH(skel, db_sched_ops, scx_db_sched);

  fprintf(stderr, "scx_db_sched: attached as the system scheduler.\n");

  if (dump_maps) dump_maps_all(skel);

  while (!exit_req && !UEI_EXITED(skel, uei)) {
    if (dump_maps) {
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
