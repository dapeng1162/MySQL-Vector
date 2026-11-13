/* Copyright (c) 2026, Oracle and/or its affiliates

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/**
  @file mysql_sched_tier.h

  C-linkage thread-tier hook shared between the server (sql/) and storage
  engines (storage/innobase/...), which cannot include sql/ headers.

  The tiers map to the task_tier BPF map of the DB-aware sched_ext
  scheduler (support-files/scx/scx_db_sched.bpf.c):

    TS     compatibility marker; smart_sched has no separate strict-priority
           queue for this tier
    NORMAL default
    BG     background weight 25/100 when semantic scheduling is enabled
           (purge, dict_stats, fts optimize, monitors, dumps); it is not
           restricted to idle CPU capacity

  The implementation is a no-op when the server was built without
  HAVE_SCX_SCHEDULER or no scheduler is connected.
*/

#ifndef MYSQL_SCHED_TIER_INCLUDED
#define MYSQL_SCHED_TIER_INCLUDED

#define MYSQL_SCHED_TIER_TS 0
#define MYSQL_SCHED_TIER_NORMAL 1
#define MYSQL_SCHED_TIER_BG 2

#ifdef __cplusplus
extern "C" {
#endif

/** Set the calling thread's scheduling tier.  Must be called from the
    thread itself, at thread entry (or around a job's execution). */
void mysql_sched_set_thread_tier(int tier);

#ifdef __cplusplus
}
#endif

#endif /* MYSQL_SCHED_TIER_INCLUDED */
