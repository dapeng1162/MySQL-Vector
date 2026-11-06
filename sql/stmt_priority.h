/*
   Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in their documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

/**
  @file stmt_priority.h

  Statement-priority taxonomy for the coroutine connection handler.

  The userspace high-priority queue of the coroutine executor pool is a
  strict priority queue (dedicated VIP workers consume it first), so a
  statement may use it only when prioritizing it is BOTH rare in any
  realistic workload AND globally beneficial.  Blanket prioritization of
  common statements (e.g. every COMMIT of an active transaction) would
  degenerate the HP queue into the main queue under write-heavy
  workloads and starve the statements of other transactions.
  Commit latency is instead addressed by the bounded kernel-side bonus
  (sched_ext bridge commit hooks) and by the lock-holder path.

  The taxonomy (per Docs/coroutine-sched-resilience-design.md, D5):

    Kill             KILL QUERY / KILL CONNECTION: rare, latency-critical,
                     and unblocks the killed session's locks.
    VictimRollback   COMMIT/ROLLBACK issued after the previous statement
                     failed with ER_LOCK_DEADLOCK or ER_LOCK_WAIT_TIMEOUT:
                     the rollback releases locks other transactions are
                     waiting on (or finishes a transaction InnoDB already
                     rolled back), so executing it promptly frees waiters.
    UnlockTables     UNLOCK TABLES while the session holds open tables:
                     releases table and metadata locks other sessions may
                     be waiting for.
    ReleaseLock      A statement containing a top-level RELEASE_LOCK() /
                     RELEASE_ALL_LOCKS(): releases advisory user locks
                     other sessions may be blocked on in GET_LOCK().

  Deliberately NOT prioritized:

    - ROLLBACK TO SAVEPOINT: rolling back to a savepoint does not
      necessarily release the row locks other transactions wait on.
    - GET_LOCK(): acquiring a lock does not release waiters.

  This header is deliberately self-contained (no THD/LEX dependencies):
  classify() is a pure function over a plain input struct so it can be
  unit-tested without a server (unittest/gunit/coroutine/stmt_priority-t).
  The THD adapter that fills Input lives next to its only caller in
  sql/conn_handler/coroutine_connection_handler.cc.
*/

#ifndef SQL_STMT_PRIORITY_INCLUDED
#define SQL_STMT_PRIORITY_INCLUDED

#include <atomic>
#include <cstddef>

#include "my_command.h"     // enum_server_command
#include "my_sqlcommand.h"  // enum_sql_command

namespace stmt_priority {

/** Why a statement was (or would be) prioritized. */
enum class Reason : uint8_t {
  None = 0,
  /** KILL QUERY / KILL CONNECTION. */
  Kill,
  /** COMMIT/ROLLBACK right after a lock-conflict failure. */
  VictimRollback,
  /** UNLOCK TABLES while holding open tables. */
  UnlockTables,
  /** Statement containing a top-level RELEASE_LOCK() call. */
  ReleaseLock,
};

constexpr size_t kNumReasons = 5;

/**
  Plain-value input for the pure classifier; every field is cheap to
  compute from a THD after the reactor parsed the statement.
*/
struct Input {
  /** Wire command (COM_QUERY, ...) of the pending packet. */
  enum_server_command command{COM_SLEEP};
  /** True when sql_command comes from a reactor-side parse (AST valid). */
  bool parsed{false};
  /** Only meaningful when parsed is true. */
  enum_sql_command sql_command{SQLCOM_END};
  /** THD holds open tables (thd->open_tables != nullptr). */
  bool holds_open_tables{false};
  /** Parsed SELECT list contains RELEASE_LOCK()/RELEASE_ALL_LOCKS(). */
  bool contains_release_lock{false};
  /** Previous statement on this connection failed with a lock conflict. */
  bool lock_conflict_victim{false};
};

/**
  Classify a pending statement.  Pure function; returns Reason::None for
  everything that should stay on the normal queue.
*/
inline Reason classify(const Input &in) {
  if (!in.parsed) return Reason::None;
  switch (in.sql_command) {
    case SQLCOM_KILL:
      return Reason::Kill;
    case SQLCOM_UNLOCK_TABLES:
      return in.holds_open_tables ? Reason::UnlockTables : Reason::None;
    case SQLCOM_SELECT:
      return in.contains_release_lock ? Reason::ReleaseLock : Reason::None;
    case SQLCOM_COMMIT:
    case SQLCOM_ROLLBACK:
    case SQLCOM_XA_COMMIT:
    case SQLCOM_XA_ROLLBACK:
      // Not SQLCOM_ROLLBACK_TO_SAVEPOINT: it does not necessarily
      // release the row locks other transactions are waiting on.
      return in.lock_conflict_victim ? Reason::VictimRollback : Reason::None;
    default:
      return Reason::None;
  }
}

/** Per-reason submit counters, exported as Coroutine_stmt_prio_* status. */
inline std::atomic<long long> g_reason_counters[kNumReasons] = {};

inline void note_prioritized(Reason r) {
  if (r != Reason::None) {
    g_reason_counters[static_cast<size_t>(r)].fetch_add(
        1, std::memory_order_relaxed);
  }
}

inline long long counter(Reason r) {
  if (r == Reason::None) return 0;
  return g_reason_counters[static_cast<size_t>(r)].load(
      std::memory_order_relaxed);
}

}  // namespace stmt_priority

#endif /* SQL_STMT_PRIORITY_INCLUDED */
