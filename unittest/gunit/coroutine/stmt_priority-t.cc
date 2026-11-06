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

#include <gtest/gtest.h>

#include "sql/stmt_priority.h"

namespace stmt_priority {
namespace {

Input parsed(enum_sql_command sql_command) {
  Input in;
  in.command = COM_QUERY;
  in.parsed = true;
  in.sql_command = sql_command;
  return in;
}

TEST(StmtPriorityTest, DefaultInputIsNormal) {
  EXPECT_EQ(classify(Input{}), Reason::None);
}

TEST(StmtPriorityTest, UnparsedIsNormal) {
  Input in = parsed(SQLCOM_KILL);
  in.parsed = false;
  EXPECT_EQ(classify(in), Reason::None);
}

TEST(StmtPriorityTest, KillIsPrioritized) {
  EXPECT_EQ(classify(parsed(SQLCOM_KILL)), Reason::Kill);
}

TEST(StmtPriorityTest, VictimRollback) {
  Input rollback = parsed(SQLCOM_ROLLBACK);
  EXPECT_EQ(classify(rollback), Reason::None);
  rollback.lock_conflict_victim = true;
  EXPECT_EQ(classify(rollback), Reason::VictimRollback);

  Input commit = parsed(SQLCOM_COMMIT);
  commit.lock_conflict_victim = true;
  EXPECT_EQ(classify(commit), Reason::VictimRollback);

  Input xa_commit = parsed(SQLCOM_XA_COMMIT);
  xa_commit.lock_conflict_victim = true;
  EXPECT_EQ(classify(xa_commit), Reason::VictimRollback);

  Input xa_rollback = parsed(SQLCOM_XA_ROLLBACK);
  xa_rollback.lock_conflict_victim = true;
  EXPECT_EQ(classify(xa_rollback), Reason::VictimRollback);
}

// Rolling back to a savepoint does not necessarily release the row locks
// other transactions are waiting on, so it must stay on the normal queue
// even for a lock-conflict victim.
TEST(StmtPriorityTest, RollbackToSavepointIsNotPrioritized) {
  Input in = parsed(SQLCOM_ROLLBACK_TO_SAVEPOINT);
  in.lock_conflict_victim = true;
  EXPECT_EQ(classify(in), Reason::None);
}

TEST(StmtPriorityTest, UnlockTables) {
  Input in = parsed(SQLCOM_UNLOCK_TABLES);
  EXPECT_EQ(classify(in), Reason::None);
  in.holds_open_tables = true;
  EXPECT_EQ(classify(in), Reason::UnlockTables);
}

TEST(StmtPriorityTest, ReleaseLock) {
  Input in = parsed(SQLCOM_SELECT);
  EXPECT_EQ(classify(in), Reason::None);
  in.contains_release_lock = true;
  EXPECT_EQ(classify(in), Reason::ReleaseLock);
}

TEST(StmtPriorityTest, CommonStatementsAreNormal) {
  for (enum_sql_command sql_command :
       {SQLCOM_SELECT, SQLCOM_INSERT, SQLCOM_UPDATE, SQLCOM_DELETE,
        SQLCOM_BEGIN, SQLCOM_SAVEPOINT, SQLCOM_SET_OPTION, SQLCOM_SHOW_STATUS,
        SQLCOM_CREATE_TABLE, SQLCOM_DROP_TABLE, SQLCOM_ALTER_TABLE,
        SQLCOM_ANALYZE, SQLCOM_EXPLAIN_OTHER}) {
    Input in = parsed(sql_command);
    in.lock_conflict_victim = true;  // victim flag only gates commit paths
    in.holds_open_tables = true;
    EXPECT_EQ(classify(in), Reason::None)
        << "sql_command " << (int)sql_command;
  }
}

TEST(StmtPriorityTest, CountersTrackPerReason) {
  for (size_t i = 0; i < kNumReasons; ++i)
    g_reason_counters[i].store(0, std::memory_order_relaxed);

  note_prioritized(Reason::Kill);
  note_prioritized(Reason::Kill);
  note_prioritized(Reason::VictimRollback);
  note_prioritized(Reason::None);  // must be a no-op

  EXPECT_EQ(counter(Reason::Kill), 2);
  EXPECT_EQ(counter(Reason::VictimRollback), 1);
  EXPECT_EQ(counter(Reason::UnlockTables), 0);
  EXPECT_EQ(counter(Reason::ReleaseLock), 0);
  EXPECT_EQ(counter(Reason::None), 0);

  g_reason_counters[static_cast<size_t>(Reason::Kill)].store(
      0, std::memory_order_relaxed);
  g_reason_counters[static_cast<size_t>(Reason::VictimRollback)].store(
      0, std::memory_order_relaxed);
}

}  // namespace
}  // namespace stmt_priority
