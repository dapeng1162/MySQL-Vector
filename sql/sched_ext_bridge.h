/*
   Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is governed by separate terms and
   described in the license file that accompanies this program.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#ifndef SQL_SCHED_EXT_BRIDGE_INCLUDED
#define SQL_SCHED_EXT_BRIDGE_INCLUDED

#include <thread>

class THD;

namespace sched_ext {

bool connected();

void init();

void commit_begin();
void commit_end();

void thread_start();
void thread_end();

enum class Latch_kind { Mutex, RwLock };

bool latch_wait_begin(std::thread::id owner, Latch_kind kind);
void latch_wait_end();

uint64_t latch_wait_count(Latch_kind kind);
uint64_t latch_wait_edge_count(Latch_kind kind);
std::thread::id latch_wait_holder(std::thread::id waiter);

bool any_lock_waits();
bool is_lock_holder(THD *thd);

void lock_wait_begin(THD *waiter, THD *holder);
void lock_wait_end(THD *waiter);

void forget_thd(THD *thd);

void set_testing_mode(bool enabled);

}  // namespace sched_ext

#endif /* SQL_SCHED_EXT_BRIDGE_INCLUDED */
