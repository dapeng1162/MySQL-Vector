/* Copyright (c) 2026, Oracle and/or its affiliates

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published
   by the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef MYSQL_SCHED_THREAD_INCLUDED
#define MYSQL_SCHED_THREAD_INCLUDED

#ifdef __cplusplus
extern "C" {
#endif

/**
  Register the calling thread with the sched_ext bridge so that its
  thread-id can be resolved when other threads block on latches it
  holds.  Threads that never acquire shared latches may skip this.
*/
void mysql_sched_register_thread(void);

#ifdef __cplusplus
}
#endif

#endif /* MYSQL_SCHED_THREAD_INCLUDED */
