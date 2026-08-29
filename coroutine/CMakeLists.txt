# Copyright (c) 2026, Oracle and/or its affiliates.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0,
# as published by the Free Software Foundation.
#
# This program is designed to work with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation.  The authors of MySQL hereby grant you an additional
# permission to link the program and your derivative works with the
# separately licensed software that they have either included with
# the program or referenced in the documentation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

# ---------------------------------------------------------------------------
# Coroutine runtime library for the epoll-based coroutine front-end.
# Platform: Linux >= 6.12 only (enforced by top-level CMakeLists.txt).
# ---------------------------------------------------------------------------

# If the coroutine connection handler is not wanted, skip the build
# entirely. The WITH_COROUTINE option is ON by default when the platform
# check passes.
OPTION(WITH_COROUTINE "Build the coroutine runtime library" ON)

IF(NOT WITH_COROUTINE)
  RETURN()
ENDIF()

SET(CORO_SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src")
SET(CORO_INC_DIR "${CMAKE_CURRENT_SOURCE_DIR}/include")

# Collect sources
FILE(GLOB CORO_SOURCES "${CORO_SRC_DIR}/*.cc")
FILE(GLOB CORO_HEADERS "${CORO_INC_DIR}/coroutine/*.h")

# C++20 coroutines are available with the project's C++23 standard
# (GCC 10+ / Clang 14+); no extra compiler flag is required.

ADD_LIBRARY(mysql_coroutine STATIC ${CORO_SOURCES} ${CORO_HEADERS})
TARGET_INCLUDE_DIRECTORIES(mysql_coroutine
  PUBLIC  "${CORO_INC_DIR}"
  PRIVATE "${CORO_SRC_DIR}")
TARGET_LINK_LIBRARIES(mysql_coroutine PUBLIC pthread)

# Make the library available to the rest of the build
SET(LL_COROUTINE_LIBRARIES mysql_coroutine CACHE INTERNAL "")

INSTALL(TARGETS mysql_coroutine
  ARCHIVE DESTINATION ${INSTALL_MYSQL_SHAREDIR})