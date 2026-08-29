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
# DB-aware sched_ext scheduler for the coroutine connection handler.
# Requires: Linux >= 6.12 with CONFIG_SCHED_CLASS_EXT, clang, bpftool.
# ---------------------------------------------------------------------------

OPTION(WITH_SCX_SCHEDULER
  "Build the DB-aware sched_ext BPF scheduler (scx_db_sched) and inject bridge"
  OFF)

# When the scheduler is NOT requested, skip everything. The bridge layer
# compiled into sql_main provides no-op fallbacks, so the server builds
# and runs normally without any sched_ext dependency.
IF(NOT WITH_SCX_SCHEDULER)
  RETURN()
ENDIF()

# -- Toolchain checks ------------------------------------------------------

# Use the project's clang (from the build environment) or fall back to PATH.
FIND_PROGRAM(SCX_CLANG clang)
IF(NOT SCX_CLANG)
  MESSAGE(FATAL_ERROR
    "WITH_SCX_SCHEDULER requires clang to compile BPF programs.")
ENDIF()

FIND_PROGRAM(SCX_BPFTOOL bpftool)
IF(NOT SCX_BPFTOOL)
  MESSAGE(FATAL_ERROR
    "WITH_SCX_SCHEDULER requires bpftool to generate BPF skeletons.")
ENDIF()

# Check that the scx/common.bpf.h header is reachable (from the kernel
# source tree under /root/src/kernel, or from another location the user
# provides via SCX_INCLUDE_DIR).
IF(NOT SCX_INCLUDE_DIR)
  SET(SCX_INCLUDE_DIR "/root/src/kernel/tools/sched_ext/include")
ENDIF()
IF(NOT EXISTS "${SCX_INCLUDE_DIR}/scx/common.bpf.h")
  MESSAGE(WARNING
    "scx/common.bpf.h not found at ${SCX_INCLUDE_DIR}. "
    "Set SCX_INCLUDE_DIR to the path containing the scx/ subdirectory. "
    "BPF compilation may fail.")
ENDIF()

# Check for vmlinux.h (needed by scx/common.bpf.h).  The user may point
# SCX_VMLINUX_H at a vmlinux.h generated from a kernel with sched_ext.
# If not provided, bpftool can generate it from /sys/kernel/btf/vmlinux
# (but that only works on a sched_ext-capable kernel).
IF(NOT SCX_VMLINUX_H)
  SET(SCX_VMLINUX_H "${CMAKE_CURRENT_BINARY_DIR}/vmlinux.h")
ENDIF()

# -- Build targets ---------------------------------------------------------

SET(SCX_SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}")
SET(SCX_BIN_DIR "${CMAKE_CURRENT_BINARY_DIR}")

# BPF object
SET(SCX_BPF_OBJ "${SCX_BIN_DIR}/scx_db_sched.bpf.o")
SET(SCX_BPF_SKEL "${SCX_BIN_DIR}/scx_db_sched.bpf.skel.h")

# BPF target architecture (must match the running kernel's arch).
# Default: derive from the host architecture. Override via SCX_TARGET_ARCH.
IF(NOT SCX_TARGET_ARCH)
  IF(CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64|AMD64)$")
    SET(SCX_TARGET_ARCH "x86")
  ELSEIF(CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64|arm64)$")
    SET(SCX_TARGET_ARCH "arm64")
  ELSEIF(CMAKE_SYSTEM_PROCESSOR MATCHES "^arm")
    SET(SCX_TARGET_ARCH "arm")
  ELSEIF(CMAKE_SYSTEM_PROCESSOR MATCHES "riscv")
    SET(SCX_TARGET_ARCH "riscv")
  ELSE()
    SET(SCX_TARGET_ARCH "x86")
    MESSAGE(WARNING "Unknown arch '${CMAKE_SYSTEM_PROCESSOR}', "
                    "defaulting BPF target to x86")
  ENDIF()
ENDIF()

# Additional BPF flags
SET(SCX_BPF_CFLAGS
  -g
  -D__TARGET_ARCH_${SCX_TARGET_ARCH}
  -mlittle-endian
  -I${SCX_INCLUDE_DIR}
  -I${SCX_BIN_DIR}
  -Wall
  -Wno-compare-distinct-pointer-types
  -O2
  -mcpu=v3
  -target bpf
)

# If vmlinux.h doesn't exist yet, try to generate it from /sys/kernel/btf/vmlinux.
# This will succeed only on a sched_ext-capable kernel.
IF(NOT EXISTS "${SCX_VMLINUX_H}")
  IF(EXISTS "/sys/kernel/btf/vmlinux")
    ADD_CUSTOM_COMMAND(
      OUTPUT ${SCX_VMLINUX_H}
      COMMAND ${SCX_BPFTOOL} btf dump file /sys/kernel/btf/vmlinux format c
              > ${SCX_VMLINUX_H}
      COMMENT "Generating vmlinux.h from /sys/kernel/btf/vmlinux"
    )
  ELSE()
    MESSAGE(WARNING
      "vmlinux.h not found and /sys/kernel/btf/vmlinux is not available. "
      "Set SCX_VMLINUX_H to a suitable vmlinux.h (from a kernel with "
      "CONFIG_SCHED_CLASS_EXT). BPF compilation will fail without it.")
  ENDIF()
ENDIF()

# Compile BPF program
ADD_CUSTOM_COMMAND(
  OUTPUT ${SCX_BPF_OBJ}
  COMMAND ${SCX_CLANG} ${SCX_BPF_CFLAGS}
          -c ${SCX_SRC_DIR}/scx_db_sched.bpf.c
          -o ${SCX_BPF_OBJ}
  DEPENDS ${SCX_SRC_DIR}/scx_db_sched.bpf.c ${SCX_VMLINUX_H}
  COMMENT "Compiling scx_db_sched.bpf.c → scx_db_sched.bpf.o"
)

# Generate BPF skeleton (three-step bpftool gen object + gen skeleton)
SET(SCX_BPF_LINKED1 "${SCX_BIN_DIR}/scx_db_sched.bpf.linked1.o")
SET(SCX_BPF_LINKED2 "${SCX_BIN_DIR}/scx_db_sched.bpf.linked2.o")
SET(SCX_BPF_LINKED3 "${SCX_BIN_DIR}/scx_db_sched.bpf.linked3.o")

ADD_CUSTOM_COMMAND(
  OUTPUT ${SCX_BPF_SKEL}
  COMMAND ${SCX_BPFTOOL} gen object ${SCX_BPF_LINKED1} ${SCX_BPF_OBJ}
  COMMAND ${SCX_BPFTOOL} gen object ${SCX_BPF_LINKED2} ${SCX_BPF_LINKED1}
  COMMAND ${SCX_BPFTOOL} gen object ${SCX_BPF_LINKED3} ${SCX_BPF_LINKED2}
  COMMAND diff ${SCX_BPF_LINKED2} ${SCX_BPF_LINKED3}
  COMMAND ${SCX_BPFTOOL} gen skeleton ${SCX_BPF_LINKED3}
          name scx_db_sched > ${SCX_BPF_SKEL}
  DEPENDS ${SCX_BPF_OBJ}
  COMMENT "Generating BPF skeleton → scx_db_sched.bpf.skel.h"
)

# The `scx_db_sched` loader executable
ADD_EXECUTABLE(scx_db_sched
  ${SCX_SRC_DIR}/scx_db_sched_main.c
  ${SCX_BPF_SKEL}
)
TARGET_INCLUDE_DIRECTORIES(scx_db_sched
  PRIVATE ${SCX_BIN_DIR}
  PRIVATE ${SCX_INCLUDE_DIR}
)
TARGET_LINK_LIBRARIES(scx_db_sched
  bpf
  elf
  z
  pthread
)

# Install the loader
INSTALL(TARGETS scx_db_sched
  RUNTIME DESTINATION ${INSTALL_SBINDIR}
  COMPONENT Server_Scripts
)

# Placeholder install for the .bpf.o artifact (optional, can be useful for
# debugging / introspection).
INSTALL(FILES ${SCX_BPF_OBJ}
  DESTINATION ${INSTALL_SHAREDIR}/scx
  COMPONENT Server_Scripts
)

# -- Export flags for the server build ------------------------------------
#
# When the scheduler is enabled, the server bridge (sql/sched_ext_bridge.cc)
# and the injection points in sql/handler.cc need HAVE_SCX_SCHEDULER.
# These are propagated via the SCX_COMPILE_DEFINITIONS cache variable so
# the sql/CMakeLists.txt can pick them up.
SET(SCX_COMPILE_DEFINITIONS
  "HAVE_SCX_SCHEDULER"
  CACHE INTERNAL "Compile definitions for WITH_SCX_SCHEDULER"
)
SET(SCX_LIBRARIES
  bpf
  CACHE INTERNAL "Libraries needed for WITH_SCX_SCHEDULER bridge"
)