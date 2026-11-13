/*
   Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#include "sql/sched_ext_bridge.h"

#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "sql/sql_class.h"

namespace sched_ext {

bool connected();

namespace {

template <typename K, typename V>
struct Sharded_map {
  static constexpr size_t kCount = 256;
  std::mutex mu[kCount];
  std::unordered_map<K, V> m[kCount];
  size_t idx(K k) const { return std::hash<K>()(k) & (kCount - 1); }
};

static Sharded_map<THD *, THD *> s_waiter_to_holder;
static Sharded_map<THD *, uint32_t> s_holder_wait_count;
static std::atomic<uint32_t> s_thd_wait_edges{0};

static Sharded_map<std::thread::id, std::thread::id>
    s_latch_waiter_to_holder;
static std::atomic<uint64_t> s_latch_waits[2] = {};
static std::atomic<uint64_t> s_latch_edges[2] = {};

static std::atomic<bool> s_testing{false};

bool mirror_active() { return connected() || s_testing.load(); }

uint32_t holder_wait_count(THD *holder) {
  const size_t i = s_holder_wait_count.idx(holder);
  std::lock_guard<std::mutex> lock(s_holder_wait_count.mu[i]);
  auto it = s_holder_wait_count.m[i].find(holder);
  return it == s_holder_wait_count.m[i].end() ? 0 : it->second;
}

#ifdef HAVE_SCX_SCHEDULER
void scx_publish_wait_edge_for_thd(THD *waiter, THD *holder);
void scx_clear_wait_edge_for_thd(THD *waiter);
bool scx_publish_latch_edge(std::thread::id owner, int kind);
void scx_clear_latch_edge();
#endif

struct ActiveEdge {
  int32_t waiter_tid;
  uint32_t seq;
  int32_t holder_tid;
  uint32_t kind;
};

thread_local ActiveEdge t_active_edges[4];
thread_local int t_active_edge_count = 0;

bool push_active_edge(int32_t waiter_tid, uint32_t seq, int32_t holder_tid,
                      uint32_t kind) {
  if (t_active_edge_count >= 4) return false;
  t_active_edges[t_active_edge_count++] = {waiter_tid, seq, holder_tid, kind};
  return true;
}

bool pop_active_edge(int32_t waiter_tid, ActiveEdge *out) {
  for (int i = t_active_edge_count - 1; i >= 0; --i) {
    if (t_active_edges[i].waiter_tid == waiter_tid) {
      *out = t_active_edges[i];
      for (int j = i; j < t_active_edge_count - 1; ++j)
        t_active_edges[j] = t_active_edges[j + 1];
      --t_active_edge_count;
      return true;
    }
  }
  return false;
}

}  // namespace

bool any_lock_waits() {
  return s_thd_wait_edges.load(std::memory_order_relaxed) != 0;
}

bool is_lock_holder(THD *thd) {
  if (thd == nullptr) return false;
  return holder_wait_count(thd) > 0;
}

void lock_wait_begin(THD *waiter, THD *holder) {
  if (waiter == nullptr || holder == nullptr || !mirror_active()) return;

  {
    const size_t i = s_waiter_to_holder.idx(waiter);
    std::lock_guard<std::mutex> lock(s_waiter_to_holder.mu[i]);
    s_waiter_to_holder.m[i][waiter] = holder;
  }
  {
    const size_t i = s_holder_wait_count.idx(holder);
    std::lock_guard<std::mutex> lock(s_holder_wait_count.mu[i]);
    ++s_holder_wait_count.m[i][holder];
  }
  s_thd_wait_edges.fetch_add(1, std::memory_order_relaxed);

#ifdef HAVE_SCX_SCHEDULER
  scx_publish_wait_edge_for_thd(waiter, holder);
#endif
}

void lock_wait_end(THD *waiter) {
  if (waiter == nullptr) return;

  THD *holder = nullptr;
  {
    const size_t i = s_waiter_to_holder.idx(waiter);
    std::lock_guard<std::mutex> lock(s_waiter_to_holder.mu[i]);
    auto w = s_waiter_to_holder.m[i].find(waiter);
    if (w != s_waiter_to_holder.m[i].end()) {
      holder = w->second;
      s_waiter_to_holder.m[i].erase(w);
      s_thd_wait_edges.fetch_sub(1, std::memory_order_relaxed);
    }
  }
  if (holder != nullptr) {
    const size_t i = s_holder_wait_count.idx(holder);
    std::lock_guard<std::mutex> lock(s_holder_wait_count.mu[i]);
    auto c = s_holder_wait_count.m[i].find(holder);
    if (c != s_holder_wait_count.m[i].end()) {
      if (c->second > 1)
        --c->second;
      else
        s_holder_wait_count.m[i].erase(c);
    }
  }

#ifdef HAVE_SCX_SCHEDULER
  scx_clear_wait_edge_for_thd(waiter);
#endif
}

void forget_thd(THD *thd) {
  if (thd == nullptr) return;

  {
    THD *holder = nullptr;
    {
      const size_t i = s_waiter_to_holder.idx(thd);
      std::lock_guard<std::mutex> lock(s_waiter_to_holder.mu[i]);
      auto w = s_waiter_to_holder.m[i].find(thd);
      if (w != s_waiter_to_holder.m[i].end()) {
        holder = w->second;
        s_waiter_to_holder.m[i].erase(w);
        s_thd_wait_edges.fetch_sub(1, std::memory_order_relaxed);
      }
    }
    if (holder != nullptr) {
      const size_t i = s_holder_wait_count.idx(holder);
      std::lock_guard<std::mutex> lock(s_holder_wait_count.mu[i]);
      auto c = s_holder_wait_count.m[i].find(holder);
      if (c != s_holder_wait_count.m[i].end()) {
        if (c->second > 1)
          --c->second;
        else
          s_holder_wait_count.m[i].erase(c);
      }
    }
  }
  {
    const size_t i = s_holder_wait_count.idx(thd);
    std::lock_guard<std::mutex> lock(s_holder_wait_count.mu[i]);
    s_holder_wait_count.m[i].erase(thd);
  }
}

bool latch_wait_begin(std::thread::id owner, Latch_kind kind) {
  if (!mirror_active()) return false;

  const int k = static_cast<int>(kind);
  s_latch_waits[k].fetch_add(1, std::memory_order_relaxed);
  if (owner == std::thread::id{}) return false;

  const std::thread::id waiter = std::this_thread::get_id();
  {
    const size_t i = s_latch_waiter_to_holder.idx(waiter);
    std::lock_guard<std::mutex> lock(s_latch_waiter_to_holder.mu[i]);
    s_latch_waiter_to_holder.m[i][waiter] = owner;
  }
  s_latch_edges[k].fetch_add(1, std::memory_order_relaxed);

#ifdef HAVE_SCX_SCHEDULER
  return scx_publish_latch_edge(owner, k);
#else
  return true;
#endif
}

void latch_wait_end() {
  {
    const std::thread::id waiter = std::this_thread::get_id();
    const size_t i = s_latch_waiter_to_holder.idx(waiter);
    std::lock_guard<std::mutex> lock(s_latch_waiter_to_holder.mu[i]);
    s_latch_waiter_to_holder.m[i].erase(waiter);
  }

#ifdef HAVE_SCX_SCHEDULER
  scx_clear_latch_edge();
#endif
}

uint64_t latch_wait_count(Latch_kind kind) {
  return s_latch_waits[static_cast<int>(kind)].load(std::memory_order_relaxed);
}

uint64_t latch_wait_edge_count(Latch_kind kind) {
  return s_latch_edges[static_cast<int>(kind)].load(std::memory_order_relaxed);
}

std::thread::id latch_wait_holder(std::thread::id waiter) {
  const size_t i = s_latch_waiter_to_holder.idx(waiter);
  std::lock_guard<std::mutex> lock(s_latch_waiter_to_holder.mu[i]);
  auto it = s_latch_waiter_to_holder.m[i].find(waiter);
  return it == s_latch_waiter_to_holder.m[i].end() ? std::thread::id{}
                                                   : it->second;
}

void set_testing_mode(bool enabled) {
  s_testing.store(enabled, std::memory_order_relaxed);
}

}  // namespace sched_ext

#ifdef HAVE_SCX_SCHEDULER

#include <bpf/bpf.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>

#include "my_thread.h"

namespace sched_ext {
namespace {

static constexpr const char *kMapTaskSem = "/sys/fs/bpf/scx_db_sched/task_sem";
static constexpr const char *kMapWaitEdges =
    "/sys/fs/bpf/scx_db_sched/wait_edges";

static std::atomic<bool> s_connected{false};
static std::atomic<int> s_fd_task_sem{-1};
static std::atomic<int> s_fd_wait_edges{-1};
static std::atomic<uint64_t> s_wait_seq{1};

static Sharded_map<my_thread_t, int32_t> s_tid_registry;
static Sharded_map<std::thread::id, int32_t> s_std_tid_registry;
thread_local int32_t s_current_tid = -1;

struct TaskState {
  bool commit;
  uint32_t holder_count;
};
static Sharded_map<int32_t, TaskState> s_task_state;

struct MapTaskSem {
  uint32_t flags;
  uint32_t holder_count;
  uint64_t ts_ns;
};

struct MapWaitEdgeKey {
  uint32_t waiter_tid;
  uint32_t seq;
};

struct MapWaitEdgeValue {
  uint32_t holder_tid;
  uint32_t kind;
  uint64_t ts_ns;
};

inline uint64_t now_ns() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

inline int32_t current_tid() {
  if (s_current_tid < 0)
    s_current_tid = static_cast<int32_t>(syscall(SYS_gettid));
  return s_current_tid;
}

int open_map(const char *path) { return bpf_obj_get(path); }

int32_t thd_os_tid(THD *thd) {
  if (thd == nullptr) return -1;
  const size_t i = s_tid_registry.idx(thd->real_id);
  std::lock_guard<std::mutex> lock(s_tid_registry.mu[i]);
  auto it = s_tid_registry.m[i].find(thd->real_id);
  return it == s_tid_registry.m[i].end() ? -1 : it->second;
}

int32_t std_tid(std::thread::id id) {
  const size_t i = s_std_tid_registry.idx(id);
  std::lock_guard<std::mutex> lock(s_std_tid_registry.mu[i]);
  auto it = s_std_tid_registry.m[i].find(id);
  return it == s_std_tid_registry.m[i].end() ? -1 : it->second;
}

void publish_task_sem(int32_t tid) {
  int fd = s_fd_task_sem.load(std::memory_order_acquire);
  if (fd < 0 || tid < 0) return;

  const size_t i = s_task_state.idx(tid);
  std::lock_guard<std::mutex> lock(s_task_state.mu[i]);
  auto it = s_task_state.m[i].find(tid);
  if (it == s_task_state.m[i].end()) {
    bpf_map_delete_elem(fd, &tid);
    return;
  }

  MapTaskSem v{};
  v.flags = (it->second.commit ? 1U : 0U) |
            (it->second.holder_count > 0 ? 2U : 0U);
  v.holder_count = it->second.holder_count;
  v.ts_ns = now_ns();
  bpf_map_update_elem(fd, &tid, &v, BPF_ANY);
}

void publish_all_task_state() {
  for (size_t i = 0; i < s_task_state.kCount; ++i) {
    std::vector<int32_t> tids;
    {
      std::lock_guard<std::mutex> lock(s_task_state.mu[i]);
      tids.reserve(s_task_state.m[i].size());
      for (const auto &kv : s_task_state.m[i]) tids.push_back(kv.first);
    }
    for (int32_t tid : tids) publish_task_sem(tid);
  }
}

void holder_count_add(int32_t holder_tid, int delta) {
  if (holder_tid < 0) return;
  const size_t i = s_task_state.idx(holder_tid);
  uint32_t count;
  {
    std::lock_guard<std::mutex> lock(s_task_state.mu[i]);
    auto it = s_task_state.m[i].find(holder_tid);
    if (it == s_task_state.m[i].end()) {
      if (delta <= 0) return;
      it = s_task_state.m[i].emplace(holder_tid, TaskState{}).first;
    }
    if (delta > 0)
      it->second.holder_count += delta;
    else if (it->second.holder_count >= static_cast<uint32_t>(-delta))
      it->second.holder_count -= static_cast<uint32_t>(-delta);
    else
      it->second.holder_count = 0;
    count = it->second.holder_count;
    if (!it->second.commit && it->second.holder_count == 0)
      s_task_state.m[i].erase(it);
  }
  publish_task_sem(holder_tid);
  (void)count;
}

void scx_publish_wait_edge_for_thd(THD *waiter, THD *holder) {
  /* The tid-level mirror exists only to feed the BPF maps; resolve the
     holder's tid only when a scheduler is actually attached (a real THD
     is then guaranteed).  Clearing stays unconditional so edges published
     before a disconnect are still repaid afterwards. */
  if (!s_connected.load(std::memory_order_acquire)) {
    (void)waiter;
    return;
  }
  int32_t waiter_tid = current_tid();
  int32_t holder_tid = thd_os_tid(holder);
  if (holder_tid < 0) return;

  uint32_t seq = static_cast<uint32_t>(s_wait_seq.fetch_add(1));
  if (!push_active_edge(waiter_tid, seq, holder_tid, 0)) return;

  int fd = s_fd_wait_edges.load(std::memory_order_acquire);
  if (fd >= 0) {
    MapWaitEdgeKey key{static_cast<uint32_t>(waiter_tid), seq};
    MapWaitEdgeValue value{static_cast<uint32_t>(holder_tid), 0, now_ns()};
    bpf_map_update_elem(fd, &key, &value, BPF_ANY);
  }
  holder_count_add(holder_tid, 1);
}

void scx_clear_wait_edge_for_thd(THD *waiter) {
  (void)waiter;

  int32_t waiter_tid = current_tid();
  ActiveEdge edge{};
  if (!pop_active_edge(waiter_tid, &edge)) return;

  int fd = s_fd_wait_edges.load(std::memory_order_acquire);
  if (fd >= 0) {
    MapWaitEdgeKey map_key{static_cast<uint32_t>(edge.waiter_tid), edge.seq};
    bpf_map_delete_elem(fd, &map_key);
  }
  holder_count_add(edge.holder_tid, -1);
}

bool scx_publish_latch_edge(std::thread::id owner, int kind) {
  if (!s_connected.load(std::memory_order_acquire)) return true;
  int32_t waiter_tid = current_tid();
  int32_t holder_tid = std_tid(owner);
  if (holder_tid < 0) return false;

  uint32_t seq = static_cast<uint32_t>(s_wait_seq.fetch_add(1));
  if (!push_active_edge(waiter_tid, seq, holder_tid,
                        static_cast<uint32_t>(kind)))
    return false;

  int fd = s_fd_wait_edges.load(std::memory_order_acquire);
  if (fd >= 0) {
    MapWaitEdgeKey key{static_cast<uint32_t>(waiter_tid), seq};
    MapWaitEdgeValue value{static_cast<uint32_t>(holder_tid),
                           static_cast<uint32_t>(kind), now_ns()};
    bpf_map_update_elem(fd, &key, &value, BPF_ANY);
  }
  holder_count_add(holder_tid, 1);
  return true;
}

void scx_clear_latch_edge() {
  int32_t waiter_tid = current_tid();
  ActiveEdge edge{};
  if (!pop_active_edge(waiter_tid, &edge)) return;

  int fd = s_fd_wait_edges.load(std::memory_order_acquire);
  if (fd >= 0) {
    MapWaitEdgeKey map_key{static_cast<uint32_t>(edge.waiter_tid), edge.seq};
    bpf_map_delete_elem(fd, &map_key);
  }
  holder_count_add(edge.holder_tid, -1);
}

void control_thread_main() {
  constexpr auto kProbePeriod = std::chrono::milliseconds(200);
  while (true) {
    std::this_thread::sleep_for(kProbePeriod);

    {
      std::ifstream state("/sys/kernel/sched_ext/state");
      std::string v;
      if (!(state >> v) || v != "enabled") {
        if (s_connected.exchange(false)) {
          s_fd_task_sem.store(-1, std::memory_order_release);
          s_fd_wait_edges.store(-1, std::memory_order_release);
        }
        continue;
      }
    }

    int task_fd = open_map(kMapTaskSem);
    int edge_fd = open_map(kMapWaitEdges);
    if (task_fd < 0 || edge_fd < 0) {
      if (task_fd >= 0) close(task_fd);
      if (edge_fd >= 0) close(edge_fd);
      s_connected.store(false, std::memory_order_release);
      continue;
    }

    s_fd_task_sem.store(task_fd, std::memory_order_release);
    s_fd_wait_edges.store(edge_fd, std::memory_order_release);
    if (!s_connected.exchange(true)) publish_all_task_state();
  }
}

}  // namespace

bool connected() { return s_connected.load(std::memory_order_acquire); }

void thread_start() {
  const int32_t tid = current_tid();
  {
    const my_thread_t self = my_thread_self();
    const size_t i = s_tid_registry.idx(self);
    std::lock_guard<std::mutex> lock(s_tid_registry.mu[i]);
    s_tid_registry.m[i][self] = tid;
  }
  {
    const std::thread::id sid = std::this_thread::get_id();
    const size_t i = s_std_tid_registry.idx(sid);
    std::lock_guard<std::mutex> lock(s_std_tid_registry.mu[i]);
    s_std_tid_registry.m[i][sid] = tid;
  }
}

void thread_end() {
  const int32_t tid = current_tid();

  while (t_active_edge_count > 0) {
    ActiveEdge edge{};
    if (!pop_active_edge(tid, &edge)) break;
    int fd = s_fd_wait_edges.load(std::memory_order_acquire);
    if (fd >= 0) {
      MapWaitEdgeKey map_key{static_cast<uint32_t>(edge.waiter_tid),
                             edge.seq};
      bpf_map_delete_elem(fd, &map_key);
    }
    holder_count_add(edge.holder_tid, -1);
  }

  {
    const my_thread_t self = my_thread_self();
    const size_t i = s_tid_registry.idx(self);
    std::lock_guard<std::mutex> lock(s_tid_registry.mu[i]);
    s_tid_registry.m[i].erase(self);
  }
  {
    const std::thread::id sid = std::this_thread::get_id();
    const size_t i = s_std_tid_registry.idx(sid);
    std::lock_guard<std::mutex> lock(s_std_tid_registry.mu[i]);
    s_std_tid_registry.m[i].erase(sid);
  }
  {
    const size_t i = s_task_state.idx(tid);
    std::lock_guard<std::mutex> lock(s_task_state.mu[i]);
    s_task_state.m[i].erase(tid);
  }
  int fd = s_fd_task_sem.load(std::memory_order_acquire);
  if (fd >= 0) bpf_map_delete_elem(fd, &tid);
}

void commit_begin() {
  int32_t tid = current_tid();
  {
    const size_t i = s_task_state.idx(tid);
    std::lock_guard<std::mutex> lock(s_task_state.mu[i]);
    s_task_state.m[i][tid].commit = true;
  }
  publish_task_sem(tid);
}

void commit_end() {
  int32_t tid = current_tid();
  {
    const size_t i = s_task_state.idx(tid);
    std::lock_guard<std::mutex> lock(s_task_state.mu[i]);
    auto it = s_task_state.m[i].find(tid);
    if (it == s_task_state.m[i].end()) return;
    it->second.commit = false;
    if (it->second.holder_count == 0) s_task_state.m[i].erase(it);
  }
  publish_task_sem(tid);
}

void init() {
  static std::atomic<bool> started{false};
  if (!started.exchange(true)) std::thread(control_thread_main).detach();
}

}  // namespace sched_ext

#else

namespace sched_ext {

bool connected() { return false; }

void thread_start() {}
void thread_end() {}
void commit_begin() {}
void commit_end() {}
void init() {}

}  // namespace sched_ext

#endif

extern "C" void mysql_sched_register_thread(void) {
  sched_ext::thread_start();
}
