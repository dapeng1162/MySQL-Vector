#include <gtest/gtest.h>

#include <stdexcept>

#include "coroutine/task.h"

namespace coroutine {
namespace {

bool g_flag = false;

Task running_task() {
  g_flag = true;
  co_return;
}

Task suspending_task() {
  co_await std::suspend_always{};
  g_flag = true;
  co_return;
}

Task throwing_task() {
  throw std::runtime_error("boom");
  co_return;
}

DetachedTask detached_flag(bool *flag) {
  *flag = true;
  co_return;
}

TEST(TaskTest, RunsOnStart) {
  g_flag = false;
  auto t = running_task();
  // Not started yet: body has not run.
  EXPECT_FALSE(g_flag);
  t.start();
  EXPECT_TRUE(g_flag);
}

TEST(TaskTest, SuspendingTaskStartsAndCompletes) {
  g_flag = false;
  auto t = suspending_task();
  t.start();
  // The inner co_await suspend_always leaves the coroutine suspended at
  // that point on the same thread; nothing resumes it, so g_flag stays
  // false.  The Task destructor destroys the frame.
  EXPECT_FALSE(g_flag);
}

TEST(TaskTest, ExceptionIsStoredAndRethrown) {
  auto t = throwing_task();
  t.start();
  EXPECT_THROW(t.rethrow_if_exception(), std::runtime_error);
}

TEST(TaskTest, DetachedRunsImmediately) {
  bool ran = false;
  auto d = detached_flag(&ran);
  // DetachedTask starts on creation (initial_suspend = suspend_never).
  EXPECT_TRUE(ran);
  (void)d;
}

TEST(TaskTest, MoveOwnershipTransfers) {
  auto t1 = throwing_task();
  Task t2 = std::move(t1);
  EXPECT_FALSE(static_cast<bool>(t1));
  EXPECT_TRUE(static_cast<bool>(t2));
  t2.start();
  EXPECT_THROW(t2.rethrow_if_exception(), std::runtime_error);
}

}  // namespace
}  // namespace coroutine