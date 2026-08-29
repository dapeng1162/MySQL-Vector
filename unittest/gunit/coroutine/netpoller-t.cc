#include <gtest/gtest.h>

#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <atomic>
#include <chrono>
#include <thread>

#include "coroutine/task.h"
#include "coroutine/netpoller.h"

namespace coroutine {
namespace {

// -------------------------------------------------------------------
// Helper: a test reactor that runs Netpoller on a background thread
// and can schedule coroutines on it.
// -------------------------------------------------------------------
class ReactorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    m_thread = std::thread([this] { m_poller.run(); });
  }

  void TearDown() override {
    m_poller.stop();
    if (m_thread.joinable()) m_thread.join();
  }

  Netpoller m_poller;
  std::thread m_thread;
};

// -------------------------------------------------------------------
// Helper: create a connected socketpair (fd[0] ↔ fd[1]).
// -------------------------------------------------------------------
static int make_socketpair(int fd[2]) {
  int rc = socketpair(AF_UNIX, SOCK_STREAM, 0, fd);
  if (rc == 0) {
    for (int i = 0; i < 2; ++i) {
      int flags = fcntl(fd[i], F_GETFL, 0);
      fcntl(fd[i], F_SETFL, flags | O_NONBLOCK);
    }
  }
  return rc;
}

// -------------------------------------------------------------------
// A self-owned coroutine runner.  Each test implements a small struct
// with a `run()` member function that is a coroutine returning
// DetachedTask, and a `done` atomic flag.  The struct is held by
// shared_ptr so it outlives the coroutine frame.
// -------------------------------------------------------------------
template <typename T>
static void spawn_owned(Netpoller *poller) {
  auto owner = std::make_shared<T>();
  owner->poller = poller;
  poller->post([owner] { owner->run(); });
}

// ===================================================================
// ReadableWakesCoroutine
// ===================================================================

struct ReadableTest {
  Netpoller *poller{nullptr};
  int fd{-1};
  std::atomic<bool> done{false};
  char buf[32];
  std::string result;

  DetachedTask run() {
    bool ok = co_await poller->wait_readable(fd);
    EXPECT_TRUE(ok);
    ssize_t n = read(fd, buf, sizeof(buf));
    EXPECT_EQ(n, 4);
    result.assign(buf, 4);
    done.store(true);
  }
};

TEST_F(ReactorTest, ReadableWakesCoroutine) {
  int fds[2];
  ASSERT_EQ(0, make_socketpair(fds));

  auto t = std::make_shared<ReadableTest>();
  t->poller = &m_poller;
  t->fd = fds[0];
  m_poller.post([t] { t->run(); });

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  ASSERT_EQ(4, write(fds[1], "ping", 4));

  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::seconds(5);
  while (!t->done.load() &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  EXPECT_TRUE(t->done.load());
  EXPECT_EQ(t->result, "ping");

  close(fds[0]);
  close(fds[1]);
}

// ===================================================================
// RepeatedReadWrite — two consecutive reads, testing ONESHOT re-arm
// ===================================================================

struct RepeatedReadTest {
  Netpoller *poller{nullptr};
  int fd{-1};
  std::atomic<bool> done{false};
  char buf[32];
  std::string first, second;

  DetachedTask run() {
    // First read
    bool ok = co_await poller->wait_readable(fd);
    EXPECT_TRUE(ok);
    ssize_t n = read(fd, buf, sizeof(buf));
    EXPECT_EQ(n, 5);
    first.assign(buf, 5);

    // Second read (re-arm via new wait_readable)
    ok = co_await poller->wait_readable(fd);
    EXPECT_TRUE(ok);
    n = read(fd, buf, sizeof(buf));
    EXPECT_EQ(n, 5);
    second.assign(buf, 5);

    done.store(true);
  }
};

TEST_F(ReactorTest, RepeatedReadWrite) {
  int fds[2];
  ASSERT_EQ(0, make_socketpair(fds));

  auto t = std::make_shared<RepeatedReadTest>();
  t->poller = &m_poller;
  t->fd = fds[0];
  m_poller.post([t] { t->run(); });

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  ASSERT_EQ(5, write(fds[1], "hello", 5));
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  ASSERT_EQ(5, write(fds[1], "world", 5));

  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::seconds(5);
  while (!t->done.load() &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  EXPECT_TRUE(t->done.load());
  EXPECT_EQ(t->first, "hello");
  EXPECT_EQ(t->second, "world");

  close(fds[0]);
  close(fds[1]);
}

// ===================================================================
// TimerSleep
// ===================================================================

struct TimerTest {
  Netpoller *poller{nullptr};
  std::atomic<bool> done{false};

  DetachedTask run() {
    co_await poller->sleep_for(std::chrono::milliseconds(50));
    done.store(true);
  }
};

TEST_F(ReactorTest, TimerSleep) {
  auto t = std::make_shared<TimerTest>();
  t->poller = &m_poller;
  m_poller.post([t] { t->run(); });

  auto start = std::chrono::steady_clock::now();
  auto deadline = start + std::chrono::seconds(5);
  while (!t->done.load() &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - start);

  EXPECT_TRUE(t->done.load());
  EXPECT_GE(elapsed.count(), 30);
}

// ===================================================================
// CancelFd
// ===================================================================

struct CancelTest {
  Netpoller *poller{nullptr};
  int fd{-1};
  std::atomic<bool> done{false};
  bool awaiter_result{true};

  DetachedTask run() {
    awaiter_result = co_await poller->wait_readable(fd);
    done.store(true);
  }
};

TEST_F(ReactorTest, CancelFd) {
  int fds[2];
  ASSERT_EQ(0, make_socketpair(fds));

  auto t = std::make_shared<CancelTest>();
  t->poller = &m_poller;
  t->fd = fds[0];
  m_poller.post([t] { t->run(); });

  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // Cancel the fd from the reactor thread.
  m_poller.post([this, fds] { m_poller.cancel_fd(fds[0]); });

  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::seconds(5);
  while (!t->done.load() &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  EXPECT_TRUE(t->done.load());
  EXPECT_FALSE(t->awaiter_result);

  close(fds[0]);
  close(fds[1]);
}

// ===================================================================
// PostRunsOnReactorThread
// ===================================================================

TEST_F(ReactorTest, PostRunsOnReactorThread) {
  std::atomic<bool> invoked{false};
  std::thread::id reactor_tid;

  m_poller.post([&] {
    reactor_tid = std::this_thread::get_id();
    invoked.store(true);
  });

  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::seconds(5);
  while (!invoked.load() &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  EXPECT_TRUE(invoked.load());
  EXPECT_EQ(reactor_tid, m_thread.get_id());
}

// ===================================================================
// WritableWakesCoroutine
// ===================================================================

struct WritableTest {
  Netpoller *poller{nullptr};
  int fd{-1};
  std::atomic<bool> done{false};
  bool ok{false};

  DetachedTask run() {
    ok = co_await poller->wait_writable(fd);
    done.store(true);
  }
};

TEST_F(ReactorTest, WritableWakesCoroutine) {
  int fds[2];
  ASSERT_EQ(0, make_socketpair(fds));

  auto t = std::make_shared<WritableTest>();
  t->poller = &m_poller;
  t->fd = fds[0];
  m_poller.post([t] { t->run(); });

  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::seconds(5);
  while (!t->done.load() &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  EXPECT_TRUE(t->done.load());
  EXPECT_TRUE(t->ok);

  close(fds[0]);
  close(fds[1]);
}

}  // namespace
}  // namespace coroutine