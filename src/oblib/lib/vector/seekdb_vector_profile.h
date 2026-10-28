#pragma once

/* Opt-in diagnostic wall-clock spans shared by the server and GPU bridge.
 * Use SEEKDB_VECTOR_PROFILE_LOG for a dedicated append-only file, otherwise
 * stderr. No database/logging ABI. */
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/resource.h>
#include <pthread.h>

static inline int seekdb_vprofile_enabled(void)
{
  const char *v = getenv("SEEKDB_VECTOR_PROFILE");
  return v != NULL && v[0] == '1' && v[1] == '\0';
}

static inline int64_t seekdb_vprofile_now(void)
{
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

static inline void seekdb_vprofile_emit(const char *stage, const void *key,
    int64_t start_ns, double ms, int64_t rows, int64_t dim,
    int64_t nq, int64_t topk, int rc, const char *kind, int64_t calls)
{
  if (!seekdb_vprofile_enabled()) { return; }
  struct rusage usage;
  memset(&usage, 0, sizeof(usage));
  getrusage(RUSAGE_SELF, &usage);
  const double user_ms = usage.ru_utime.tv_sec * 1000.0 + usage.ru_utime.tv_usec / 1000.0;
  const double system_ms = usage.ru_stime.tv_sec * 1000.0 + usage.ru_stime.tv_usec / 1000.0;
#ifdef __APPLE__
  const long rss_kib = usage.ru_maxrss / 1024;
#else
  const long rss_kib = usage.ru_maxrss;
#endif
  char record[1024];
  const int length = snprintf(record, sizeof(record), "[seekdb_profile] {\"version\":1,\"pid\":%ld,\"tid\":%llu,"
      "\"stage\":\"%s\",\"key\":\"%p\",\"start_ns\":%lld,\"ms\":%.6f,"
      "\"rows\":%lld,\"dim\":%lld,\"nq\":%lld,\"topk\":%lld,\"rc\":%d,"
      "\"kind\":\"%s\",\"calls\":%lld,\"process_user_ms\":%.3f,"
      "\"process_system_ms\":%.3f,\"process_peak_rss_kib\":%ld}\n",
      (long)getpid(), (unsigned long long)(uintptr_t)pthread_self(), stage, key,
      (long long)start_ns, ms, (long long)rows, (long long)dim,
      (long long)nq, (long long)topk, rc, kind, (long long)calls, user_ms, system_ms, rss_kib);
  if (length <= 0 || (size_t)length >= sizeof(record)) { return; }
  const char *path = getenv("SEEKDB_VECTOR_PROFILE_LOG");
  /* The server logger may redirect stderr and independently manage file offsets.
   * A dedicated O_APPEND file avoids both overwrite and interleaved stdio writes.
   * Open per record so no process-global descriptor lifetime leaks into the ABI. */
  int fd = path != NULL && path[0] != '\0'
      ? open(path, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0600) : STDERR_FILENO;
  if (fd < 0) { fd = STDERR_FILENO; }
  ssize_t written;
  do { written = write(fd, record, (size_t)length); } while (written < 0 && errno == EINTR);
  (void)written;
  if (fd != STDERR_FILENO) { close(fd); }
}

static inline void seekdb_vprofile_span(const char *stage, const void *key,
    int64_t start_ns, int64_t rows, int64_t dim, int64_t nq, int64_t topk, int rc)
{
  if (start_ns != 0) {
    const double ms = (seekdb_vprofile_now() - start_ns) / 1e6;
    seekdb_vprofile_emit(stage, key, start_ns, ms, rows, dim, nq, topk, rc, "span", 1);
  }
}

#ifdef __cplusplus
#include <atomic>

class VectorProfileScope {
public:
  VectorProfileScope(const char *stage, const void *key, int &rc,
      int64_t rows = 0, int64_t dim = 0, int64_t nq = 0, int64_t topk = 0)
      : stage_(stage), key_(key), rc_(rc), rows_(rows), dim_(dim), nq_(nq), topk_(topk),
        start_(seekdb_vprofile_enabled() ? seekdb_vprofile_now() : 0) {}
  ~VectorProfileScope() {
    seekdb_vprofile_span(stage_, key_, start_, rows_, dim_, nq_, topk_, rc_);
  }
  void rows(int64_t value) { rows_ = value; }
  VectorProfileScope(const VectorProfileScope &) = delete;
  VectorProfileScope &operator=(const VectorProfileScope &) = delete;
private:
  const char *stage_;
  const void *key_;
  int &rc_;
  int64_t rows_, dim_, nq_, topk_, start_;
};

/* Row-at-a-time work is accumulated, not logged for every row. Snapshots are
 * cumulative and must NOT be summed by consumers. start_ns identifies a counter
 * lifetime even when a handler address is reused. */
class VectorProfileCounter {
public:
  VectorProfileCounter(const char *stage, const void *key)
      : stage_(stage), key_(key), start_(seekdb_vprofile_enabled() ? seekdb_vprofile_now() : 0),
        ns_(0), calls_(0), rows_(0), failures_(0) {}
  ~VectorProfileCounter() { flush(); }
  void add(int64_t start, int64_t rows, int rc) {
    if (start == 0) { return; }
    ns_.fetch_add(seekdb_vprofile_now() - start, std::memory_order_relaxed);
    rows_.fetch_add(rows, std::memory_order_relaxed);
    if (rc != 0) { failures_.fetch_add(1, std::memory_order_relaxed); }
    const int64_t calls = calls_.fetch_add(1, std::memory_order_relaxed) + 1;
    if ((calls & (calls - 1)) == 0) { flush(); }
  }
  void flush() const {
    const int64_t calls = calls_.load(std::memory_order_relaxed);
    if (start_ == 0 || calls == 0) { return; }
    seekdb_vprofile_emit(stage_, key_, start_, ns_.load(std::memory_order_relaxed) / 1e6,
        rows_.load(std::memory_order_relaxed), 0, 0, 0,
        failures_.load(std::memory_order_relaxed) > 0 ? 1 : 0, "counter", calls);
  }
private:
  const char *stage_;
  const void *key_;
  const int64_t start_;
  std::atomic<int64_t> ns_, calls_, rows_, failures_;
};
#endif
