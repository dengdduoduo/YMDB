// Copyright (c) YugabyteDB, Inc.
#pragma once

#include <chrono>
#include <cstdint>

namespace yb::mdb {
inline uint64_t WallMicros() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count();
}

// Nested counters describe elapsed wall time, not mutually exclusive CPU time.
class WallTimeScope {
 public:
  explicit WallTimeScope(uint64_t* counter) : counter_(counter), start_(WallMicros()) {}
  ~WallTimeScope() { if (counter_) *counter_ += WallMicros() - start_; }
  WallTimeScope(const WallTimeScope&) = delete;
  WallTimeScope& operator=(const WallTimeScope&) = delete;
 private:
  uint64_t* counter_;
  uint64_t start_;
};
}  // namespace yb::mdb
