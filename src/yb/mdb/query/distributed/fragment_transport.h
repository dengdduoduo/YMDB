// Copyright (c) YugabyteDB, Inc.
#pragma once

#include <array>
#include <atomic>
#include <memory>
#include <google/protobuf/message_lite.h>

#include "yb/util/monotime.h"
#include "yb/util/status.h"

namespace yb::mdb {

constexpr uint32_t kFragmentVersion = 1;
constexpr size_t kFragmentMaxFrame = 1024 * 1024;

// Four-byte network-order length followed by one bounded protobuf message.
// Pipes are nonblocking. Deadlines and cancellation include partial frames.
// Retains at most one frame payload per stream, including partial headers/body.
// ReadAvailable never waits for more bytes; the caller multiplexes readable fds.
class FragmentFrameReader {
 public:
  Status ReadAvailable(int fd, google::protobuf::MessageLite* message, bool* complete,
                       CoarseTimePoint deadline, const std::atomic<bool>* cancelled = nullptr);
 private:
  std::array<char, 4> header_;
  size_t header_used_ = 0;
  std::unique_ptr<char[]> payload_;
  size_t payload_size_ = 0;
  size_t payload_used_ = 0;
};

Status SetFragmentNonblocking(int fd);
Status WriteFragmentFrame(int fd, const google::protobuf::MessageLite& message,
                          CoarseTimePoint deadline, const std::atomic<bool>* cancelled = nullptr);
Status ReadFragmentFrame(int fd, google::protobuf::MessageLite* message,
                         CoarseTimePoint deadline, const std::atomic<bool>* cancelled = nullptr);

}  // namespace yb::mdb
