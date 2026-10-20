// Copyright (c) YugabyteDB, Inc.
#pragma once

#include <atomic>
#include <google/protobuf/message_lite.h>

#include "yb/util/monotime.h"
#include "yb/util/status.h"

namespace yb::mdb {

constexpr uint32_t kFragmentVersion = 1;
constexpr size_t kFragmentMaxFrame = 1024 * 1024;

// Four-byte network-order length followed by one bounded protobuf message.
// Pipes are nonblocking. Deadlines and cancellation include partial frames.
Status SetFragmentNonblocking(int fd);
Status WriteFragmentFrame(int fd, const google::protobuf::MessageLite& message,
                          CoarseTimePoint deadline, const std::atomic<bool>* cancelled = nullptr);
Status ReadFragmentFrame(int fd, google::protobuf::MessageLite* message,
                         CoarseTimePoint deadline, const std::atomic<bool>* cancelled = nullptr);

}  // namespace yb::mdb
