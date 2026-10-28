// Copyright (c) YugabyteDB, Inc.
#include "yb/mdb/query/distributed/fragment_transport.h"

#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>

namespace yb::mdb {
namespace {
Status Transfer(int fd, char* data, size_t size, bool writing,
                CoarseTimePoint deadline, const std::atomic<bool>* cancelled) {
  while (size) {
    if (cancelled && cancelled->load(std::memory_order_relaxed))
      return STATUS(Aborted, "MDB fragment cancelled");
    if (CoarseMonoClock::Now() >= deadline)
      return STATUS(TimedOut, "MDB fragment deadline expired");
    pollfd ready{fd, static_cast<short>(writing ? POLLOUT : POLLIN), 0};
    const auto result = poll(&ready, 1, 20);
    if (result < 0 && errno == EINTR) continue;
    if (result < 0) return STATUS(IOError, "MDB fragment pipe poll failed");
    if (!result) continue;
    const auto count = writing ? write(fd, data, size) : read(fd, data, size);
    if (count < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)) continue;
    if (count <= 0) return STATUS(NetworkError, "MDB worker disconnected or frame truncated");
    data += count;
    size -= count;
  }
  return Status::OK();
}
}  // namespace

Status SetFragmentNonblocking(int fd) {
  const int flags = fcntl(fd, F_GETFL);
  if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
    return STATUS(IOError, "Cannot configure MDB worker pipe");
  return Status::OK();
}

Status WriteFragmentFrame(int fd, const google::protobuf::MessageLite& message,
                         CoarseTimePoint deadline, const std::atomic<bool>* cancelled) {
  const auto size = message.ByteSizeLong();
  if (!size || size > kFragmentMaxFrame) return STATUS(RuntimeError, "MDB frame exceeds resource limit");
  std::string bytes;
  if (!message.SerializeToString(&bytes)) return STATUS(Corruption, "Cannot encode MDB frame");
  uint32_t header = htonl(static_cast<uint32_t>(bytes.size()));
  RETURN_NOT_OK(Transfer(fd, reinterpret_cast<char*>(&header), sizeof(header), true, deadline, cancelled));
  return Transfer(fd, bytes.data(), bytes.size(), true, deadline, cancelled);
}

Status FragmentFrameReader::ReadAvailable(
    int fd, google::protobuf::MessageLite* message, bool* complete,
    CoarseTimePoint deadline, const std::atomic<bool>* cancelled) {
  *complete = false;
  for (;;) {
    if (cancelled && cancelled->load(std::memory_order_relaxed))
      return STATUS(Aborted, "MDB fragment cancelled");
    if (CoarseMonoClock::Now() >= deadline)
      return STATUS(TimedOut, "MDB fragment deadline expired");
    if (header_used_ == header_.size()) {
      if (!payload_) {
        uint32_t size;
        std::memcpy(&size, header_.data(), sizeof(size));
        payload_size_ = ntohl(size);
        if (!payload_size_ || payload_size_ > kFragmentMaxFrame)
          return STATUS(RuntimeError, "Invalid MDB frame size");
        payload_ = std::make_unique<char[]>(payload_size_);
      }
      if (payload_used_ == payload_size_) {
        if (!message->ParseFromArray(payload_.get(), payload_size_))
          return STATUS(Corruption, "Invalid MDB protobuf frame");
        payload_.reset();
        header_used_ = payload_size_ = payload_used_ = 0;
        *complete = true;
        return Status::OK();
      }
    }
    const bool header = header_used_ < header_.size();
    auto& used = header ? header_used_ : payload_used_;
    auto* data = header ? header_.data() : payload_.get();
    const auto size = header ? header_.size() : payload_size_;
    const auto count = read(fd, data + used, size - used);
    if (count < 0 && errno == EINTR) continue;
    if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return Status::OK();
    if (count <= 0) return STATUS(NetworkError, "MDB worker disconnected or frame truncated");
    used += count;
  }
}

Status ReadFragmentFrame(int fd, google::protobuf::MessageLite* message,
                        CoarseTimePoint deadline, const std::atomic<bool>* cancelled) {
  uint32_t header;
  RETURN_NOT_OK(Transfer(fd, reinterpret_cast<char*>(&header), sizeof(header), false, deadline, cancelled));
  const auto size = ntohl(header);
  if (!size || size > kFragmentMaxFrame) return STATUS(RuntimeError, "Invalid MDB frame size");
  std::string bytes(size, '\0');
  RETURN_NOT_OK(Transfer(fd, bytes.data(), bytes.size(), false, deadline, cancelled));
  if (!message->ParseFromString(bytes)) return STATUS(Corruption, "Invalid MDB protobuf frame");
  return Status::OK();
}
}  // namespace yb::mdb
