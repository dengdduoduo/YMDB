// Copyright (c) YugabyteDB, Inc.
#include "yb/mdb/query/distributed/fragment_transport.h"

#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
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
