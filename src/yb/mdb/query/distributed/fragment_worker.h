// Copyright (c) YugabyteDB, Inc.
#pragma once

#include "yb/util/status.h"

namespace yb::mdb {
// Legacy transports own one fragment. Opt-in v3 requires a release ACK between leases.
Status RunFragmentWorker(int input_fd, int output_fd, bool allow_reuse = false);
}  // namespace yb::mdb
