// Copyright (c) YugabyteDB, Inc.
#pragma once

#include "yb/util/status.h"

namespace yb::mdb {
// One authenticated transport session owns one fragment and child transaction.
Status RunFragmentWorker(int input_fd, int output_fd);
}  // namespace yb::mdb
