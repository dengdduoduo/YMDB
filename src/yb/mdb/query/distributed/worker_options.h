// Copyright (c) YugabyteDB, Inc.
#pragma once

#include <cstddef>
#include <string>
#include <vector>

namespace yb::mdb {

struct WorkerOptions {
  // "local" runs an isolated process for single-host deployments and tests.
  // Other targets are SSH destinations, using existing host and user authentication.
  std::vector<std::string> targets;
  std::string binary;
  // Optional remote-local flagfile for the existing YB client's TLS settings.
  std::string flagfile;
  std::string ssh_config;
  size_t max_workers = 3;
  size_t batch_rows = 128;
  size_t page_rows = 16;
  size_t memory_bytes = 8 * 1024 * 1024;
  // Limits retained Gather input, including conservative per-row operator slack.
  // This is not a total process RSS limit. P8 will add external-memory execution.
  size_t coordinator_memory_bytes = 64 * 1024 * 1024;
};

}  // namespace yb::mdb
