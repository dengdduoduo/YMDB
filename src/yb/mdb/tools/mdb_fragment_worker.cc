// Copyright (c) YugabyteDB, Inc.
#include <csignal>
#include <iostream>
#include <unistd.h>

#include "yb/mdb/query/distributed/fragment_worker.h"
#include "yb/util/flags.h"

DEFINE_NON_RUNTIME_bool(mdb_worker_reuse_protocol, false,
                       "Accept bounded sequential v3 worker leases (experimental)");

int main(int argc, char** argv) {
  yb::ParseCommandLineFlags(&argc, &argv, true);
  std::signal(SIGPIPE, SIG_IGN);
  // stdout belongs exclusively to the framed protocol.
  auto status =
      yb::mdb::RunFragmentWorker(STDIN_FILENO, STDOUT_FILENO, FLAGS_mdb_worker_reuse_protocol);
  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
  }
  return status.ok() ? 0 : 1;
}
