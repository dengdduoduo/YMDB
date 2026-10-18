// Copyright (c) YugabyteDB, Inc.

#pragma once

#include <atomic>
#include <memory>

#include "yb/client/client_fwd.h"
#include "yb/util/monotime.h"
#include "yb/util/result.h"

namespace yb::mdb {

// Owned by one query, activated explicitly for legacy synchronous operators.
// The transaction manager and client must outlive this object. Sessions are
// never shared concurrently, and read restarts fail the query rather than
// mixing rows from two views.
class DocDBReadContext {
 public:
  static Result<std::unique_ptr<DocDBReadContext>> Create(
      client::YBClient* client, client::TransactionManager* manager);
  ~DocDBReadContext();

  client::YBSessionPtr session;
  std::atomic<bool> cancelled{false};
  CoarseTimePoint deadline = CoarseMonoClock::Now() + MonoDelta::FromSeconds(30);
  size_t page_rows = 256;
  size_t page_bytes = 64 * 1024;
  uint64_t scan_rpc_calls = 0;

 private:
  DocDBReadContext() = default;
  client::YBTransactionPtr transaction_;
};

class DocDBReadContextScope {
 public:
  explicit DocDBReadContextScope(DocDBReadContext& context);
  ~DocDBReadContextScope();
  DocDBReadContextScope(const DocDBReadContextScope&) = delete;
  DocDBReadContextScope& operator=(const DocDBReadContextScope&) = delete;
  static DocDBReadContext* Current();

 private:
  DocDBReadContext* previous_;
};

}  // namespace yb::mdb
