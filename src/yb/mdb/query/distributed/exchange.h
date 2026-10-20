// Copyright (c) YugabyteDB, Inc.
#pragma once

#include <atomic>
#include "yb/mdb/query/distributed/fragment_gather.h"

namespace yb::mdb {

// Explicit query ownership; independent producers never share a legacy TLS scope.
struct ExchangeControl {
  std::atomic<bool>* cancelled = nullptr;
  CoarseTimePoint deadline = CoarseMonoClock::Now() + MonoDelta::FromSeconds(30);
  BatchQueryStats* stats = nullptr;
  const CoarseTimePoint* live_deadline = nullptr;
};
using ExchangeInput = std::function<Status(size_t, const FragmentRowSink&)>;

Status ValidateExchange(const FragmentOpenPB& input, const WorkerOptions& options);
// Producers supply all bound columns of their own triple, sorted by variable ID.
// Both inputs finish before any join output. Errors never become EOF or retry.
Status ExchangeRows(const FragmentOpenPB& input, const WorkerOptions& options,
                    ExchangeControl control, const ExchangeInput& producer,
                    const FragmentRowSink& sink);
Status GatherExchangeFragments(const FragmentPlan& plan, const WorkerOptions& options,
                               DocDBReadContext& context, const FragmentRowSink& sink);

}  // namespace yb::mdb
