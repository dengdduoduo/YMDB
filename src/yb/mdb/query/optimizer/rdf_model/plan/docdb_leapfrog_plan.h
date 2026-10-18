// Portions Copyright (c) YugabyteDB, Inc.

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <set>
#include <vector>

#include "query/optimizer/plan/plan.h"

namespace SPARQL {
class TriplePlan;

struct DocDBLeapfrogReuseStats {
  uint64_t hits = 0;
  uint64_t misses = 0;
  uint64_t admissions = 0;
  uint64_t rejections = 0;
  uint64_t retained_rows = 0;
  uint64_t retained_bytes = 0;
};

struct DocDBLeapfrogMemory {
  size_t private_rows = 0;
  size_t private_bytes = 0;
  size_t shared_rows = 0;
  size_t shared_bytes = 0;
};

// One sequential Scatter execution; nested scopes restore their caller's cache.
class DocDBLeapfrogQueryScope {
 public:
  DocDBLeapfrogQueryScope();
  ~DocDBLeapfrogQueryScope();
  DocDBLeapfrogQueryScope(const DocDBLeapfrogQueryScope&) = delete;
  DocDBLeapfrogQueryScope& operator=(const DocDBLeapfrogQueryScope&) = delete;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

// build_ns is contained in load_ns; load minus build includes RPC and loading overhead.
struct DocDBLeapfrogProfile {
  int64_t load_ns = 0;
  int64_t build_ns = 0;
  int64_t join_ns = 0;
  int64_t output_ns = 0;
  uint64_t loads = 0;
  uint64_t scanned_rows = 0;
  uint64_t loaded_rows = 0;
};

bool AppendDocDBLeapfrogScan(
    const TriplePlan& plan, std::vector<std::unique_ptr<LeapfrogIter>>* scans,
    const std::vector<VarId>& order, uint_fast32_t enumeration_level);

// Only the explicit opt-in, unbound three-variable predicate triangle is supported.
std::unique_ptr<BindingIter> TryBuildDocDBLeapfrogTriangle(
    const std::vector<std::unique_ptr<Plan>>& plans, const std::set<VarId>& input_vars);
}  // namespace SPARQL
