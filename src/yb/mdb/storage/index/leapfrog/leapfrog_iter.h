// Portions Copyright (c) YugabyteDB, Inc.

#pragma once

#include <utility>
#include <vector>

#include "yb/util/result.h"
#include "yb/util/status.h"

#include "query/executor/binding_iter/scan_ranges/scan_range.h"

class LeapfrogIter {
  friend class LeapfrogJoin;

 public:
  enum class ProgressCountUnit { kUnspecified, kTuples };

  virtual ~LeapfrogIter() = default;

  const yb::Status& status() const { return status_; }

  bool* const interruption_requested;

  virtual void up() { level--; }

  virtual void down() = 0;
  virtual bool open_terms(Binding& input_binding) = 0;
  // Sampling must never invoke a loader. Implementations opt in with a cache-only open.
  virtual yb::Result<bool> open_for_estimation(Binding&) {
    using yb::Status;
    return STATUS(NotSupported, "Cache-only Leapfrog sampling is unavailable");
  }
  virtual bool next() = 0;
  virtual bool seek(uint64_t key) = 0;
  virtual uint64_t get_key() const = 0;

  inline const std::vector<VarId>& get_intersection_vars() const { return intersection_vars; }

  inline const std::vector<VarId>& get_enumeration_vars() const { return enumeration_vars; }

  virtual void begin_enumeration() = 0;
  virtual void reset_enumeration() = 0;
  virtual bool next_enumeration(Binding&) = 0;

  const std::vector<std::unique_ptr<ScanRange>> initial_ranges;

  size_t get_relation_size() const {
    return initial_ranges.size() + intersection_vars.size() + enumeration_vars.size();
  }

  virtual std::string get_iter_name() const = 0;

  virtual ProgressCountUnit progress_count_unit() const {
    return ProgressCountUnit::kUnspecified;
  }

  virtual bool try_estimate(
      std::vector<double>& initial_estimations, std::vector<double>& after_estimations) const = 0;

 protected:
  void set_status(yb::Status status) {
    if (status_.ok() && !status.ok()) status_ = std::move(status);
  }

  const std::vector<VarId> intersection_vars;
  const std::vector<VarId> enumeration_vars;

  int_fast32_t level = -1;

  LeapfrogIter(
      bool* interruption_requested, std::vector<std::unique_ptr<ScanRange>> initial_ranges,
      std::vector<VarId> intersection_vars, std::vector<VarId> enumeration_vars)
      : interruption_requested(interruption_requested),
        initial_ranges(std::move(initial_ranges)),
        intersection_vars(std::move(intersection_vars)),
        enumeration_vars(std::move(enumeration_vars)) {}

 private:
  yb::Status status_ = yb::Status::OK();
};
