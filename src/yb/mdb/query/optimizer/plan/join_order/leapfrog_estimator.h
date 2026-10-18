// Portions Copyright (c) YugabyteDB, Inc.

#pragma once

#include <algorithm>
#include <cmath>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "query/executor/binding_iter/scan_ranges/term.h"
#include "query/query_context.h"
#include "storage/index/leapfrog/leapfrog_iter.h"

class LeapfrogEstimator {
  using Status = yb::Status;

 public:
  static constexpr size_t MAX_SEEKS = 1000;

  LeapfrogEstimator(VarId join_var, std::vector<std::unique_ptr<LeapfrogIter>>&& iters)
      : join_var_(join_var), iters_(std::move(iters)) {}

  struct InputProgress {
    LeapfrogIter::ProgressCountUnit unit = LeapfrogIter::ProgressCountUnit::kUnspecified;
    bool available = false;
    std::vector<double> initial, remaining;
  };

  struct SampledWork {
    bool finished = false;
    bool has_empty_input = false;
    size_t matched_keys = 0;
    // Dispatches by this sampler only; next may internally invoke an additional seek.
    size_t direct_seeks = 0;
    size_t next_calls = 0;
    std::vector<InputProgress> progress;
  };

  // Owns/repositions input cursors, never enumerates outputs or loads missing caches.
  // Missing progress does not invalidate observed work. Errors never publish partial work.
  yb::Result<SampledWork> SampleVarWork(size_t binding_size) {
    RETURN_NOT_OK(CheckInputs());
    if (iters_.empty()) return STATUS(NotSupported, "No Leapfrog inputs to sample");
    if (join_var_.id >= binding_size) {
      return STATUS(InvalidArgument, "Leapfrog join variable exceeds sampling binding");
    }
    for (const auto& iter : iters_) {
      const auto& keys = iter->get_intersection_vars();
      if (keys.empty() || keys.front() != join_var_) {
        return STATUS(
            InvalidArgument, "Leapfrog sampling requires the first intersection variable");
      }
      for (const auto* vars : {&keys, &iter->get_enumeration_vars()}) {
        for (const auto var : *vars) {
          if (var.id >= binding_size) {
            return STATUS(InvalidArgument, "Leapfrog variable exceeds sampling binding");
          }
        }
      }
      // Sampling has no input bindings; never read an unbound AssignedVar.
      for (const auto& range : iter->initial_ranges) {
        if (!dynamic_cast<Term*>(range.get())) {
          return STATUS(NotSupported, "Leapfrog sampling requires constant initial ranges");
        }
      }
    }

    Binding binding(binding_size);
    bool empty = false;
    bool unavailable = false;
    for (const auto& iter : iters_) {
      const auto opened = iter->open_for_estimation(binding);
      RETURN_NOT_OK(CheckInputs());
      if (!opened.ok()) {
        if (!opened.status().IsNotSupported()) return opened.status();
        unavailable = true;
      } else if (!*opened) {
        empty = true;
      } else {
        iter->down();
        RETURN_NOT_OK(CheckInputs());
      }
    }
    // Check every input before allowing an empty relation to hide a later failure.
    if (unavailable) return STATUS(NotSupported, "A Leapfrog sampling cache is unavailable");
    SampledWork sample;
    if (empty) {
      sample.finished = true;
      sample.has_empty_input = true;
    } else {
      sample = VERIFY_RESULT(Sample());
    }
    for (const auto& iter : iters_) {
      InputProgress progress;
      progress.unit = iter->progress_count_unit();
      // Exhaustion invalidates positional counts; completion already gives exact key work.
      if (!sample.finished) {
        progress.available = iter->try_estimate(progress.initial, progress.remaining);
        RETURN_NOT_OK(CheckInputs());
        if (progress.available) {
          if (progress.initial.empty() || progress.initial.size() != progress.remaining.size()) {
            return STATUS(InvalidArgument, "Invalid Leapfrog progress vector sizes");
          }
          for (size_t i = 0; i < progress.initial.size(); ++i) {
            const auto initial = progress.initial[i], remaining = progress.remaining[i];
            if (!std::isfinite(initial) || !std::isfinite(remaining) || initial <= 0 ||
                remaining < 0 || remaining > initial) {
              return STATUS(InvalidArgument, "Invalid Leapfrog progress counts");
            }
          }
        } else {
          progress.initial.clear();
          progress.remaining.clear();
        }
      }
      sample.progress.push_back(std::move(progress));
    }
    return sample;
  }

  // Compatibility heuristic, not join cardinality. Completed sampling retains zero cost.
  yb::Result<double> EstimateVarCost(size_t binding_size) {
    const auto sample = VERIFY_RESULT(SampleVarWork(binding_size));
    if (sample.finished) return 0.0;
    long double ratio_sum = 0;
    size_t counts = 0;
    for (const auto& progress : sample.progress) {
      if (!progress.available) {
        return STATUS(NotSupported, "Leapfrog progress counts are unavailable");
      }
      for (size_t i = 0; i < progress.initial.size(); ++i) {
        ratio_sum += progress.initial[i] / (1.0L + progress.remaining[i]);
        ++counts;
      }
    }
    const auto cost = ratio_sum / counts * (sample.matched_keys + 1);
    if (!std::isfinite(cost) || cost > std::numeric_limits<double>::max()) {
      return STATUS(InvalidArgument, "Leapfrog sample cost overflow");
    }
    return static_cast<double>(cost);
  }

  // Keep the inactive legacy planner's negative-cost fallback contract.
  double estimate_var_cost() {
    const auto result = EstimateVarCost(get_query_ctx().get_var_size());
    return result.ok() ? *result : -1;
  }

 private:
  yb::Status CheckInputs() const {
    for (const auto& iter : iters_) {
      if (!iter) return STATUS(InvalidArgument, "Null Leapfrog sampling iterator");
      RETURN_NOT_OK(iter->status());
      if (iter->interruption_requested && *iter->interruption_requested) {
        return STATUS(Aborted, "Leapfrog sampling interrupted");
      }
    }
    return yb::Status::OK();
  }

  yb::Result<SampledWork> Sample() {
    std::vector<LeapfrogIter*> sorted;
    for (const auto& iter : iters_) sorted.push_back(iter.get());
    std::sort(sorted.begin(), sorted.end(), [](const auto* left, const auto* right) {
      return left->get_key() < right->get_key();
    });
    size_t current = 0;
    auto maximum = sorted.back()->get_key();
    SampledWork sample;
    for (size_t seeks = 0; seeks < MAX_SEEKS; ++seeks) {
      RETURN_NOT_OK(CheckInputs());
      ++sample.direct_seeks;
      const bool found = sorted[current]->seek(maximum);
      RETURN_NOT_OK(CheckInputs());
      if (!found) {
        sample.finished = true;
        return sample;
      }
      maximum = sorted[current]->get_key();
      current = (current + 1) % sorted.size();
      if (sorted[current]->get_key() == maximum) {
        ++sample.matched_keys;
        ++sample.next_calls;
        const bool advanced = sorted[current]->next();
        RETURN_NOT_OK(CheckInputs());
        if (!advanced) {
          sample.finished = true;
          return sample;
        }
        maximum = sorted[current]->get_key();
        current = (current + 1) % sorted.size();
      }
    }
    return sample;
  }

  const VarId join_var_;
  std::vector<std::unique_ptr<LeapfrogIter>> iters_;
};
