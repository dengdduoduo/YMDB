// Portions Copyright (c) YugabyteDB, Inc.

#include <array>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <map>
#include <limits>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "query/executor/binding_iter/leapfrog_join.h"
#include "query/executor/binding_iter/scan_ranges/assigned_var.h"
#include "query/optimizer/plan/join_order/leapfrog_estimator.h"
#include "yb/mdb/storage/leapfrog_docdb_iter.h"
#include "yb/util/test_macros.h"

namespace yb::mdb {

TEST(LeapfrogEstimatorTest, InitialErrorNeverLooksEmpty) {
  bool interrupted = false;
  std::vector<std::unique_ptr<LeapfrogIter>> scans;
  scans.push_back(
      std::make_unique<LeapfrogDocDBIter>(
          &interrupted, std::vector<std::unique_ptr<ScanRange>>{}, std::vector<VarId>{VarId(0)},
          std::vector<VarId>{VarId(1), VarId(2)}, STATUS(IOError, "injected estimator error")));
  LeapfrogEstimator estimator(VarId(0), std::move(scans));
  ASSERT_LT(estimator.estimate_var_cost(), 0);
  ASSERT_TRUE(ResultToStatus(estimator.EstimateVarCost(3)).IsIOError());
}

namespace {

enum class Fault { kNone, kOpen, kDown, kSeek, kNext, kCounts, kCancelSeek };

class SampleIter : public LeapfrogIter {
 public:
  explicit SampleIter(bool* interrupted, Fault fault = Fault::kNone)
      : LeapfrogIter(interrupted, {}, {VarId(0)}, {VarId(7)}), fault_(fault) {}

  void up() override {}
  void down() override { Fail(Fault::kDown); }
  bool open_terms(Binding& binding) override {
    ++opens;
    binding_size = binding.size;
    position = 0;
    Fail(Fault::kOpen);
    return opened;
  }
  Result<bool> open_for_estimation(Binding& binding) override { return open_terms(binding); }
  bool next() override {
    ++nexts;
    Fail(Fault::kNext);
    return ++position < limit;
  }
  bool seek(uint64_t key) override {
    ++seeks;
    Fail(Fault::kSeek);
    if (fault_ == Fault::kCancelSeek) *interruption_requested = true;
    position = std::max(position, key);
    return position < limit;
  }
  uint64_t get_key() const override { return position; }
  void begin_enumeration() override {}
  void reset_enumeration() override {}
  bool next_enumeration(Binding&) override { return false; }
  std::string get_iter_name() const override { return "SampleIter"; }
  bool try_estimate(std::vector<double>& initial, std::vector<double>& remaining) const override {
    if (fault_ == Fault::kCounts) {
      const_cast<SampleIter*>(this)->set_status(STATUS(IOError, "injected sampling error"));
    }
    initial = before;
    remaining = after;
    return counts_available;
  }

  size_t opens = 0, seeks = 0, nexts = 0, binding_size = 0;
  uint64_t position = 0, limit = 10000;
  bool opened = true, counts_available = true;
  std::vector<double> before{10000}, after{9000};

 private:
  void Fail(Fault at) {
    if (fault_ == at) set_status(STATUS(IOError, "injected sampling error"));
  }
  const Fault fault_;
};

class UnknownIter : public SampleIter {
 public:
  explicit UnknownIter(bool* interrupted) : SampleIter(interrupted) {}
  Result<bool> open_for_estimation(Binding& binding) override {
    return LeapfrogIter::open_for_estimation(binding);
  }
};

std::unique_ptr<LeapfrogDocDBIter> Cached(bool* interrupted, size_t size) {
  auto iter = std::make_unique<LeapfrogDocDBIter>(
      interrupted, std::vector<std::unique_ptr<ScanRange>>{}, std::vector<VarId>{VarId(3)},
      std::vector<VarId>{VarId(5), VarId(7)});
  for (size_t i = 0; i < size; ++i) iter->range_cache().InsertRow(i, i, i);
  return iter;
}

}  // namespace

TEST(LeapfrogEstimatorTest, CacheSamplingSizesBindingAndRestartsBudget) {
  bool interrupted = false;
  auto cached = Cached(&interrupted, 2000);
  auto* cache = cached.get();
  std::vector<std::unique_ptr<LeapfrogIter>> scans;
  scans.push_back(std::move(cached));
  LeapfrogEstimator estimator(VarId(3), std::move(scans));
  for (int repeat = 0; repeat < 2; ++repeat) {
    const auto result = estimator.EstimateVarCost(8);
    ASSERT_OK(result);
    ASSERT_DOUBLE_EQ(*result, 2000);
    ASSERT_EQ(cache->get_key(), 1000);
    ASSERT_EQ(cache->read_cache().Size(), 2000);
    ASSERT_OK(cache->status());
  }
}

TEST(LeapfrogEstimatorTest, CacheOnlyOpenBypassesVirtualLoader) {
  class Loader : public LeapfrogDocDBIter {
   public:
    explicit Loader(bool* interrupted)
        : LeapfrogDocDBIter(interrupted, {}, {VarId(0)}, {VarId(1), VarId(2)}) {}
    bool open_terms(Binding&) override {
      ++loads;
      return false;
    }
    int loads = 0;
  };
  bool interrupted = false;
  auto loader = std::make_unique<Loader>(&interrupted);
  auto* observed = loader.get();
  loader->range_cache().InsertRow(1, 2, 3);
  std::vector<std::unique_ptr<LeapfrogIter>> scans;
  scans.push_back(std::move(loader));
  LeapfrogEstimator estimator(VarId(0), std::move(scans));
  const auto result = estimator.EstimateVarCost(3);
  ASSERT_OK(result);
  ASSERT_DOUBLE_EQ(*result, 0);
  ASSERT_EQ(observed->loads, 0);
}

TEST(LeapfrogEstimatorTest, UnsupportedSamplingNeverOpensLoader) {
  bool interrupted = false;
  auto unknown = std::make_unique<UnknownIter>(&interrupted);
  auto* observed = unknown.get();
  std::vector<std::unique_ptr<LeapfrogIter>> scans;
  scans.push_back(std::move(unknown));
  LeapfrogEstimator estimator(VarId(0), std::move(scans));
  ASSERT_TRUE(ResultToStatus(estimator.EstimateVarCost(8)).IsNotSupported());
  ASSERT_EQ(observed->opens, 0);
  ASSERT_OK(observed->status());
  ASSERT_LT(estimator.estimate_var_cost(), 0);
}

TEST(LeapfrogEstimatorTest, OperationErrorsSurviveTrueAndFalseReturns) {
  for (const auto fault :
       {Fault::kOpen, Fault::kDown, Fault::kSeek, Fault::kNext, Fault::kCounts}) {
    for (const bool true_return : {false, true}) {
      bool interrupted = false;
      auto iter = std::make_unique<SampleIter>(&interrupted, fault);
      iter->opened = fault != Fault::kOpen || true_return;
      if (!true_return && fault == Fault::kSeek) iter->limit = 0;
      if (!true_return && fault == Fault::kNext) iter->limit = 1;
      std::vector<std::unique_ptr<LeapfrogIter>> scans;
      scans.push_back(std::move(iter));
      LeapfrogEstimator estimator(VarId(0), std::move(scans));
      const auto result = estimator.EstimateVarCost(8);
      ASSERT_FALSE(result.ok()) << static_cast<int>(fault);
      ASSERT_TRUE(result.status().IsIOError()) << result.status();
      ASSERT_NE(result.status().ToString().find("injected sampling error"), std::string::npos);
    }
  }
}

TEST(LeapfrogEstimatorTest, EmptyAndUnknownInputsDoNotHideLaterErrors) {
  for (const bool unknown_first : {false, true}) {
    bool interrupted = false;
    std::vector<std::unique_ptr<LeapfrogIter>> scans;
    if (unknown_first) {
      scans.push_back(std::make_unique<UnknownIter>(&interrupted));
    } else {
      auto empty = std::make_unique<SampleIter>(&interrupted);
      empty->opened = false;
      scans.push_back(std::move(empty));
    }
    scans.push_back(std::make_unique<SampleIter>(&interrupted, Fault::kOpen));
    LeapfrogEstimator estimator(VarId(0), std::move(scans));
    ASSERT_TRUE(ResultToStatus(estimator.EstimateVarCost(8)).IsIOError());
  }
}

TEST(LeapfrogEstimatorTest, CancellationBeforeAndDuringSampling) {
  for (const bool before : {false, true}) {
    bool interrupted = before;
    std::vector<std::unique_ptr<LeapfrogIter>> scans;
    scans.push_back(std::make_unique<SampleIter>(&interrupted, Fault::kCancelSeek));
    LeapfrogEstimator estimator(VarId(0), std::move(scans));
    ASSERT_TRUE(ResultToStatus(estimator.EstimateVarCost(8)).IsAborted());
  }
}

TEST(LeapfrogEstimatorTest, MalformedCountsAreRejected) {
  const double nan = std::numeric_limits<double>::quiet_NaN();
  const double inf = std::numeric_limits<double>::infinity();
  const double max = std::numeric_limits<double>::max();
  using Counts = std::pair<std::vector<double>, std::vector<double>>;
  for (const auto& counts : std::vector<Counts>{
           {{}, {}},
           {{1}, {}},
           {{1}, {1, 1}},
           {{nan}, {1}},
           {{1}, {nan}},
           {{inf}, {1}},
           {{1}, {inf}},
           {{-1}, {0}},
           {{0}, {0}},
           {{2}, {-1}},
           {{1}, {2}},
           {{max}, {0}}}) {
    bool interrupted = false;
    auto iter = std::make_unique<SampleIter>(&interrupted);
    iter->before = counts.first;
    iter->after = counts.second;
    std::vector<std::unique_ptr<LeapfrogIter>> scans;
    scans.push_back(std::move(iter));
    LeapfrogEstimator estimator(VarId(0), std::move(scans));
    ASSERT_TRUE(ResultToStatus(estimator.EstimateVarCost(8)).IsInvalidArgument());
  }
}

TEST(LeapfrogEstimatorTest, UnavailableCountsRemainUnknown) {
  bool interrupted = false;
  auto iter = std::make_unique<SampleIter>(&interrupted);
  iter->counts_available = false;
  std::vector<std::unique_ptr<LeapfrogIter>> scans;
  scans.push_back(std::move(iter));
  LeapfrogEstimator estimator(VarId(0), std::move(scans));
  ASSERT_TRUE(ResultToStatus(estimator.EstimateVarCost(8)).IsNotSupported());
}

TEST(LeapfrogEstimatorTest, CompletedEmptyAndNonemptySamplesHaveZeroHeuristicCost) {
  for (const size_t size : {0, 1, 25}) {
    bool interrupted = false;
    std::vector<std::unique_ptr<LeapfrogIter>> scans;
    scans.push_back(Cached(&interrupted, size));
    LeapfrogEstimator estimator(VarId(3), std::move(scans));
    const auto result = estimator.EstimateVarCost(8);
    ASSERT_OK(result);
    ASSERT_DOUBLE_EQ(*result, 0);
  }
}

TEST(LeapfrogEstimatorTest, InvalidInputsFailBeforeOpening) {
  bool interrupted = false;
  std::vector<std::unique_ptr<LeapfrogIter>> empty;
  LeapfrogEstimator no_inputs(VarId(0), std::move(empty));
  ASSERT_TRUE(ResultToStatus(no_inputs.EstimateVarCost(8)).IsNotSupported());
  std::vector<std::unique_ptr<LeapfrogIter>> nulls;
  nulls.push_back(nullptr);
  LeapfrogEstimator null_input(VarId(0), std::move(nulls));
  ASSERT_TRUE(ResultToStatus(null_input.EstimateVarCost(8)).IsInvalidArgument());
  for (const size_t size : {0, 1, 7}) {
    auto iter = std::make_unique<SampleIter>(&interrupted);
    auto* observed = iter.get();
    std::vector<std::unique_ptr<LeapfrogIter>> scans;
    scans.push_back(std::move(iter));
    LeapfrogEstimator estimator(VarId(0), std::move(scans));
    ASSERT_TRUE(ResultToStatus(estimator.EstimateVarCost(size)).IsInvalidArgument());
    ASSERT_EQ(observed->opens, 0);
  }
  std::vector<std::unique_ptr<LeapfrogIter>> scans;
  scans.push_back(Cached(&interrupted, 1));
  LeapfrogEstimator wrong_key(VarId(0), std::move(scans));
  ASSERT_TRUE(ResultToStatus(wrong_key.EstimateVarCost(8)).IsInvalidArgument());
}

TEST(LeapfrogEstimatorTest, MultiInputSamplingCountsAndBindingAreValid) {
  bool interrupted = false;
  std::vector<SampleIter*> observed;
  std::vector<std::unique_ptr<LeapfrogIter>> scans;
  for (int i = 0; i < 3; ++i) {
    auto iter = std::make_unique<SampleIter>(&interrupted);
    observed.push_back(iter.get());
    scans.push_back(std::move(iter));
  }
  LeapfrogEstimator estimator(VarId(0), std::move(scans));
  for (int repeat = 0; repeat < 2; ++repeat) {
    size_t seeks_before = 0, results_before = 0;
    for (const auto* iter : observed) {
      seeks_before += iter->seeks;
      results_before += iter->nexts;
    }
    const auto cost = estimator.EstimateVarCost(8);
    ASSERT_OK(cost);
    size_t seeks = 0, results = 0;
    for (const auto* iter : observed) {
      seeks += iter->seeks;
      results += iter->nexts;
      ASSERT_EQ(iter->binding_size, 8);
    }
    ASSERT_EQ(seeks - seeks_before, LeapfrogEstimator::MAX_SEEKS);
    ASSERT_GT(results - results_before, 0);
    ASSERT_DOUBLE_EQ(*cost, 10000.0 / 9001 * (results - results_before + 1));
  }
}

TEST(LeapfrogEstimatorTest, NonconstantRangesCannotReadUnboundVariables) {
  bool interrupted = false;
  std::vector<std::unique_ptr<ScanRange>> ranges;
  ranges.push_back(std::make_unique<AssignedVar>(VarId(UINT_FAST32_MAX)));
  auto iter = std::make_unique<LeapfrogDocDBIter>(
      &interrupted, std::move(ranges), std::vector<VarId>{VarId(0)}, std::vector<VarId>{VarId(1)});
  std::vector<std::unique_ptr<LeapfrogIter>> scans;
  scans.push_back(std::move(iter));
  LeapfrogEstimator estimator(VarId(0), std::move(scans));
  ASSERT_TRUE(ResultToStatus(estimator.EstimateVarCost(2)).IsNotSupported());
}

TEST(LeapfrogEstimatorTest, UnknownCountsDoNotHideLaterErrors) {
  bool interrupted = false;
  std::vector<std::unique_ptr<LeapfrogIter>> scans;
  auto first = std::make_unique<SampleIter>(&interrupted);
  first->counts_available = false;
  scans.push_back(std::move(first));
  scans.push_back(std::make_unique<SampleIter>(&interrupted, Fault::kCounts));
  LeapfrogEstimator estimator(VarId(0), std::move(scans));
  ASSERT_TRUE(ResultToStatus(estimator.EstimateVarCost(8)).IsIOError());
}

TEST(LeapfrogEstimatorTest, MaximumKeyExhaustsWithoutWrapping) {
  bool interrupted = false;
  auto iter = Cached(&interrupted, 0);
  iter->range_cache().InsertRow(UINT64_MAX, 0, 0);
  iter->range_cache().InsertRow(UINT64_MAX, 1, 0);
  std::vector<std::unique_ptr<LeapfrogIter>> scans;
  scans.push_back(std::move(iter));
  LeapfrogEstimator estimator(VarId(3), std::move(scans));
  const auto cost = estimator.EstimateVarCost(8);
  ASSERT_OK(cost);
  ASSERT_DOUBLE_EQ(*cost, 0);
}


namespace {

struct WorkCounts {
  size_t seeks = 0, nexts = 0, enumerations = 0;
};

class MeasuredCacheIter : public LeapfrogDocDBIter {
 public:
  MeasuredCacheIter(bool* interrupted, VarId output, WorkCounts* counts)
      : LeapfrogDocDBIter(interrupted, {}, {VarId(0)}, {output}), counts_(counts) {}
  bool seek(uint64_t key) override {
    ++counts_->seeks;
    return LeapfrogDocDBIter::seek(key);
  }
  bool next() override {
    ++counts_->nexts;
    return LeapfrogDocDBIter::next();
  }
  bool next_enumeration(Binding& binding) override {
    ++counts_->enumerations;
    return LeapfrogDocDBIter::next_enumeration(binding);
  }
 private:
  WorkCounts* counts_;
};

using CalibrationRelation = std::map<uint64_t, std::vector<uint64_t>>;
using CalibrationRow = std::array<uint64_t, 4>;

struct CalibrationFixture {
  const char* name;
  std::array<CalibrationRelation, 3> relations;
};

CalibrationFixture MakeCalibrationFixture(const char* name, size_t keys, int shape) {
  CalibrationFixture fixture{name, {}};
  for (size_t input = 0; input < 3; ++input) {
    for (size_t i = 0; i < keys; ++i) {
      uint64_t key = i + 1;
      if (shape == 1) key = 3 * i + input + 1;
      if (shape == 2) key += input * 10000;
      if (shape == 3) key += input * 300;
      const size_t copies = shape == 4 ? 2 : shape == 5 && i < 100 ? 5 :
          shape == 6 && i >= keys - 100 ? 5 : shape == 7 ? 8 : 1;
      for (size_t suffix = 0; suffix < copies; ++suffix) {
        fixture.relations[input][key].push_back(suffix + 1);
      }
    }
  }
  return fixture;
}

std::vector<std::unique_ptr<LeapfrogIter>> CalibrationScans(
    const CalibrationFixture& fixture, bool* interrupted, WorkCounts* counts) {
  std::vector<std::unique_ptr<LeapfrogIter>> scans;
  for (size_t input = 0; input < fixture.relations.size(); ++input) {
    auto iter = std::make_unique<MeasuredCacheIter>(
        interrupted, VarId(input + 1), counts);
    for (const auto& [key, values] : fixture.relations[input]) {
      for (const auto value : values) iter->range_cache().InsertRow(key, value, 0);
    }
    scans.push_back(std::move(iter));
  }
  return scans;
}

}  // namespace

TEST(LeapfrogEstimatorTest, CachedCostCalibrationUsesIndependentResultBags) {
  const std::vector<CalibrationFixture> fixtures{
      MakeCalibrationFixture("small_dense", 25, 0),
      MakeCalibrationFixture("dense", 1200, 0),
      MakeCalibrationFixture("sparse_disjoint", 1200, 1),
      MakeCalibrationFixture("range_disjoint", 1200, 2),
      MakeCalibrationFixture("partial_overlap", 1200, 3),
      MakeCalibrationFixture("duplicate_uniform", 1200, 4),
      MakeCalibrationFixture("skew_head", 1200, 5),
      MakeCalibrationFixture("skew_tail", 1200, 6),
      MakeCalibrationFixture("duplicate_completed", 64, 7)};
  for (const auto& fixture : fixtures) {
    SCOPED_TRACE(fixture.name);
    bool interrupted = false;
    std::vector<CalibrationRow> oracle;
    size_t intersection_keys = 0, tuples = 0;
    for (const auto& relation : fixture.relations) {
      for (const auto& [key, values] : relation) tuples += values.size();
    }
    for (const auto& [key, left] : fixture.relations[0]) {
      const auto middle = fixture.relations[1].find(key);
      const auto right = fixture.relations[2].find(key);
      if (middle == fixture.relations[1].end() || right == fixture.relations[2].end()) continue;
      ++intersection_keys;
      for (const auto a : left) {
        for (const auto b : middle->second) {
          for (const auto c : right->second) oracle.push_back({key, a, b, c});
        }
      }
    }
    std::sort(oracle.begin(), oracle.end());
    WorkCounts sample;
    LeapfrogEstimator estimator(VarId(0), CalibrationScans(fixture, &interrupted, &sample));
    const auto cost = estimator.EstimateVarCost(4);
    ASSERT_OK(cost);
    ASSERT_TRUE(std::isfinite(*cost));
    ASSERT_GE(*cost, 0);
    ASSERT_EQ(sample.enumerations, 0);
    const auto first_sample = sample;
    const auto repeated_cost = estimator.EstimateVarCost(4);
    ASSERT_OK(repeated_cost);
    ASSERT_DOUBLE_EQ(*cost, *repeated_cost);
    ASSERT_EQ(sample.seeks, 2 * first_sample.seeks);
    ASSERT_EQ(sample.nexts, 2 * first_sample.nexts);

    const auto before_work = sample;
    const auto work = estimator.SampleVarWork(4);
    ASSERT_OK(work);
    ASSERT_EQ(work->matched_keys, first_sample.nexts);
    ASSERT_EQ(work->next_calls, first_sample.nexts);
    ASSERT_EQ(sample.seeks - before_work.seeks, work->direct_seeks + work->next_calls);
    ASSERT_LE(work->direct_seeks, LeapfrogEstimator::MAX_SEEKS);
    ASSERT_EQ(work->progress.size(), 3);
    for (const auto& progress : work->progress) {
      ASSERT_EQ(progress.unit, LeapfrogIter::ProgressCountUnit::kTuples);
      ASSERT_EQ(progress.available, !work->finished);
    }

    WorkCounts execution;
    LeapfrogJoin join(CalibrationScans(fixture, &interrupted, &execution),
        {VarId(0), VarId(1), VarId(2), VarId(3)}, 1);
    Binding binding(4);
    join.begin(binding);
    std::vector<CalibrationRow> actual;
    while (join.next()) {
      actual.push_back({binding[VarId(0)].id, binding[VarId(1)].id,
                        binding[VarId(2)].id, binding[VarId(3)].id});
    }
    ASSERT_OK(join.status());
    std::sort(actual.begin(), actual.end());
    ASSERT_EQ(actual, oracle);
    ASSERT_LE(first_sample.nexts, LeapfrogEstimator::MAX_SEEKS);
    // seek includes the virtual seek invoked by next; these counters must not be added.
    std::cout << std::setprecision(17) << "CALIBRATION {\"shape\":\"" << fixture.name
        << "\",\"tuples\":" << tuples << ",\"intersection_keys\":" << intersection_keys
        << ",\"output_rows\":" << oracle.size() << ",\"legacy_cost\":" << *cost
        << ",\"sample_seeks_including_next\":" << first_sample.seeks
        << ",\"sample_nexts\":" << first_sample.nexts
        << ",\"direct_seeks\":" << work->direct_seeks
        << ",\"matched_keys\":" << work->matched_keys
        << ",\"finished\":" << (work->finished ? "true" : "false")
        << ",\"execution_seeks_including_next\":" << execution.seeks
        << ",\"execution_nexts\":" << execution.nexts
        << ",\"execution_enumerations\":" << execution.enumerations << "}" << std::endl;
  }
}


TEST(LeapfrogEstimatorTest, CompletedSampleReportsWorkAndEmptyInput) {
  for (const size_t size : {0, 25}) {
    bool interrupted = false;
    std::vector<std::unique_ptr<LeapfrogIter>> scans;
    scans.push_back(Cached(&interrupted, size));
    LeapfrogEstimator estimator(VarId(3), std::move(scans));
    const auto work = estimator.SampleVarWork(8);
    ASSERT_OK(work);
    ASSERT_TRUE(work->finished);
    ASSERT_EQ(work->has_empty_input, size == 0);
    ASSERT_EQ(work->matched_keys, size);
    ASSERT_EQ(work->direct_seeks, size);
    ASSERT_EQ(work->next_calls, size);
    ASSERT_EQ(work->progress.size(), 1);
    ASSERT_FALSE(work->progress[0].available);
    ASSERT_EQ(work->progress[0].unit, LeapfrogIter::ProgressCountUnit::kTuples);
    const auto cost = estimator.EstimateVarCost(8);
    ASSERT_OK(cost);
    ASSERT_DOUBLE_EQ(*cost, 0);
  }
}

TEST(LeapfrogEstimatorTest, BudgetedSampleSeparatesCallsAndTupleProgress) {
  const auto fixture = MakeCalibrationFixture("dense", 1200, 0);
  bool interrupted = false;
  WorkCounts counts;
  LeapfrogEstimator estimator(VarId(0), CalibrationScans(fixture, &interrupted, &counts));
  const auto work = estimator.SampleVarWork(4);
  ASSERT_OK(work);
  ASSERT_FALSE(work->finished);
  ASSERT_EQ(work->direct_seeks, LeapfrogEstimator::MAX_SEEKS);
  ASSERT_EQ(work->matched_keys, 500);
  ASSERT_EQ(work->next_calls, 500);
  ASSERT_EQ(counts.seeks, 1500);
  ASSERT_EQ(counts.enumerations, 0);
  for (const auto& progress : work->progress) {
    ASSERT_TRUE(progress.available);
    ASSERT_EQ(progress.unit, LeapfrogIter::ProgressCountUnit::kTuples);
    ASSERT_EQ(progress.initial, (std::vector<double>{1200}));
    ASSERT_GT(progress.remaining[0], 0);
    ASSERT_LT(progress.remaining[0], 1200);
  }
}

TEST(LeapfrogEstimatorTest, UnavailableProgressDoesNotDiscardObservedWork) {
  bool interrupted = false;
  auto iter = std::make_unique<SampleIter>(&interrupted);
  iter->counts_available = false;
  std::vector<std::unique_ptr<LeapfrogIter>> scans;
  scans.push_back(std::move(iter));
  LeapfrogEstimator estimator(VarId(0), std::move(scans));
  const auto work = estimator.SampleVarWork(8);
  ASSERT_OK(work);
  ASSERT_FALSE(work->finished);
  ASSERT_EQ(work->direct_seeks, LeapfrogEstimator::MAX_SEEKS);
  ASSERT_GT(work->matched_keys, 0);
  ASSERT_FALSE(work->progress[0].available);
  ASSERT_TRUE(work->progress[0].initial.empty());
  ASSERT_TRUE(ResultToStatus(estimator.EstimateVarCost(8)).IsNotSupported());
}

TEST(LeapfrogEstimatorTest, UnspecifiedProgressNeverClaimsDistinctKeys) {
  bool interrupted = false;
  std::vector<std::unique_ptr<LeapfrogIter>> scans;
  scans.push_back(std::make_unique<SampleIter>(&interrupted));
  LeapfrogEstimator estimator(VarId(0), std::move(scans));
  const auto work = estimator.SampleVarWork(8);
  ASSERT_OK(work);
  ASSERT_TRUE(work->progress[0].available);
  ASSERT_EQ(work->progress[0].unit, LeapfrogIter::ProgressCountUnit::kUnspecified);
  ASSERT_OK(estimator.EstimateVarCost(8));
}

TEST(LeapfrogEstimatorTest, StructuredSamplingPreservesFailuresAndCancellation) {
  for (const auto fault : {Fault::kOpen, Fault::kDown, Fault::kSeek,
                           Fault::kNext, Fault::kCounts, Fault::kCancelSeek}) {
    bool interrupted = false;
    std::vector<std::unique_ptr<LeapfrogIter>> scans;
    scans.push_back(std::make_unique<SampleIter>(&interrupted, fault));
    LeapfrogEstimator estimator(VarId(0), std::move(scans));
    const auto work = estimator.SampleVarWork(8);
    ASSERT_FALSE(work.ok());
    if (fault == Fault::kCancelSeek) {
      ASSERT_TRUE(work.status().IsAborted());
    } else {
      ASSERT_TRUE(work.status().IsIOError());
    }
  }
  bool interrupted = false;
  auto unknown = std::make_unique<UnknownIter>(&interrupted);
  auto* observed = unknown.get();
  std::vector<std::unique_ptr<LeapfrogIter>> scans;
  scans.push_back(std::move(unknown));
  LeapfrogEstimator estimator(VarId(0), std::move(scans));
  ASSERT_TRUE(ResultToStatus(estimator.SampleVarWork(8)).IsNotSupported());
  ASSERT_EQ(observed->opens, 0);
}

}  // namespace yb::mdb
