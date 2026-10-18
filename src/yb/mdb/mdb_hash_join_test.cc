// Portions Copyright (c) YugabyteDB, Inc.


#include <array>
#include <iostream>
#include <memory>
#include <set>
#include <sstream>
#include <vector>

#include <gtest/gtest.h>

#include "query/executor/binding_iter/hash_join_binding_iter.h"
#include "query/optimizer/plan/join/hash_join_plan.h"
#include "yb/util/flags.h"
#include "yb/util/scope_exit.h"

DECLARE_uint64(mdb_hash_join_max_build_bytes);

using yb::Status;

namespace {

using Row = std::vector<uint64_t>;
using Rows = std::vector<Row>;

class RowsIter : public BindingIter {
 public:
  RowsIter(std::vector<VarId> vars, Rows rows, size_t fail_after = SIZE_MAX, int input_var = -1)
      : vars_(std::move(vars)),
        rows_(std::move(rows)),
        fail_after_(fail_after),
        input_var_(input_var) {}

  void assign_nulls() override {
    for (auto var : vars_) binding_->add(var, ObjectId::get_null());
  }
  void print(std::ostream&, int, bool) const override {}

 protected:
  void _begin(Binding& binding) override {
    binding_ = &binding;
    index_ = 0;
  }
  void _reset() override { index_ = 0; }
  bool _next() override {
    if (index_ == fail_after_) {
      set_status(STATUS(IOError, "injected join read failure"));
      return false;
    }
    while (index_ < rows_.size()) {
      const auto& row = rows_[index_++];
      if (input_var_ >= 0 && row.front() != (*binding_)[VarId(input_var_)].id) continue;
      for (size_t i = 0; i < vars_.size(); ++i) binding_->add(vars_[i], ObjectId(row[i]));
      return true;
    }
    return false;
  }

 private:
  std::vector<VarId> vars_;
  Rows rows_;
  size_t fail_after_;
  int input_var_;
  Binding* binding_ = nullptr;
  size_t index_ = 0;
};

class RowsPlan : public Plan {
 public:
  RowsPlan(std::vector<VarId> vars, Rows rows) : vars_(std::move(vars)), rows_(std::move(rows)) {}
  std::unique_ptr<Plan> clone() const override { return std::make_unique<RowsPlan>(*this); }
  double estimate_cost() const override { return rows_.size(); }
  double estimate_output_size() const override { return rows_.size(); }
  std::set<VarId> get_vars() const override { return {vars_.begin(), vars_.end()}; }
  void set_input_vars(const std::set<VarId>&) override {}
  int relation_size() const override { return 1; }
  void print(std::ostream&, int) const override {}
  std::unique_ptr<BindingIter> get_binding_iter() const override {
    return std::make_unique<RowsIter>(vars_, rows_);
  }
  bool get_leapfrog_iter(
      std::vector<std::unique_ptr<LeapfrogIter>>&, std::vector<VarId>&,
      uint_fast32_t&) const override {
    return false;
  }

 private:
  std::vector<VarId> vars_;
  Rows rows_;
};

std::multiset<Row> Consume(BindingIter& iter, Binding& binding) {
    std::multiset<Row> rows;
    while (iter.next()) {
        Row row;
        for (size_t i = 0; i < binding.size; ++i) row.push_back(binding[VarId(i)].id);
        rows.insert(std::move(row));
    }
    return rows;
}

TEST(HashJoinTest, PlanPreservesDuplicateProductsAndClone) {
    HashJoinPlan plan(
        std::make_unique<RowsPlan>(std::vector<VarId>{VarId(0), VarId(1)},
                                   Rows{{1, 10}, {1, 10}, {2, 20}}),
        std::make_unique<RowsPlan>(std::vector<VarId>{VarId(0), VarId(2)},
                                   Rows{{1, 30}, {1, 30}, {1, 40}, {3, 50}}));
    ASSERT_EQ(plan.get_vars().size(), 3U);
    const auto cloned = plan.clone();
    for (const Plan* candidate : std::array<const Plan*, 2>{&plan, cloned.get()}) {
        auto iter = candidate->get_binding_iter();
        Binding binding(3);
        iter->begin(binding);
        const auto rows = Consume(*iter, binding);
        const std::multiset<Row> expected{
            {1, 10, 30}, {1, 10, 30}, {1, 10, 30}, {1, 10, 30},
            {1, 10, 40}, {1, 10, 40}};
        ASSERT_EQ(rows, expected);
        ASSERT_TRUE(iter->status().ok()) << iter->status();
        ASSERT_FALSE(iter->next());
    }
}

TEST(HashJoinTest, MultipleKeysAndUnboundAgreeWithCompatibilityOracle) {
    const auto null = ObjectId::NULL_ID;
    const Rows left{{1, 2, 10}, {1, 3, 11}, {null, 2, 12}, {4, null, 13}};
    const Rows right{{1, 2, 20}, {1, 3, 21}, {null, 2, 22},
                     {4, null, 23}, {null, null, 24}, {1, 2, 20}};
    HashJoinBindingIter iter(
        std::make_unique<RowsIter>(std::vector<VarId>{VarId(0), VarId(1), VarId(2)}, left),
        std::make_unique<RowsIter>(std::vector<VarId>{VarId(0), VarId(1), VarId(3)}, right),
        {VarId(0), VarId(1), VarId(2)}, {VarId(0), VarId(1), VarId(3)});
    std::multiset<Row> expected;
    for (const auto& l : left) {
        for (const auto& r : right) {
            if ((l[0] == null || r[0] == null || l[0] == r[0]) &&
                (l[1] == null || r[1] == null || l[1] == r[1])) {
                expected.insert({l[0] == null ? r[0] : l[0],
                                 l[1] == null ? r[1] : l[1], l[2], r[2]});
            }
        }
    }
    Binding binding(4);
    iter.begin(binding);
    ASSERT_EQ(Consume(iter, binding), expected);
    ASSERT_TRUE(iter.status().ok()) << iter.status();
}

TEST(HashJoinTest, CartesianEmptyAndNoMatch) {
    for (bool empty : {false, true}) {
        HashJoinPlan plan(
            std::make_unique<RowsPlan>(std::vector<VarId>{VarId(0)}, Rows{{1}, {2}}),
            std::make_unique<RowsPlan>(std::vector<VarId>{VarId(1)},
                                       empty ? Rows{} : Rows{{3}, {4}}));
        auto iter = plan.get_binding_iter();
        Binding binding(2);
        iter->begin(binding);
        ASSERT_EQ(Consume(*iter, binding).size(), empty ? 0U : 4U);
        ASSERT_TRUE(iter->status().ok()) << iter->status();
    }
    HashJoinPlan plan(
        std::make_unique<RowsPlan>(std::vector<VarId>{VarId(0)}, Rows{{1}}),
        std::make_unique<RowsPlan>(std::vector<VarId>{VarId(0)}, Rows{{2}}));
    auto iter = plan.get_binding_iter();
    Binding binding(1);
    iter->begin(binding);
    ASSERT_TRUE(Consume(*iter, binding).empty());
    ASSERT_TRUE(iter->status().ok()) << iter->status();
}

TEST(HashJoinTest, ResetRebuildsFromChangedOuterInputAndAssignsNulls) {
    HashJoinBindingIter iter(
        std::make_unique<RowsIter>(std::vector<VarId>{VarId(0)}, Rows{{1}, {2}}),
        std::make_unique<RowsIter>(std::vector<VarId>{VarId(0), VarId(1)},
                                   Rows{{1, 10}, {2, 20}}, SIZE_MAX, 2),
        {VarId(0)}, {VarId(0), VarId(1)});
    Binding binding(3);
    binding.add(VarId(2), ObjectId(1));
    iter.begin(binding);
    const std::multiset<Row> first{{1, 10, 1}};
    ASSERT_EQ(Consume(iter, binding), first);
    binding.add(VarId(2), ObjectId(2));
    iter.reset();
    const std::multiset<Row> second{{2, 20, 2}};
    ASSERT_EQ(Consume(iter, binding), second);
    ASSERT_TRUE(iter.status().ok()) << iter.status();
    iter.assign_nulls();
    ASSERT_TRUE(binding[VarId(0)].is_null());
    ASSERT_TRUE(binding[VarId(1)].is_null());
    ASSERT_EQ(binding[VarId(2)].id, 2U);
}

TEST(HashJoinTest, BuildAndProbeFailuresStayVisible) {
    for (bool build_failure : {false, true}) {
        HashJoinBindingIter iter(
            std::make_unique<RowsIter>(std::vector<VarId>{VarId(0)}, Rows{{1}, {2}},
                                       build_failure ? SIZE_MAX : 1),
            std::make_unique<RowsIter>(std::vector<VarId>{VarId(0)}, Rows{{1}, {2}},
                                       build_failure ? 1 : SIZE_MAX),
            {VarId(0)}, {VarId(0)});
        Binding binding(1);
        iter.begin(binding);
        ASSERT_EQ(Consume(iter, binding).size(), build_failure ? 0U : 1U);
        ASSERT_TRUE(iter.status().IsIOError()) << iter.status();
        iter.reset();
        ASSERT_FALSE(iter.next());
        ASSERT_TRUE(iter.status().IsIOError()) << iter.status();
    }
}

TEST(HashJoinTest, BuildLimitSurvivesPlanClone) {
    HashJoinPlan plan(
        std::make_unique<RowsPlan>(std::vector<VarId>{VarId(0)}, Rows{{1}}),
        std::make_unique<RowsPlan>(std::vector<VarId>{VarId(0)}, Rows{{1}, {2}}), 1);
    auto cloned = plan.clone();
    auto iter = cloned->get_binding_iter();
    Binding binding(1);
    iter->begin(binding);
    ASSERT_FALSE(iter->next());
    ASSERT_FALSE(iter->status().ok());
    ASSERT_NE(iter->status().ToString().find("build row limit"), std::string::npos);
}

TEST(HashJoinTest, ByteBudgetIncludesNestedContainersAndGrowth) {
    for (int shape = 0; shape < 3; ++shape) {
        SCOPED_TRACE(shape);
        std::vector<VarId> vars;
        std::set<VarId> output_vars;
        for (size_t i = 0; i < 8; ++i) {
            vars.emplace_back(i);
            output_vars.emplace(i);
        }
        Rows data;
        for (size_t i = 0; i < 128; ++i) {
            data.emplace_back(8, i + 1);
            data.back()[0] = shape == 0 ? i + 1 : shape == 1 ? 1 : ObjectId::NULL_ID;
        }
        const auto make_iter = [&](size_t limit) {
            return std::make_unique<HashJoinBindingIter>(
                std::make_unique<RowsIter>(std::vector<VarId>{VarId(0)}, Rows{{1}}),
                std::make_unique<RowsIter>(vars, data),
                std::set<VarId>{VarId(0)}, output_vars,
                HashJoinBindingIter::kDefaultMaxBuildRows, limit);
        };
        Binding binding(8);
        auto measured = make_iter(HashJoinBindingIter::kDefaultMaxBuildBytes);
        measured->begin(binding);
        ASSERT_TRUE(measured->status().ok()) << measured->status();
        const auto live = measured->build_memory();
        ASSERT_EQ(live.payload_bytes, 128U * 8 * sizeof(ObjectId));
        ASSERT_GT(live.tracked_bytes, live.payload_bytes);
        const auto expected = Consume(*measured, binding);
        ASSERT_TRUE(measured->status().ok()) << measured->status();
        ASSERT_EQ(expected.size(), shape == 0 ? 1U : 128U);
        const auto peak = measured->build_memory().peak_tracked_bytes;
        ASSERT_GE(peak, live.tracked_bytes);
        ASSERT_EQ(measured->build_memory().tracked_bytes, 0U);
        for (size_t limit : {peak, peak - 1}) {
            auto iter = make_iter(limit);
            iter->begin(binding);
            const auto actual = Consume(*iter, binding);
            const auto memory = iter->build_memory();
            ASSERT_LE(memory.peak_tracked_bytes, limit);
            ASSERT_EQ(memory.tracked_bytes, 0U);
            if (limit == peak) {
                ASSERT_TRUE(iter->status().ok()) << iter->status();
                ASSERT_EQ(actual, expected);
            } else {
                ASSERT_NE(iter->status().ToString().find("build byte limit"), std::string::npos);
                iter->reset();
                ASSERT_FALSE(iter->next());
                ASSERT_FALSE(iter->status().ok());
                ASSERT_EQ(iter->build_memory().tracked_bytes, 0U);
            }
        }
    }
}

TEST(HashJoinTest, ByteBudgetReleasedBeforeResetRebuild) {
    const auto make_iter = [](size_t limit) {
        return std::make_unique<HashJoinBindingIter>(
            std::make_unique<RowsIter>(std::vector<VarId>{VarId(0)}, Rows{{1}, {2}}),
            std::make_unique<RowsIter>(std::vector<VarId>{VarId(0), VarId(1)},
                                      Rows{{1, 10}, {2, 20}}, SIZE_MAX, 2),
            std::set<VarId>{VarId(0)}, std::set<VarId>{VarId(0), VarId(1)},
            HashJoinBindingIter::kDefaultMaxBuildRows, limit);
    };
    Binding binding(3);
    binding.add(VarId(2), ObjectId(1));
    auto measured = make_iter(HashJoinBindingIter::kDefaultMaxBuildBytes);
    measured->begin(binding);
    Consume(*measured, binding);
    ASSERT_TRUE(measured->status().ok()) << measured->status();
    const auto limit = measured->build_memory().peak_tracked_bytes;
    auto iter = make_iter(limit);
    for (uint64_t input : {1, 2, 1}) {
        binding.add(VarId(2), ObjectId(input));
        if (input == 1) {
          iter->begin(binding);
        } else {
          iter->reset();  // Rebuild while the preceding build is still live.
        }
        ASSERT_TRUE(iter->status().ok()) << iter->status();
        ASSERT_GT(iter->build_memory().tracked_bytes, 0U);
        ASSERT_LE(iter->build_memory().peak_tracked_bytes, limit);
        if (input == 2) {
            const std::multiset<Row> expected{{2, 20, 2}};
            ASSERT_EQ(Consume(*iter, binding), expected);
            ASSERT_EQ(iter->build_memory().tracked_bytes, 0U);
        }
    }
}

TEST(HashJoinTest, ProbeByteBudgetFailureReleasesBuildAndStaysVisible) {
    const auto make_iter = [](size_t limit) {
        return std::make_unique<HashJoinBindingIter>(
            std::make_unique<RowsIter>(std::vector<VarId>{VarId(0)}, Rows{{1}}),
            std::make_unique<RowsIter>(std::vector<VarId>{VarId(0), VarId(1)}, Rows(1024, {1, 2})),
            std::set<VarId>{VarId(0)}, std::set<VarId>{VarId(0), VarId(1)},
            HashJoinBindingIter::kDefaultMaxBuildRows, limit);
    };
    Binding binding(2);
    auto measured = make_iter(HashJoinBindingIter::kDefaultMaxBuildBytes);
    measured->begin(binding);
    const auto build_peak = measured->build_memory().peak_tracked_bytes;
    ASSERT_EQ(Consume(*measured, binding).size(), 1024U);
    ASSERT_TRUE(measured->status().ok()) << measured->status();
    ASSERT_GT(measured->build_memory().peak_tracked_bytes, build_peak);
    auto iter = make_iter(build_peak);
    iter->begin(binding);
    ASSERT_TRUE(iter->status().ok()) << iter->status();
    ASSERT_FALSE(iter->next());
    ASSERT_NE(iter->status().ToString().find("during probe"), std::string::npos);
    ASSERT_LE(iter->build_memory().peak_tracked_bytes, build_peak);
    ASSERT_EQ(iter->build_memory().tracked_bytes, 0U);
    iter->reset();
    ASSERT_FALSE(iter->next());
    ASSERT_FALSE(iter->status().ok());
}

TEST(HashJoinTest, ByteLimitFlagIsCapturedByPlanAndClone) {
    const auto previous = FLAGS_mdb_hash_join_max_build_bytes;
    auto restore = yb::ScopeExit([&] { FLAGS_mdb_hash_join_max_build_bytes = previous; });
    FLAGS_mdb_hash_join_max_build_bytes = 0;
    HashJoinPlan plan(
        std::make_unique<RowsPlan>(std::vector<VarId>{VarId(0)}, Rows{{1}}),
        std::make_unique<RowsPlan>(std::vector<VarId>{VarId(0)}, Rows{{1}}));
    auto clone = plan.clone();
    FLAGS_mdb_hash_join_max_build_bytes = HashJoinBindingIter::kDefaultMaxBuildBytes;
    Binding binding(1);
    for (const Plan* candidate : std::array<const Plan*, 2>{&plan, clone.get()}) {
        auto iter = candidate->get_binding_iter();
        iter->begin(binding);
        ASSERT_FALSE(iter->next());
        ASSERT_NE(iter->status().ToString().find("build byte limit"), std::string::npos);
        const auto* hash = dynamic_cast<HashJoinBindingIter*>(iter.get());
        ASSERT_EQ(hash->build_memory().byte_limit, 0U);
        ASSERT_EQ(hash->build_memory().tracked_bytes, 0U);
    }
    HashJoinPlan empty(
        std::make_unique<RowsPlan>(std::vector<VarId>{VarId(0)}, Rows{{1}}),
        std::make_unique<RowsPlan>(std::vector<VarId>{VarId(0)}, Rows{}),
        HashJoinBindingIter::kDefaultMaxBuildRows, 0);
    auto iter = empty.get_binding_iter();
    iter->begin(binding);
    ASSERT_FALSE(iter->next());
    ASSERT_TRUE(iter->status().ok()) << iter->status();
}

TEST(HashJoinTest, BuildMemoryScalesWithRowWidth) {
    size_t previous_bytes = 0;
    for (size_t width : {2U, 8U, 32U}) {
        std::vector<VarId> vars;
        std::set<VarId> output_vars;
        for (size_t i = 0; i < width; ++i) {
            vars.push_back(VarId(i));
            output_vars.insert(VarId(i));
        }
        Rows rows;
        for (uint64_t i = 0; i < 4096; ++i) rows.emplace_back(width, i + 1);
        HashJoinBindingIter iter(
            std::make_unique<RowsIter>(std::vector<VarId>{VarId(0)}, Rows{{1}}),
            std::make_unique<RowsIter>(vars, std::move(rows)), {VarId(0)}, output_vars);
        Binding binding(width);
        iter.begin(binding);
        ASSERT_TRUE(iter.status().ok()) << iter.status();
        const auto memory = iter.build_memory();
        ASSERT_EQ(memory.rows, 4096U);
        ASSERT_EQ(memory.key_buckets, 4096U);
        ASSERT_EQ(memory.payload_bytes, 4096U * width * sizeof(ObjectId));
        ASSERT_GT(memory.accounted_bytes, memory.payload_bytes);
        ASSERT_GT(memory.accounted_bytes, previous_bytes);
        previous_bytes = memory.accounted_bytes;
        std::cout << "MDB_HASH_MEMORY width=" << width << " rows=" << memory.rows
                  << " keys=" << memory.key_buckets
                  << " payload_bytes=" << memory.payload_bytes
                  << " accounted_bytes=" << memory.accounted_bytes << std::endl;
        ASSERT_EQ(Consume(iter, binding).size(), 1U);
        ASSERT_TRUE(iter.status().ok()) << iter.status();
    }
}

}  // namespace
