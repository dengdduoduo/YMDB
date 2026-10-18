// Copyright (c) YugabyteDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//

#include <gtest/gtest.h>

#include "query/optimizer/plan/plan.h"
#include "query/optimizer/plan/join_order/leapfrog_optimizer.h"
#include "query/optimizer/plan/join_order/greedy_optimizer.h"
#include "query/optimizer/plan/join_order/selinger_optimizer.h"
#include "query/exceptions.h"
#include "storage/index/leapfrog/leapfrog_iter.h"

namespace yb::mdb {

namespace {

class MockLeapfrogIter : public LeapfrogIter {
 public:
  MockLeapfrogIter(std::vector<VarId> iv, std::vector<VarId> ev)
      : LeapfrogIter(&interrupt, {}, std::move(iv), std::move(ev)) {}

  void down() override {}
  bool open_terms(Binding&) override { return false; }
  bool next() override { return false; }
  bool seek(uint64_t) override { return false; }
  uint64_t get_key() const override { return 0; }
  void begin_enumeration() override {}
  void reset_enumeration() override {}
  bool next_enumeration(Binding&) override { return false; }
  std::string get_iter_name() const override { return "MockLeapfrogIter"; }
  bool try_estimate(std::vector<double>&, std::vector<double>&) const override { return false; }

 private:
  bool interrupt = false;
};

class MockPlan : public Plan {
 public:
  MockPlan(
      std::set<VarId> vars, std::vector<VarId> intersection_vars,
      std::vector<VarId> enumeration_vars, double cost, double output_size)
      : vars_(std::move(vars)),
        intersection_vars_(std::move(intersection_vars)),
        enumeration_vars_(std::move(enumeration_vars)),
        cost_(cost),
        output_size_(output_size) {}

  std::unique_ptr<Plan> clone() const override {
    return std::make_unique<MockPlan>(
        vars_, intersection_vars_, enumeration_vars_, cost_, output_size_);
  }

  double estimate_cost() const override { return cost_; }
  double estimate_output_size() const override { return output_size_; }
  std::set<VarId> get_vars() const override { return vars_; }
  int relation_size() const override {
    return static_cast<int>(intersection_vars_.size() + enumeration_vars_.size());
  }

  void print(std::ostream&, int) const override {}

  void set_input_vars(const std::set<VarId>&) override {}

  std::unique_ptr<BindingIter> get_binding_iter() const override {
    throw NotSupportedException("MockPlan::get_binding_iter");
  }

  bool get_leapfrog_iter(
      std::vector<std::unique_ptr<LeapfrogIter>>& iters, std::vector<VarId>& intersection,
      uint_fast32_t& enumeration_level) const override {
    iters.push_back(std::make_unique<MockLeapfrogIter>(intersection_vars_, enumeration_vars_));
    intersection = intersection_vars_;
    enumeration_level = enumeration_vars_.size();
    return true;
  }

 private:
  std::set<VarId> vars_;
  std::vector<VarId> intersection_vars_;
  std::vector<VarId> enumeration_vars_;
  double cost_;
  double output_size_;
};

} // anonymous namespace

TEST(JoinOptimizerTest, GreedyOptimizerTwoPlans) {
  auto plan_a = std::make_unique<MockPlan>(
      std::set<VarId>{VarId(0), VarId(1)}, std::vector<VarId>{VarId(0)},
      std::vector<VarId>{VarId(1)}, 10.0, 100.0);
  auto plan_b = std::make_unique<MockPlan>(
      std::set<VarId>{VarId(1), VarId(2)}, std::vector<VarId>{VarId(1)},
      std::vector<VarId>{VarId(2)}, 5.0, 50.0);

  std::vector<std::unique_ptr<Plan>> base_plans;
  base_plans.push_back(std::move(plan_a));
  base_plans.push_back(std::move(plan_b));

  auto result = GreedyOptimizer::get_plan(base_plans);
  ASSERT_NE(result, nullptr);
  auto result_vars = result->get_vars();
  ASSERT_EQ(result_vars.size(), 3);
}

TEST(JoinOptimizerTest, GreedyFirstJoinCostAndDisconnectedFallback) {
    for (bool disconnected : {false, true}) {
        SCOPED_TRACE(disconnected);
        std::vector<std::unique_ptr<Plan>> base_plans;
        base_plans.push_back(std::make_unique<MockPlan>(
            std::set<VarId>{VarId(0), VarId(1)}, std::vector<VarId>{},
            std::vector<VarId>{}, 1.0, 100.0));
        base_plans.push_back(std::make_unique<MockPlan>(
            disconnected ? std::set<VarId>{VarId(2), VarId(3)}
                         : std::set<VarId>{VarId(1), VarId(2)},
            std::vector<VarId>{}, std::vector<VarId>{}, 10.0, 1.0));

        auto legacy = GreedyOptimizer::get_plan(base_plans);
        ASSERT_NE(legacy, nullptr);
        ASSERT_DOUBLE_EQ(legacy->estimate_cost(), 1001.0);
        auto lookahead = GreedyOptimizer::get_plan(base_plans, true);
        ASSERT_NE(lookahead, nullptr);
        ASSERT_DOUBLE_EQ(lookahead->estimate_cost(), disconnected ? 1001.0 : 11.0);
        ASSERT_EQ(lookahead->get_vars().size(), disconnected ? 4 : 3);
        ASSERT_DOUBLE_EQ(base_plans[0]->estimate_cost(), 1.0);
        ASSERT_DOUBLE_EQ(base_plans[1]->estimate_cost(), 10.0);
    }
}

TEST(JoinOptimizerTest, SelingerOptimizerTwoPlans) {
  auto plan_a = std::make_unique<MockPlan>(
      std::set<VarId>{VarId(0), VarId(1)}, std::vector<VarId>{VarId(0)},
      std::vector<VarId>{VarId(1)}, 10.0, 100.0);
  auto plan_b = std::make_unique<MockPlan>(
      std::set<VarId>{VarId(1), VarId(2)}, std::vector<VarId>{VarId(1)},
      std::vector<VarId>{VarId(2)}, 5.0, 50.0);

  std::vector<std::unique_ptr<Plan>> base_plans;
  base_plans.push_back(std::move(plan_a));
  base_plans.push_back(std::move(plan_b));

  SelingerOptimizer optimizer(std::move(base_plans));
  auto result = optimizer.get_plan();
  ASSERT_NE(result, nullptr);
  auto result_vars = result->get_vars();
  ASSERT_EQ(result_vars.size(), 3);
}

TEST(JoinOptimizerTest, LeapfrogOptimizerTwoPlans) {
  auto plan_a = std::make_unique<MockPlan>(
      std::set<VarId>{VarId(0), VarId(1)}, std::vector<VarId>{VarId(0)},
      std::vector<VarId>{VarId(1)}, 10.0, 100.0);
  auto plan_b = std::make_unique<MockPlan>(
      std::set<VarId>{VarId(1), VarId(2)}, std::vector<VarId>{VarId(1)},
      std::vector<VarId>{VarId(2)}, 5.0, 50.0);

  std::vector<std::unique_ptr<Plan>> base_plans;
  base_plans.push_back(std::move(plan_a));
  base_plans.push_back(std::move(plan_b));

  auto result = LeapfrogOptimizer::try_get_iter_with_assigned(base_plans);
  ASSERT_NE(result, nullptr);
}

TEST(JoinOptimizerTest, LeapfrogOptimizerWithoutAssigned) {
  auto plan_a = std::make_unique<MockPlan>(
      std::set<VarId>{VarId(0), VarId(1)}, std::vector<VarId>{VarId(0), VarId(1)},
      std::vector<VarId>{}, 10.0, 100.0);
  auto plan_b = std::make_unique<MockPlan>(
      std::set<VarId>{VarId(1), VarId(2)}, std::vector<VarId>{VarId(1), VarId(2)},
      std::vector<VarId>{}, 5.0, 50.0);

  std::vector<std::unique_ptr<Plan>> base_plans;
  base_plans.push_back(std::move(plan_a));
  base_plans.push_back(std::move(plan_b));

  auto result = LeapfrogOptimizer::try_get_iter_without_assigned(base_plans);
  // The mock has no cache-only sampling implementation.
  ASSERT_EQ(result, nullptr);
}

TEST(JoinOptimizerTest, SinglePlan) {
    auto plan = std::make_unique<MockPlan>(
        std::set<VarId>{VarId(0)}, std::vector<VarId>{VarId(0)}, std::vector<VarId>{}, 1.0, 10.0);

    std::vector<std::unique_ptr<Plan>> base_plans;
    base_plans.push_back(std::move(plan));

    auto result = GreedyOptimizer::get_plan(base_plans);
    ASSERT_NE(result, nullptr);
}

} // namespace yb::mdb
