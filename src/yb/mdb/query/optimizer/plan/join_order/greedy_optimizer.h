// Portions Copyright (c) YugabyteDB, Inc.

#pragma once

#include "query/optimizer/plan/plan.h"

class GreedyOptimizer {
 public:
  static bool should_use_hash_join(const Plan& lhs, const Plan& rhs);

  static std::unique_ptr<Plan> get_plan(
      const std::vector<std::unique_ptr<Plan>>& base_plans, bool compare_first_join = false);
};
