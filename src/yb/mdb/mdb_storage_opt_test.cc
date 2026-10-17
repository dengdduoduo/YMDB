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

#include <vector>

#include "query/exceptions.h"
#include "query/optimizer/rdf_model/plan/triple_plan.h"
#include "storage/index/leapfrog/leapfrog_iter.h"

namespace yb::mdb {

TEST(StorageOptTest, TriplePlanGetVars_AllVars) {
    Id subj(VarId(1));
    Id pred(VarId(2));
    Id obj(VarId(3));

    SPARQL::TriplePlan plan(subj, pred, obj);
    auto vars = plan.get_vars();
    EXPECT_EQ(vars.size(), 3);
    EXPECT_TRUE(vars.count(VarId(1)));
    EXPECT_TRUE(vars.count(VarId(2)));
    EXPECT_TRUE(vars.count(VarId(3)));
}

TEST(StorageOptTest, TriplePlanGetVars_SubjAssigned) {
    Id subj(ObjectId(1));
    Id pred(VarId(2));
    Id obj(VarId(3));

    SPARQL::TriplePlan plan(subj, pred, obj);
    auto vars = plan.get_vars();
    EXPECT_EQ(vars.size(), 2);
    EXPECT_TRUE(vars.count(VarId(2)));
    EXPECT_TRUE(vars.count(VarId(3)));
}

TEST(StorageOptTest, TriplePlanGetVars_AllAssigned) {
    Id subj(ObjectId(1));
    Id pred(ObjectId(2));
    Id obj(ObjectId(3));

    SPARQL::TriplePlan plan(subj, pred, obj);
    auto vars = plan.get_vars();
    EXPECT_EQ(vars.size(), 0);
}

TEST(StorageOptTest, TriplePlanSetInputVars) {
    Id subj(VarId(1));
    Id pred(VarId(2));
    Id obj(VarId(3));

    SPARQL::TriplePlan plan(subj, pred, obj);

    EXPECT_FALSE(plan.subject_assigned);
    EXPECT_FALSE(plan.predicate_assigned);
    EXPECT_FALSE(plan.object_assigned);

    std::set<VarId> input_vars = { VarId(1) };
    plan.set_input_vars(input_vars);

    EXPECT_TRUE(plan.subject_assigned);
    EXPECT_FALSE(plan.predicate_assigned);
    EXPECT_FALSE(plan.object_assigned);

    auto vars = plan.get_vars();
    EXPECT_EQ(vars.size(), 2);
}

TEST(StorageOptTest, TriplePlanRelationSize) {
    Id subj(VarId(1));
    Id pred(VarId(2));
    Id obj(VarId(3));

    SPARQL::TriplePlan plan(subj, pred, obj);
    EXPECT_EQ(plan.relation_size(), 3);
}

TEST(StorageOptTest, TriplePlanEstimateAllTerms) {
    Id subj(ObjectId(1));
    Id pred(ObjectId(2));
    Id obj(ObjectId(3));

    SPARQL::TriplePlan plan(subj, pred, obj);
    EXPECT_EQ(plan.estimate_output_size(), 1.0);
}

TEST(StorageOptTest, TriplePlanEstimateCost) {
    Id subj(VarId(1));
    Id pred(VarId(2));
    Id obj(VarId(3));

    SPARQL::TriplePlan plan(subj, pred, obj);
    double size = plan.estimate_output_size();
    double cost = plan.estimate_cost();
    EXPECT_EQ(cost, size);
}

TEST(StorageOptTest, TriplePlanGetBindingIterThrows) {
    Id subj(VarId(1));
    Id pred(VarId(2));
    Id obj(VarId(3));

    SPARQL::TriplePlan plan(subj, pred, obj);
    EXPECT_THROW(plan.get_binding_iter(), NotSupportedException);
}

TEST(StorageOptTest, TriplePlanGetLeapfrogIterReturnsFalse) {
    Id subj(VarId(1));
    Id pred(VarId(2));
    Id obj(VarId(3));

    SPARQL::TriplePlan plan(subj, pred, obj);
    std::vector<std::unique_ptr<LeapfrogIter>> leapfrog_iters;
    std::vector<VarId> var_order;
    uint_fast32_t enumeration_level = 0;
    EXPECT_FALSE(plan.get_leapfrog_iter(leapfrog_iters, var_order, enumeration_level));
}

TEST(StorageOptTest, TriplePlanClone) {
    Id subj(VarId(1));
    Id pred(ObjectId(2));
    Id obj(VarId(3));

    SPARQL::TriplePlan plan(subj, pred, obj);
    auto clone = plan.clone();

    auto orig_vars = plan.get_vars();
    auto clone_vars = clone->get_vars();
    EXPECT_EQ(orig_vars, clone_vars);

    EXPECT_EQ(plan.estimate_output_size(), clone->estimate_output_size());
}

} // namespace yb::mdb
