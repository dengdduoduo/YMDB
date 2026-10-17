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

#include "yb/mdb/query/distributed/scatter_executor.h"

#include "yb/mdb/query/executor/binding.h"
#include "yb/mdb/query/executor/binding_iter.h"
#include "yb/mdb/query/optimizer/binding_iter_constructor.h"
#include "yb/mdb/query/query_context.h"
#include "yb/util/logging.h"
#include "yb/util/status.h"

namespace yb::mdb {

Status ScatterExecutor::Execute(const DistributedPlan& plan,
                                std::vector<FragmentResultSet>* results) {
  results->clear();
  results->reserve(plan.num_fragments());

  for (const auto& fragment : plan.fragments()) {
    FragmentResultSet result;
    result.fragment_index = static_cast<uint32_t>(results->size());
    result.status = ExecuteLocal(fragment, &result);
    results->push_back(std::move(result));
  }

  for (const auto& result : *results) {
    if (!result.status.ok()) {
      return result.status;
    }
  }

  return Status::OK();
}

Status ScatterExecutor::ExecuteLocal(const TabletFragment& fragment,
                                     FragmentResultSet* result) {
  if (fragment.op_tree == nullptr) {
    return STATUS(InvalidArgument, "Fragment has null op_tree");
  }

  SPARQL::BindingIterConstructor optimizer;
  fragment.op_tree->accept_visitor(optimizer);

  if (optimizer.tmp == nullptr) {
    return STATUS(RuntimeError, "BindingIterConstructor produced null iter");
  }

  auto& iter = optimizer.tmp;
  auto& qctx = get_query_ctx();
  Binding binding(qctx.get_var_size());
  iter->begin(binding);

  while (iter->next()) {
    auto row = std::make_unique<Binding>(qctx.get_var_size());
    row->add_all(binding);
    result->bindings.push_back(std::move(row));
  }

  return Status::OK();
}

}  // namespace yb::mdb
