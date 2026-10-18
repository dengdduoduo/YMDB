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

#include "yb/mdb/query/executor/binding_iter/sub_select.h"

void SubSelect::_begin(Binding& _parent_binding) {
    parent_binding = &_parent_binding;

    child_binding = std::make_unique<Binding>(_parent_binding.size);

    for (auto& var : safe_assigned_vars) {
        child_binding->add(var, (*parent_binding)[var]);
    }

    child_iter->begin(*child_binding);
    set_status_from(*child_iter);
}

void SubSelect::_reset() {
    for (auto& var : safe_assigned_vars) {
        child_binding->add(var, (*parent_binding)[var]);
    }
    child_iter->reset();
    set_status_from(*child_iter);
}

bool SubSelect::_next() {
    if (!status().ok()) return false;
    const auto has_next = child_iter->next();
    set_status_from(*child_iter);
    if (has_next && status().ok()) {
        for (auto& var : projection_vars) {
            parent_binding->add(var, (*child_binding)[var]);
        }
        return true;
    }
    return false;
}

void SubSelect::assign_nulls() {
    child_iter->assign_nulls();

    for (auto& var : projection_vars) {
        parent_binding->add(var, (*child_binding)[var]);
    }
}

void SubSelect::print(std::ostream& os, int indent, bool stats) const {
    if (stats) {
        print_generic_stats(os, indent);
    }
    os << std::string(indent, ' ') << "SubSelect(proj_vars: ";
    bool first = true;
    for (auto& var : projection_vars) {
        if (!first) os << ", ";
        first = false;
        os << var;
    }

    os << ", fixed_vars: ";
    first = true;
    for (auto& var : safe_assigned_vars) {
        if (!first) os << ", ";
        first = false;
        os << var;
    }
    os << ")\n";
    child_iter->print(os, indent + 2, stats);
}
