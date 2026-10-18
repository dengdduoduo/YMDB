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

#pragma once

#include <memory>
#include <set>
#include <vector>

#include "graph_models/object_id.h"
#include "graph_models/rdf_model/conversions.h"
#include "query/executor/binding_iter.h"
#include "query/var_id.h"

class Values : public BindingIter {
 public:
    Values(std::vector<std::pair<VarId, bool>>&& vars, std::vector<ObjectId>&& values,
           std::set<VarId> fixed_vars = {}, size_t empty_rows = 0) :
        vars(std::move(vars)), values(std::move(values)), fixed_vars_(std::move(fixed_vars)) {
        num_cols_ = this->vars.size();
        num_rows_ = num_cols_ ? this->values.size() / num_cols_ : empty_rows;
    }

    void _begin(Binding& parent_binding) override {
        parent_binding_ = &parent_binding;
        _reset();
    }

    bool _next() override {
        while (current_row_ < num_rows_) {
            const auto base = current_row_++ * num_cols_;
            bool compatible = true;
            for (size_t i = 0; i < num_cols_; ++i) {
                const auto value = values[base + i];
                if (fixed_vars_.count(vars[i].first) && !value.is_null() &&
                    !fixed_values_[i].is_null() &&
                    SPARQL::Conversions::term_identity(value) !=
                        SPARQL::Conversions::term_identity(fixed_values_[i])) {
                    compatible = false;
                    break;
                }
            }
            if (!compatible) continue;
            for (size_t i = 0; i < num_cols_; ++i) {
                const auto value = values[base + i];
                parent_binding_->add(vars[i].first,
                    value.is_null() && fixed_vars_.count(vars[i].first) ? fixed_values_[i] : value);
            }
            return true;
        }
        return false;
    }

    void _reset() override {
        current_row_ = 0;
        fixed_values_.clear();
        for (const auto& var : vars) {
            fixed_values_.push_back(fixed_vars_.count(var.first)
                ? (*parent_binding_)[var.first] : ObjectId::get_null());
        }
    }

    void assign_nulls() override {
        for (size_t i = 0; i < num_cols_; i++) {
            parent_binding_->add(vars[i].first, ObjectId::get_null());
        }
    }

    void print(std::ostream& os, int indent, bool stats) const override {
        if (stats) {
            print_generic_stats(os, indent);
        }

        os << std::string(indent, ' ') << "Values(";
        for (size_t i = 0; i < num_cols_; i++) {
            if (i != 0) os << ", ";
            os << "?" << get_query_ctx().get_var_name(vars[i].first);
        }
        os << ")\n";

        for (size_t row = 0; row < num_rows_; row++) {
            os << std::string(indent + 2, ' ') << "(";
            for (size_t col = 0; col < num_cols_; col++) {
                if (col != 0) os << ", ";
                os << values[row * num_cols_ + col];
            }
            os << ")\n";
        }
    }

    std::vector<std::pair<VarId, bool>> vars;
    std::vector<ObjectId> values;

 private:
    std::set<VarId> fixed_vars_;
    std::vector<ObjectId> fixed_values_;
    size_t num_cols_;
    size_t num_rows_;
    size_t current_row_ = 0;
    Binding* parent_binding_ = nullptr;
};
