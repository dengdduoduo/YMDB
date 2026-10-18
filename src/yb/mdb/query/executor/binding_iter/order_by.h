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

#include <algorithm>
#include <memory>
#include <numeric>
#include <set>
#include <vector>

#include "graph_models/object_id.h"
#include "query/executor/binding_iter.h"

class OrderBy : public BindingIter {
 public:
    using Status = yb::Status;
    OrderBy(
        std::unique_ptr<BindingIter> child_iter,
        std::set<VarId>&& saved_vars,
        std::vector<VarId>&& order_vars,
        std::vector<bool>&& ascending,
        int64_t (*_compare)(ObjectId, ObjectId)
    ) : child_iter(std::move(child_iter)),
        saved_vars(std::move(saved_vars)),
        order_vars(std::move(order_vars)),
        ascending(std::move(ascending)),
        compare(_compare) { }

    void _begin(Binding& parent_binding) override {
        parent_binding_ = &parent_binding;
        result_index_ = 0;
        sorted_rows_.clear();
        if (!status().ok()) return;
        if (order_vars.size() != ascending.size()) {
            set_status(STATUS(InvalidArgument, "ORDER BY key/direction size mismatch"));
            return;
        }
        for (auto var : order_vars) {
            if (var.id >= parent_binding.size) {
                set_status(STATUS(InvalidArgument, "ORDER BY variable outside binding"));
                return;
            }
        }
        child_iter->begin(parent_binding);
        set_status_from(*child_iter);
        if (!status().ok()) return;

        std::vector<std::vector<ObjectId>> sort_keys;
        sorted_rows_.clear();

        while (child_iter->next()) {
            set_status_from(*child_iter);
            if (!status().ok()) break;
            std::vector<ObjectId> row(parent_binding.size);
            for (std::size_t i = 0; i < parent_binding.size; i++) {
                row[i] = parent_binding[VarId(static_cast<uint_fast32_t>(i))];
            }

            std::vector<ObjectId> keys;
            for (auto& var : order_vars) {
                const auto value = parent_binding[var];
                if (compare == rdf_compare) set_status(ValidateKey(value));
                if (!status().ok()) {
                    sorted_rows_.clear();
                    return;
                }
                keys.push_back(value);
            }

            sorted_rows_.push_back(std::move(row));
            sort_keys.push_back(std::move(keys));
        }

        set_status_from(*child_iter);
        if (!status().ok()) {
            sorted_rows_.clear();
            return;
        }
        std::vector<std::size_t> indices(sorted_rows_.size());
        std::iota(indices.begin(), indices.end(), 0);

        std::stable_sort(indices.begin(), indices.end(), [&](std::size_t a, std::size_t b) {
            for (std::size_t i = 0; i < order_vars.size(); i++) {
                auto lhs = sort_keys[a][i];
                auto rhs = sort_keys[b][i];
                int64_t cmp = compare(lhs, rhs);
                if (cmp != 0) {
                    return ascending[i] ? (cmp < 0) : (cmp > 0);
                }
            }
            return false;
        });

        std::vector<std::vector<ObjectId>> reordered;
        reordered.reserve(indices.size());
        for (std::size_t idx : indices) {
            reordered.push_back(std::move(sorted_rows_[idx]));
        }
        sorted_rows_ = std::move(reordered);

        result_index_ = 0;
    }

    bool _next() override {
        if (!status().ok() || result_index_ >= sorted_rows_.size()) return false;
        auto& row = sorted_rows_[result_index_++];
        for (std::size_t i = 0; i < parent_binding_->size; i++) {
            parent_binding_->add(VarId(static_cast<uint_fast32_t>(i)), row[i]);
        }
        return true;
    }

    void _reset() override {
        if (parent_binding_ != nullptr) _begin(*parent_binding_);
    }

    void assign_nulls() override { child_iter->assign_nulls(); }

    void print(std::ostream& os, int indent, bool stats) const override {
        os << std::string(indent, ' ') << "OrderBy\n";
    }

    std::unique_ptr<BindingIter> child_iter;
    Binding* parent_binding_ = nullptr;
    std::set<VarId> saved_vars;
    std::vector<VarId> order_vars;
    std::vector<bool> ascending;
    int64_t (*compare)(ObjectId, ObjectId);
    std::vector<std::vector<ObjectId>> sorted_rows_;
    std::size_t result_index_ = 0;

    static int64_t rdf_compare(ObjectId lhs, ObjectId rhs);
    static yb::Status ValidateKey(ObjectId key);

    static int64_t internal_compare(ObjectId lhs, ObjectId rhs) {
        return (lhs.id > rhs.id) - (lhs.id < rhs.id);
    }
};
