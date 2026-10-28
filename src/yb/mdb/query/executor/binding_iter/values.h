#pragma once

#include <memory>
#include <vector>

#include "graph_models/object_id.h"
#include "query/executor/binding_iter.h"
#include "query/var_id.h"

class Values : public BindingIter {
public:
    Values(std::vector<std::pair<VarId, bool>>&& vars, std::vector<ObjectId>&& values,
           std::vector<bool> fixed = {}, size_t empty_rows = 0) :
        vars(std::move(vars)), values(std::move(values))
    {
        num_cols_ = this->vars.size();
        row_count_ = num_cols_ ? this->values.size() / num_cols_ : empty_rows;
        fixed_ = std::move(fixed);
        fixed_.resize(num_cols_, false);
    }

    void _begin(Binding& parent_binding) override
    {
        parent_binding_ = &parent_binding;
        _reset();
    }

    bool _next() override
    {
        while (current_row_ < row_count_) {
            const auto base = current_row_++ * num_cols_;
            bool compatible = true;
            for (size_t i = 0; i < num_cols_; ++i) {
                if (fixed_[i] && !input_[i].is_null() && !values[base + i].is_null() &&
                    input_[i] != values[base + i]) { compatible = false; break; }
            }
            if (!compatible) continue;
            for (size_t i = 0; i < num_cols_; ++i)
                parent_binding_->add(vars[i].first,
                    fixed_[i] && values[base + i].is_null() ? input_[i] : values[base + i]);
            return true;
        }
        return false;
    }

    void _reset() override {
        current_row_ = 0;
        input_.clear();
        for (const auto& var : vars) input_.push_back((*parent_binding_)[var.first]);
    }

    void assign_nulls() override
    {
        for (size_t i = 0; i < num_cols_; i++) {
            if (!fixed_[i]) parent_binding_->add(vars[i].first, ObjectId::get_null());
        }
    }

    void print(std::ostream& os, int indent, bool stats) const override
    {
        if (stats) {
            print_generic_stats(os, indent);
        }

        os << std::string(indent, ' ') << "Values(";
        for (size_t i = 0; i < num_cols_; i++) {
            if (i != 0) os << ", ";
            os << "?" << get_query_ctx().get_var_name(vars[i].first);
        }
        os << ")\n";

        size_t num_rows = row_count_;
        for (size_t row = 0; row < num_rows; row++) {
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
    size_t num_cols_;
    size_t row_count_ = 0;
    std::vector<bool> fixed_;
    std::vector<ObjectId> input_;
    size_t current_row_ = 0;
    Binding* parent_binding_ = nullptr;
};
