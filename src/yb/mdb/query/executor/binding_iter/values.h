#pragma once

#include <memory>
#include <vector>

#include "graph_models/object_id.h"
#include "query/executor/binding_iter.h"
#include "query/var_id.h"

class Values : public BindingIter {
public:
    Values(std::vector<std::pair<VarId, bool>>&& vars, std::vector<ObjectId>&& values) :
        vars(std::move(vars)), values(std::move(values))
    {
        num_cols_ = this->vars.size();
    }

    void _begin(Binding& parent_binding) override
    {
        parent_binding_ = &parent_binding;
        current_row_ = 0;
    }

    bool _next() override
    {
        if (current_row_ * num_cols_ >= values.size()) {
            return false;
        }

        size_t base = current_row_ * num_cols_;
        for (size_t i = 0; i < num_cols_; i++) {
            parent_binding_->add(vars[i].first, values[base + i]);
        }
        current_row_++;
        return true;
    }

    void _reset() override
    {
        current_row_ = 0;
    }

    void assign_nulls() override
    {
        for (size_t i = 0; i < num_cols_; i++) {
            parent_binding_->add(vars[i].first, ObjectId::get_null());
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

        size_t num_rows = values.size() / num_cols_;
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
    size_t current_row_ = 0;
    Binding* parent_binding_ = nullptr;
};
