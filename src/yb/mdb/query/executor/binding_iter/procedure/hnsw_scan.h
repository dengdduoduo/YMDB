#pragma once
#include <cassert>
#include <memory>
#include <string>
#include <vector>
#include "query/executor/binding_iter.h"
#include "query/executor/binding_iter/binding_expr/binding_expr.h"
#include "query/var_id.h"
#include "storage/index/hnsw/hnsw_index_manager.h"
namespace Procedure {
class HNSWScan : public BindingIter {
public:
    using UnpackStringFuncType = std::string(ObjectId);
    using UnpackIntFuncType = int64_t(ObjectId);
    HNSWScan(
        std::vector<std::unique_ptr<BindingExpr>>&& argument_binding_exprs_,
        std::vector<VarId>&& yield_vars_,
        HNSW::HNSWIndexManager& hnsw_index_manager_,
        UnpackStringFuncType* unpack_string_func_ptr_,
        UnpackIntFuncType* unpack_int_func_ptr_
    ) { }
    void _begin(Binding& parent_binding) override { }
    bool _next() override { return false; }
    void _reset() override { }
    void assign_nulls() override { }
    void print(std::ostream& os, int indent, bool stats) const override { os << std::string(indent, ' ') << "HNSWScan\n"; }
};
}
