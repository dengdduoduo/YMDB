#pragma once
#include <cassert>
#include <memory>
#include <string>
#include <vector>
#include "query/executor/binding_iter.h"
#include "query/executor/binding_iter/binding_expr/binding_expr.h"
#include "query/var_id.h"
#include "storage/index/text_search/text_index_manager.h"
namespace Procedure {
class TextSearchMultiScan : public BindingIter {
public:
    using UnpackStringFuncType = std::string(ObjectId);
    struct HeapItem {
        double score;
        std::size_t pos;
        HeapItem() = default;
        explicit HeapItem(double score_, std::size_t pos_) : score{score_}, pos{pos_} { }
    };
    struct HeapItemLess {
        bool operator()(const HeapItem& lhs, const HeapItem& rhs) { return lhs.score < rhs.score; }
    };
    TextSearchMultiScan(
        std::vector<std::unique_ptr<BindingExpr>>&& argument_binding_exprs_,
        std::vector<VarId>&& yield_vars_,
        TextSearch::TextIndexManager& text_index_manager_,
        UnpackStringFuncType* unpack_string_func_ptr_
    ) { }
    void _begin(Binding& parent_binding) override { }
    bool _next() override { return false; }
    void _reset() override { }
    void assign_nulls() override { }
    void print(std::ostream& os, int indent, bool stats) const override { os << std::string(indent, ' ') << "TextSearchMultiScan\n"; }
};
}
