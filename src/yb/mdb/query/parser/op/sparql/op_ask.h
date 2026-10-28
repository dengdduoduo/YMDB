#pragma once

#include "query/parser/op/sparql/op.h"

namespace SPARQL {

class OpAsk : public Op {
public:
    std::unique_ptr<Op> op;

    const uint64_t offset;
    const uint64_t limit;

    OpAsk(std::unique_ptr<Op> op, uint64_t offset = Op::DEFAULT_OFFSET,
          uint64_t limit = Op::DEFAULT_LIMIT)
        : op(std::move(op)), offset(offset), limit(limit) {}

    std::unique_ptr<Op> clone() const override {
        return std::make_unique<OpAsk>(op->clone(), offset, limit);
    }

    void accept_visitor(OpVisitor& visitor) override {
        visitor.visit(*this);
    }

    std::set<VarId> get_all_vars() const override {
        return op->get_all_vars();
    }

    std::set<VarId> get_scope_vars() const override {
        return {};
    }

    std::set<VarId> get_safe_vars() const override {
        return {};
    }

    std::set<VarId> get_fixable_vars() const override {
        return op->get_fixable_vars();
    }

    std::ostream& print_to_ostream(std::ostream& os, int indent = 0) const override {
        os << std::string(indent, ' ') << "OpAsk()\n";
        return op->print_to_ostream(os, indent + 2);
    }
};
} // namespace SPARQL
