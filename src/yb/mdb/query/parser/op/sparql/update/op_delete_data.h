#pragma once

#include <memory>
#include <vector>

#include "query/parser/op/sparql/op.h"
#include "query/parser/op/sparql/op_triple.h"

namespace SPARQL {

class OpDeleteData : public Op {
public:
    std::vector<OpTriple> triples;

    OpDeleteData() = default;

    OpDeleteData(std::vector<OpTriple>&& triples) :
        triples(std::move(triples))
    { }

    std::unique_ptr<Op> clone() const override {
        std::vector<OpTriple> new_triples;
        new_triples.reserve(triples.size());
        for (auto& triple : triples) {
            new_triples.push_back(triple);
        }
        return std::make_unique<OpDeleteData>(std::move(new_triples));
    }

    void accept_visitor(OpVisitor& visitor) override {
        visitor.visit(*this);
    }

    std::set<VarId> get_all_vars() const override {
        std::set<VarId> res;
        for (auto& triple : triples) {
            if (triple.subject.is_var()) res.insert(triple.subject.get_var());
            if (triple.predicate.is_var()) res.insert(triple.predicate.get_var());
            if (triple.object.is_var()) res.insert(triple.object.get_var());
        }
        return res;
    }

    std::set<VarId> get_scope_vars() const override { return {}; }
    std::set<VarId> get_safe_vars() const override { return {}; }
    std::set<VarId> get_fixable_vars() const override { return {}; }

    std::ostream& print_to_ostream(std::ostream& os, int indent = 0) const override {
        os << std::string(indent, ' ') << "OpDeleteData (triples: " << triples.size() << ")\n";
        return os;
    }
};

} // namespace SPARQL
