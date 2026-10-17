#pragma once
#include <set>
#include <variant>
#include "query/executor/binding_iter.h"
class SparqlService : public BindingIter {
public:
    SparqlService(bool silent, std::string&& query, std::string&& prefixes, std::variant<VarId, std::string> var_or_iri, std::set<VarId> scope_vars, std::set<VarId> fixed_vars, std::set<VarId> fixed_join_vars) { }
    void _begin(Binding& parent_binding) override { }
    bool _next() override { return false; }
    void _reset() override { }
    void assign_nulls() override { }
    void print(std::ostream& os, int indent, bool stats) const override { os << std::string(indent, ' ') << "SparqlService\\n"; }
};
