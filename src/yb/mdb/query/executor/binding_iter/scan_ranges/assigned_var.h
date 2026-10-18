#pragma once

#include <cassert>
#include <unordered_map>

#include "graph_models/rdf_model/rdf_object_id.h"
#include "query/query_context.h"
#include "query/executor/binding_iter/scan_ranges/scan_range.h"

class AssignedVar : public ScanRange {
private:
    VarId var;

    static uint64_t to_gmap(uint64_t val) {
        auto it = g_pack_to_gmap.find(val);
        return it != g_pack_to_gmap.end() ? it->second : val;
    }

public:
    AssignedVar(VarId var) : var(var) { }

    uint64_t get_min(Binding& binding) override {
        return to_gmap(binding[var].id);
    }

    uint64_t get_max(Binding& binding) override {
        return to_gmap(binding[var].id);
    }

    void print(std::ostream& os) const override {
        os << '[' << var << ']';
    }

    void try_assign(Binding&, ObjectId) override { }
};
