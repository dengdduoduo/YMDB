#pragma once

#include <cassert>
#include <memory>
#include <set>
#include <vector>

#include "graph_models/object_id.h"
#include "query/var_id.h"
#include "storage/file_id.h"
#include "storage/page/private_page.h"

struct OrderInfo {
    std::vector<VarId> saved_vars;
    std::vector<bool> ascending;

    OrderInfo(
        std::vector<VarId>&& order_vars,
        std::vector<bool>&& ascending,
        const std::set<VarId>& other_saved_vars
    ) :
        ascending(ascending)
    {
        std::set<VarId> inserted_vars;
        for (auto& var : order_vars) {
            if (inserted_vars.insert(var).second) {
                saved_vars.push_back(var);
            }
        }
        assert(saved_vars.size() == ascending.size());
        for (auto& var : other_saved_vars) {
            if (inserted_vars.insert(var).second) {
                saved_vars.push_back(var);
            }
        }
    }
};

class TupleCollectionPage {
public:
    TupleCollectionPage(PPage& page, OrderInfo& info, int64_t (*compare)(ObjectId, ObjectId)) :
        page(page), info(info), tuples(reinterpret_cast<ObjectId*>(page.bytes + sizeof(uint64_t))),
        tuple_count(reinterpret_cast<uint64_t*>(page.bytes)), compare(compare) { }

    ~TupleCollectionPage() = default;

    bool is_full() const { return false; }

    uint64_t get_tuple_count() const noexcept { return *tuple_count; }

    const ObjectId* get(uint64_t n) const { return &tuples[n * info.saved_vars.size()]; }

    bool less_or_equal(const ObjectId* lhs, const ObjectId* rhs) const { return false; }

    void add(const ObjectId* tuple) { }

    void sort() { }

    void reset() { *tuple_count = 0; }

private:
    PPage& page;
    OrderInfo& info;
    ObjectId* const tuples;
    uint64_t* const tuple_count;
    int64_t (*compare)(ObjectId, ObjectId);
};

class TupleCollectionMerger {
public:
    TupleCollectionMerger(OrderInfo& info, int64_t (*compare)(ObjectId, ObjectId)) { }
    void merge(uint64_t, uint64_t, uint64_t, uint64_t, TmpFileId, TmpFileId) { }
    void copy_page(uint64_t, TmpFileId, TmpFileId) { }
};
