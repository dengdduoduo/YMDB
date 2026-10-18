#pragma once

#include <array>
#include <memory>

#include "graph_models/object_id.h"
#include "query/executor/binding.h"
#include "query/executor/binding_iter.h"
#include "query/executor/binding_iter/scan_ranges/scan_range.h"
#include "query/id.h"
#include "yb/mdb/storage/docdb_range_cache.h"
#include "yb/mdb/storage/docdb_read_context.h"
#include "yb/mdb/storage/docdb_scan_cursor.h"

class DocDBScan : public BindingIter {
public:
    DocDBScan(yb::mdb::DocDBRangeCache* cache, int perm_index,
              std::array<std::unique_ptr<ScanRange>, 3> ranges,
              int num_ranges,
              Id subject, Id predicate, Id object,
              bool subject_assigned, bool predicate_assigned, bool object_assigned,
              bool check_c0_eq_c1 = false, bool check_c1_eq_c2 = false,
              bool check_c0_eq_c2 = false,
              const yb::client::TableHandle* table = nullptr,
              yb::mdb::DocDBReadContext* read_context = nullptr);

    void _begin(Binding& parent_binding) override;
    bool _next() override;
    void _reset() override;
    void assign_nulls() override;
    void print(std::ostream& os, int indent, bool stats) const override;

private:
    bool NextStreamRow(uint64_t* c0, uint64_t* c1, uint64_t* c2);
    yb::mdb::DocDBRangeCache* cache_;
    const yb::client::TableHandle* table_;
    yb::mdb::DocDBReadContext* read_context_;
    yb::mdb::DocDBScanCursor cursor_;
    yb::mdb::DocDBScanBatch batch_;
    size_t batch_position_ = 0;
    int perm_index_;
    std::array<std::unique_ptr<ScanRange>, 3> ranges_;
    int num_ranges_;
    Id subject_, predicate_, object_;
    bool subject_assigned_, predicate_assigned_, object_assigned_;
    bool check_c0_eq_c1_, check_c1_eq_c2_, check_c0_eq_c2_;
    Binding* parent_binding_ = nullptr;
    bool first_call_ = true;
    size_t row_index_ = 0;
    uint64_t current_c0_, current_c1_, current_c2_;

    static constexpr uint64_t kMax = 0xFFFFFFFFFFFFFFULL;
};
