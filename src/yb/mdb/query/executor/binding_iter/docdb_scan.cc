#include "docdb_scan.h"

#include <limits>

#include "graph_models/object_id.h"
#include "graph_models/rdf_model/rdf_object_id.h"
#include "query/executor/binding.h"
#include "query/exceptions.h"
#include "yb/mdb/storage/docdb_key_encoder.h"

DocDBScan::DocDBScan(yb::mdb::DocDBRangeCache* cache, int perm_index,
                     std::array<std::unique_ptr<ScanRange>, 3> ranges,
                     int num_ranges,
                     Id subject, Id predicate, Id object,
                     bool subject_assigned, bool predicate_assigned, bool object_assigned,
                     bool check_c0_eq_c1, bool check_c1_eq_c2,
                     bool check_c0_eq_c2,
                     const yb::client::TableHandle* table,
                     yb::mdb::DocDBReadContext* read_context)
    : cache_(cache), table_(table), read_context_(read_context), perm_index_(perm_index),
      ranges_(std::move(ranges)), num_ranges_(num_ranges),
      subject_(subject), predicate_(predicate), object_(object),
      subject_assigned_(subject_assigned),
      predicate_assigned_(predicate_assigned),
      object_assigned_(object_assigned),
      check_c0_eq_c1_(check_c0_eq_c1),
      check_c1_eq_c2_(check_c1_eq_c2),
      check_c0_eq_c2_(check_c0_eq_c2) {
    if ((!cache_ && !read_context_) || (read_context_ && !table_)) {
        throw QueryException("DocDBScan has no valid input source");
    }
}

void DocDBScan::_begin(Binding& parent_binding) {
    parent_binding_ = &parent_binding;
    _reset();
}

bool DocDBScan::_next() {
    if (read_context_) {
        for (const auto& term : {subject_, predicate_, object_}) {
            if (term.is_OID() && term.get_OID().is_not_found()) return false;
        }
        auto missing_binding = [&](Id term, bool assigned) {
            return term.is_var() && assigned &&
                g_pack_to_gmap.find((*parent_binding_)[term.get_var()].id) ==
                    g_pack_to_gmap.end();
        };
        if (missing_binding(subject_, subject_assigned_) ||
            missing_binding(predicate_, predicate_assigned_) ||
            missing_binding(object_, object_assigned_)) return false;
    }
    uint64_t min[3], max[3];
    for (int i = 0; i < num_ranges_; i++) {
        min[i] = ranges_[i]->get_min(*parent_binding_);
        max[i] = ranges_[i]->get_max(*parent_binding_);
    }
    for (int i = num_ranges_; i < 3; i++) {
        min[i] = 0;
        max[i] = kMax;
    }

    uint64_t c0, c1, c2;
    bool found;
    if (read_context_) {
        if (first_call_) {
            first_call_ = false;
            yb::mdb::DocDBScanSpec spec;
            for (int i = 0; i < 3; ++i) spec.columns[i] = {min[i], max[i]};
            if (min[0] == max[0]) spec.hash_key = min[0];
            spec.page_rows = read_context_->page_rows;
            spec.page_bytes = read_context_->page_bytes;
            spec.deadline = read_context_->deadline;
            spec.cancelled = &read_context_->cancelled;
            auto status = cursor_.Open(table_, read_context_->session, std::move(spec));
            if (!status.ok()) throw QueryException(status.ToString());
        }
        found = NextStreamRow(&c0, &c1, &c2);
    } else if (first_call_) {
        first_call_ = false;
        found = cache_->SeekRangeWithIndex(min[0], max[0], &c0, &c1, &c2, &row_index_);
    } else {
        found = cache_->NextWithIndex(max[0], &c0, &c1, &c2, &row_index_);
    }

    while (found) {
        bool in_range = true;
        for (int i = 1; i < num_ranges_; i++) {
            uint64_t val = (i == 1) ? c1 : c2;
            if (val < min[i] || val > max[i]) {
                in_range = false;
                break;
            }
        }

        if (in_range && check_c0_eq_c1_ && c0 != c1) {
            in_range = false;
        }
        if (in_range && check_c1_eq_c2_ && c1 != c2) {
            in_range = false;
        }
        if (in_range && check_c0_eq_c2_ && c0 != c2) {
            in_range = false;
        }

        if (in_range) {
            current_c0_ = c0;
            current_c1_ = c1;
            current_c2_ = c2;
            uint64_t s, p, o;
            yb::mdb::DocKeyEncoder::ReversePermute(
                perm_index_, c0, c1, c2, &s, &p, &o);

            auto decode = [&](uint64_t id) {
                auto it = g_gmap_to_pack.find(id);
                if (it != g_gmap_to_pack.end()) return it->second;
                if (read_context_) {
                    throw QueryException("Missing typed dictionary entry for stored RDF ID " +
                                         std::to_string(id));
                }
                return ObjectId(id);
            };

            if (subject_.is_var() && !subject_assigned_) {
                auto soid = decode(s);
                parent_binding_->add(subject_.get_var(), soid);
            }
            if (predicate_.is_var() && !predicate_assigned_) {
                auto poid = decode(p);
                parent_binding_->add(predicate_.get_var(), poid);
            }
            if (object_.is_var() && !object_assigned_) {
                auto ooid = decode(o);
                parent_binding_->add(object_.get_var(), ooid);
            }
            return true;
        }

        found = read_context_ ? NextStreamRow(&c0, &c1, &c2)
                              : cache_->NextWithIndex(max[0], &c0, &c1, &c2, &row_index_);
    }

    return false;
}

void DocDBScan::_reset() {
    cursor_.Close();
    batch_ = {};
    batch_position_ = 0;
    row_index_ = 0;
    first_call_ = true;
}

bool DocDBScan::NextStreamRow(uint64_t* c0, uint64_t* c1, uint64_t* c2) {
    while (batch_position_ == batch_.rows.size()) {
        if (batch_.eof) return false;
        auto before = cursor_.rpc_calls();
        auto bytes = cursor_.payload_bytes();
        auto decoded = cursor_.decoded_rows();
        auto result = cursor_.NextBatch();
        auto operations = cursor_.rpc_calls() - before;
        read_context_->scan_rpc_calls += operations;
        read_context_->stats.read_operations += operations;
        read_context_->stats.flush_rounds += operations != 0;
        read_context_->stats.response_payload_bytes += cursor_.payload_bytes() - bytes;
        read_context_->stats.decoded_rows += cursor_.decoded_rows() - decoded;
        if (!result.ok()) throw QueryException(result.status().ToString());
        batch_ = std::move(*result);
        batch_position_ = 0;
    }
    const auto& row = batch_.rows[batch_position_++];
    *c0 = row[0];
    *c1 = row[1];
    *c2 = row[2];
    return true;
}

bool DocDBScan::CanBatchBoundJoin() const {
    if (!read_context_ || !table_) return false;
    uint64_t a, b, c;
    yb::mdb::DocKeyEncoder::Permute(perm_index_, subject_assigned_, predicate_assigned_,
                                  object_assigned_, &a, &b, &c);
    return a && ((subject_.is_var() && subject_assigned_) ||
                 (predicate_.is_var() && predicate_assigned_) ||
                 (object_.is_var() && object_assigned_));
}

yb::Result<std::optional<yb::mdb::DocDBScanSpec>> DocDBScan::ProbeSpec(Binding& input) const {
    for (const auto& term : {subject_, predicate_, object_}) {
        if (term.is_OID() && term.get_OID().is_not_found()) {
            return std::optional<yb::mdb::DocDBScanSpec>();
        }
    }
    auto missing = [&](Id term, bool assigned) {
        return term.is_var() && assigned &&
            (input[term.get_var()].is_null() ||
             g_pack_to_gmap.find(input[term.get_var()].id) == g_pack_to_gmap.end());
    };
    if (missing(subject_, subject_assigned_) || missing(predicate_, predicate_assigned_) ||
        missing(object_, object_assigned_)) return std::optional<yb::mdb::DocDBScanSpec>();
    yb::mdb::DocDBScanSpec spec;
    for (int i = 0; i < 3; ++i) {
        spec.columns[i] = i < num_ranges_ ?
            yb::mdb::DocDBScanSpec::Range{ranges_[i]->get_min(input), ranges_[i]->get_max(input)} :
            yb::mdb::DocDBScanSpec::Range{0, kMax};
    }
    if (spec.columns[0].min == spec.columns[0].max) spec.hash_key = spec.columns[0].min;
    spec.page_rows = read_context_->page_rows;
    spec.page_bytes = read_context_->page_bytes;
    spec.cancelled = &read_context_->cancelled;
    spec.deadline = read_context_->deadline;
    return std::optional<yb::mdb::DocDBScanSpec>(std::move(spec));
}

bool DocDBScan::Matches(const yb::mdb::DocDBScanBatch::Row& row) const {
    return (!check_c0_eq_c1_ || row[0] == row[1]) &&
           (!check_c1_eq_c2_ || row[1] == row[2]) &&
           (!check_c0_eq_c2_ || row[0] == row[2]);
}

yb::Status DocDBScan::DecodeRow(const yb::mdb::DocDBScanBatch::Row& row, Binding* output) const {
    using yb::Status;
    uint64_t s, p, o;
    yb::mdb::DocKeyEncoder::ReversePermute(perm_index_, row[0], row[1], row[2], &s, &p, &o);
    for (const auto& item : {std::make_pair(subject_, std::make_pair(subject_assigned_, s)),
                             std::make_pair(predicate_, std::make_pair(predicate_assigned_, p)),
                             std::make_pair(object_, std::make_pair(object_assigned_, o))}) {
        if (!item.first.is_var() || item.second.first) continue;
        auto found = g_gmap_to_pack.find(item.second.second);
        if (found == g_gmap_to_pack.end()) {
            return STATUS(Corruption, "Missing typed dictionary entry for stored RDF ID");
        }
        output->add(item.first.get_var(), found->second);
    }
    return yb::Status::OK();
}

void DocDBScan::assign_nulls() {
    if (subject_.is_var() && !subject_assigned_) {
        parent_binding_->add(subject_.get_var(), ObjectId::get_null());
    }
    if (predicate_.is_var() && !predicate_assigned_) {
        parent_binding_->add(predicate_.get_var(), ObjectId::get_null());
    }
    if (object_.is_var() && !object_assigned_) {
        parent_binding_->add(object_.get_var(), ObjectId::get_null());
    }
}

void DocDBScan::print(std::ostream& os, int indent, bool stats) const {
    if (stats) {
        print_generic_stats(os, indent);
    }
    os << std::string(indent, ' ') << "DocDBScan(" << subject_ << ", "
       << predicate_ << ", " << object_ << ", perm=" << perm_index_ << ")\n";
}
