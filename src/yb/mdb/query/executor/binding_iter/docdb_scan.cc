#include "docdb_scan.h"

#include <limits>

#include "graph_models/object_id.h"
#include "graph_models/rdf_model/rdf_object_id.h"
#include "query/executor/binding.h"
#include "yb/mdb/storage/docdb_key_encoder.h"

DocDBScan::DocDBScan(yb::mdb::DocDBRangeCache& cache, int perm_index,
                     std::array<std::unique_ptr<ScanRange>, 3> ranges,
                     int num_ranges,
                     Id subject, Id predicate, Id object,
                     bool subject_assigned, bool predicate_assigned, bool object_assigned,
                     bool check_c0_eq_c1, bool check_c1_eq_c2,
                     bool check_c0_eq_c2)
    : cache_(cache), perm_index_(perm_index),
      ranges_(std::move(ranges)), num_ranges_(num_ranges),
      subject_(subject), predicate_(predicate), object_(object),
      subject_assigned_(subject_assigned),
      predicate_assigned_(predicate_assigned),
      object_assigned_(object_assigned),
      check_c0_eq_c1_(check_c0_eq_c1),
      check_c1_eq_c2_(check_c1_eq_c2),
      check_c0_eq_c2_(check_c0_eq_c2) {}

void DocDBScan::_begin(Binding& parent_binding) {
    parent_binding_ = &parent_binding;
    first_call_ = true;
    row_index_ = 0;
}

bool DocDBScan::_next() {
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
    if (first_call_) {
        first_call_ = false;
        found = cache_.SeekRangeWithIndex(min[0], max[0], &c0, &c1, &c2, &row_index_);
    } else {
        found = cache_.NextWithIndex(max[0], &c0, &c1, &c2, &row_index_);
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

            if (subject_.is_var() && !subject_assigned_) {
                auto soid = g_gmap_to_pack.count(s) ? g_gmap_to_pack[s] : ObjectId(s);
                parent_binding_->add(subject_.get_var(), soid);
            }
            if (predicate_.is_var() && !predicate_assigned_) {
                auto poid = g_gmap_to_pack.count(p) ? g_gmap_to_pack[p] : ObjectId(p);
                parent_binding_->add(predicate_.get_var(), poid);
            }
            if (object_.is_var() && !object_assigned_) {
                auto ooid = g_gmap_to_pack.count(o) ? g_gmap_to_pack[o] : ObjectId(o);
                parent_binding_->add(object_.get_var(), ooid);
            }
            return true;
        }

        found = cache_.NextWithIndex(max[0], &c0, &c1, &c2, &row_index_);
    }

    return false;
}

void DocDBScan::_reset() {
    row_index_ = 0;
    first_call_ = true;
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
