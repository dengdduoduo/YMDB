// Portions Copyright (c) YugabyteDB, Inc.

#include "yb/mdb/query/executor/binding_iter/docdb_scan.h"

#include <limits>

#include "graph_models/object_id.h"
#include "graph_models/rdf_model/rdf_object_id.h"
#include "query/executor/binding.h"
#include "yb/mdb/storage/docdb_key_encoder.h"
#include "yb/mdb/model/rdf_yb_model.h"

DocDBScan::DocDBScan(yb::mdb::DocDBRangeCache& cache, int perm_index,
                     std::array<std::unique_ptr<ScanRange>, 3> ranges,
                     int num_ranges,
                     Id subject, Id predicate, Id object,
                     bool subject_assigned, bool predicate_assigned, bool object_assigned,
                     bool check_c0_eq_c1, bool check_c1_eq_c2,
                     bool check_c0_eq_c2, yb::mdb::RdfYBModel* model,
                     int c0_var_id, yb::Status initial_status)
    : cache_(&cache), fallback_cache_(cache), model_(model), c0_var_id_(c0_var_id),
      perm_index_(perm_index),
      ranges_(std::move(ranges)), num_ranges_(num_ranges),
      subject_(subject), predicate_(predicate), object_(object),
      subject_assigned_(subject_assigned),
      predicate_assigned_(predicate_assigned),
      object_assigned_(object_assigned),
      check_c0_eq_c1_(check_c0_eq_c1),
      check_c1_eq_c2_(check_c1_eq_c2),
      check_c0_eq_c2_(check_c0_eq_c2) {
    set_status(std::move(initial_status));
}

void DocDBScan::SelectPartition() {
    cache_ = &fallback_cache_;
    if (model_ == nullptr || c0_var_id_ < 0 || !model_->UsePartitionLoad()) return;
    if (use_full_cache_ || model_->IsLoaded(perm_index_)) {
        use_full_cache_ = true;
        if (!model_->IsLoaded(perm_index_)) {
            auto status = model_->LoadFromDocDB(perm_index_);
            if (status.ok()) {
                model_->SetLoaded(perm_index_);
            } else {
                set_status(std::move(status));
            }
        }
        cache_ = &model_->GetCache(perm_index_);
        return;
    }
    uint64_t c0 = (*parent_binding_)[VarId(static_cast<uint32_t>(c0_var_id_))].id;
    auto it = g_pack_to_gmap.find(c0);
    if (it != g_pack_to_gmap.end()) c0 = it->second;
    auto& partition = model_->GetPartitionCache(perm_index_, c0);
    if (dynamic_partitions_.insert(c0).second) {
        ++dynamic_partition_count_;
    }
    if (dynamic_partition_count_ > kMaxDynamicPartitions) {
        use_full_cache_ = true;
        if (!model_->IsLoaded(perm_index_)) {
            auto status = model_->LoadFromDocDB(perm_index_);
            if (status.ok()) {
                model_->SetLoaded(perm_index_);
            } else {
                set_status(std::move(status));
            }
        }
        cache_ = &model_->GetCache(perm_index_);
        return;
    }
    auto status = model_->LoadPartition(perm_index_, c0);
    if (!status.ok()) {
        set_status(std::move(status));
    }
    cache_ = &partition;
}

void DocDBScan::_begin(Binding& parent_binding) {
    parent_binding_ = &parent_binding;
    first_call_ = true;
    row_index_ = 0;
    if (status().ok()) SelectPartition();
}

bool DocDBScan::_next() {
    if (!status().ok()) return false;
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
        use_c0_prefix_seek_ = num_ranges_ == 3 && min[0] == max[0];
        if (use_c0_prefix_seek_) {
            prefix_c0_ = min[0];
            prefix_max_c1_ = max[1];
            prefix_min_c2_ = min[2];
            prefix_max_c2_ = max[2];
            found = cache_->SeekC0PrefixWithIndex(
                prefix_c0_, min[1], prefix_max_c1_, prefix_min_c2_, prefix_max_c2_,
                &c0, &c1, &c2, &row_index_);
        } else {
            found = cache_->SeekRangeWithIndex(min[0], max[0], &c0, &c1, &c2, &row_index_);
        }
    } else {
        found = use_c0_prefix_seek_
            ? cache_->NextC0PrefixWithIndex(
                prefix_c0_, prefix_max_c1_, prefix_min_c2_, prefix_max_c2_,
                &c0, &c1, &c2, &row_index_)
            : cache_->NextWithIndex(max[0], &c0, &c1, &c2, &row_index_);
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

        found = use_c0_prefix_seek_
            ? cache_->NextC0PrefixWithIndex(
                prefix_c0_, prefix_max_c1_, prefix_min_c2_, prefix_max_c2_,
                &c0, &c1, &c2, &row_index_)
            : cache_->NextWithIndex(max[0], &c0, &c1, &c2, &row_index_);
    }

    return false;
}

void DocDBScan::_reset() {
    row_index_ = 0;
    first_call_ = true;
    use_c0_prefix_seek_ = false;
    if (status().ok()) SelectPartition();
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
