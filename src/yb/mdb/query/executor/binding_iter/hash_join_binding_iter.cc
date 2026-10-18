// Portions Copyright (c) YugabyteDB, Inc.

#include "yb/mdb/query/executor/binding_iter/hash_join_binding_iter.h"

#include <algorithm>
#include <functional>
#include <numeric>

#include "yb/util/flags.h"

DEFINE_NON_RUNTIME_uint64(mdb_hash_join_max_build_bytes,
    HashJoinBindingIter::kDefaultMaxBuildBytes,
    "Per-HashJoin requested byte limit for build/probe containers; excludes child caches");

using yb::Status;

size_t HashJoinBindingIter::DefaultMaxBuildBytes() {
    return FLAGS_mdb_hash_join_max_build_bytes;
}

void* HashJoinBindingIter::BuildResource::do_allocate(size_t size, size_t alignment) {
    if (size > limit_ - bytes) throw LimitExceeded{};
    void* ptr = std::pmr::new_delete_resource()->allocate(size, alignment);
    bytes += size;
    peak = std::max(peak, bytes);
    return ptr;
}

void HashJoinBindingIter::BuildResource::do_deallocate(
    void* ptr, size_t size, size_t alignment) {
    std::pmr::new_delete_resource()->deallocate(ptr, size, alignment);
    bytes -= size;
}

HashJoinBindingIter::HashJoinBindingIter(
    std::unique_ptr<BindingIter> lhs, std::unique_ptr<BindingIter> rhs,
    std::set<VarId> lhs_vars, std::set<VarId> rhs_vars, size_t max_build_rows,
    size_t max_build_bytes)
    : lhs_(std::move(lhs)), rhs_(std::move(rhs)),
      lhs_vars_(lhs_vars.begin(), lhs_vars.end()),
      rhs_vars_(rhs_vars.begin(), rhs_vars.end()), max_build_rows_(max_build_rows),
      memory_(max_build_bytes), rows_(&memory_), buckets_(&memory_),
      unbound_rows_(&memory_), candidates_(&memory_) {
    for (size_t i = 0; i < rhs_vars_.size(); ++i) {
        if (lhs_vars.count(rhs_vars_[i])) {
            join_vars_.push_back(rhs_vars_[i]);
            join_positions_.push_back(i);
        }
    }
}

size_t HashJoinBindingIter::KeyHash::operator()(const Key& key) const {
    size_t result = 0;
    for (auto value : key) {
        result ^= std::hash<uint64_t>{}(value) + 0x9e3779b9 + (result << 6) + (result >> 2);
    }
    return result;
}

HashJoinBindingIter::Key HashJoinBindingIter::MakeKey(const Binding& binding) {
    Key key(&memory_);
    key.reserve(join_vars_.size());
    for (auto var : join_vars_) {
        key.push_back(binding[var].id);
    }
    return key;
}

void HashJoinBindingIter::ClearBuild() {
    // clear() retains capacities; swap with empty containers releases the budget.
    decltype(rows_)(&memory_).swap(rows_);
    decltype(buckets_)(&memory_).swap(buckets_);
    decltype(unbound_rows_)(&memory_).swap(unbound_rows_);
    decltype(candidates_)(&memory_).swap(candidates_);
    candidate_index_ = 0;
    exhausted_ = true;
}

void HashJoinBindingIter::_begin(Binding& parent_binding) {
    parent_binding_ = &parent_binding;
    lhs_binding_ = std::make_unique<Binding>(parent_binding.size);
    rhs_binding_ = std::make_unique<Binding>(parent_binding.size);
    Start();
}

void HashJoinBindingIter::Start() {
    ClearBuild();
    if (!status().ok()) return;
    memory_.peak = 0;
    lhs_binding_->add_all(*parent_binding_);
    rhs_binding_->add_all(*parent_binding_);
    for (auto var : lhs_vars_) lhs_binding_->add(var, ObjectId::get_null());
    for (auto var : rhs_vars_) rhs_binding_->add(var, ObjectId::get_null());

    try {
        rhs_->begin(*rhs_binding_);
        set_status_from(*rhs_);
        if (!status().ok()) return;
        while (rhs_->next()) {
            set_status_from(*rhs_);
            if (!status().ok()) break;
            if (rows_.size() >= max_build_rows_) {
                set_status(STATUS(RuntimeError, "HashJoin build row limit exceeded"));
                break;
            }
            Row row(&memory_);
            row.reserve(rhs_vars_.size());
            for (auto var : rhs_vars_) row.push_back((*rhs_binding_)[var]);
            auto key = MakeKey(*rhs_binding_);
            const size_t index = rows_.size();
            rows_.push_back(std::move(row));
            if (std::find(key.begin(), key.end(), ObjectId::NULL_ID) != key.end()) {
                unbound_rows_.push_back(index);
            } else {
                buckets_[std::move(key)].push_back(index);
            }
        }
        set_status_from(*rhs_);
    } catch (const BuildResource::LimitExceeded&) {
        set_status(STATUS(RuntimeError, "HashJoin build byte limit exceeded"));
    }
    if (!status().ok()) {
        ClearBuild();
        return;
    }
    if (rows_.empty()) return;
    lhs_->begin(*lhs_binding_);
    set_status_from(*lhs_);
    if (!status().ok()) {
        ClearBuild();
        return;
    }
    exhausted_ = false;
}

bool HashJoinBindingIter::Compatible(const Row& row) const {
    for (size_t i = 0; i < join_vars_.size(); ++i) {
        const auto left = (*lhs_binding_)[join_vars_[i]];
        const auto right = row[join_positions_[i]];
        if (!left.is_null() && !right.is_null() && left != right) return false;
    }
    return true;
}

bool HashJoinBindingIter::_next() {
    if (!status().ok() || exhausted_) return false;
    while (true) {
        while (candidate_index_ < candidates_.size()) {
            const auto& row = rows_[candidates_[candidate_index_++]];
            if (!Compatible(row)) continue;
            for (auto var : lhs_vars_) parent_binding_->add(var, (*lhs_binding_)[var]);
            for (size_t i = 0; i < rhs_vars_.size(); ++i) {
                const auto var = rhs_vars_[i];
                // A null RHS key must not overwrite a bound LHS value.
                if (!row[i].is_null() || (*parent_binding_)[var].is_null() ||
                    !std::binary_search(lhs_vars_.begin(), lhs_vars_.end(), var)) {
                    parent_binding_->add(var, row[i]);
                }
            }
            return true;
        }
        const bool found = lhs_->next();
        set_status_from(*lhs_);
        if (!status().ok() || !found) {
            ClearBuild();
            return false;
        }
        try {
            candidates_.clear();
            candidate_index_ = 0;
            const auto key = MakeKey(*lhs_binding_);
            if (std::find(key.begin(), key.end(), ObjectId::NULL_ID) != key.end()) {
                candidates_.resize(rows_.size());
                std::iota(candidates_.begin(), candidates_.end(), size_t{0});
            } else {
                const auto it = buckets_.find(key);
                if (it != buckets_.end()) candidates_ = it->second;
                candidates_.insert(candidates_.end(), unbound_rows_.begin(), unbound_rows_.end());
            }
        } catch (const BuildResource::LimitExceeded&) {
            set_status(STATUS(RuntimeError, "HashJoin build byte limit exceeded during probe"));
            ClearBuild();
            return false;
        }
    }
}

void HashJoinBindingIter::_reset() {
    // Outer inputs may change between resets; the build must be evaluated again.
    Start();
}

void HashJoinBindingIter::assign_nulls() {
    for (auto var : lhs_vars_) parent_binding_->add(var, ObjectId::get_null());
    for (auto var : rhs_vars_) parent_binding_->add(var, ObjectId::get_null());
}

void HashJoinBindingIter::print(std::ostream& os, int indent, bool stats) const {
    if (stats) print_generic_stats(os, indent);
    os << std::string(indent, ' ') << "HashJoin(build_row_limit=" << max_build_rows_
       << ", build_byte_limit=" << memory_.limit_ << ")\n";
    lhs_->print(os, indent + 2, stats);
    rhs_->print(os, indent + 2, stats);
}


HashJoinBindingIter::BuildMemory HashJoinBindingIter::build_memory() const {
    BuildMemory memory;
    memory.rows = rows_.size();
    memory.key_buckets = buckets_.size();
    memory.tracked_bytes = memory_.bytes;
    memory.peak_tracked_bytes = memory_.peak;
    memory.byte_limit = memory_.limit_;
    memory.accounted_bytes = sizeof(*this)
        + rows_.capacity() * sizeof(decltype(rows_)::value_type)
        + buckets_.bucket_count() * sizeof(void*)
        + unbound_rows_.capacity() * sizeof(size_t)
        + candidates_.capacity() * sizeof(size_t)
        + lhs_vars_.capacity() * sizeof(VarId)
        + rhs_vars_.capacity() * sizeof(VarId)
        + join_vars_.capacity() * sizeof(VarId)
        + join_positions_.capacity() * sizeof(size_t);
    for (const auto& row : rows_) {
        memory.payload_bytes += row.size() * sizeof(ObjectId);
        memory.accounted_bytes += row.capacity() * sizeof(ObjectId);
    }
    for (const auto& bucket : buckets_) {
        memory.accounted_bytes += sizeof(decltype(buckets_)::value_type)
            + bucket.first.capacity() * sizeof(uint64_t)
            + bucket.second.capacity() * sizeof(size_t);
    }
    if (lhs_binding_)
      memory.accounted_bytes += sizeof(Binding) + lhs_binding_->size * sizeof(ObjectId);
    if (rhs_binding_)
      memory.accounted_bytes += sizeof(Binding) + rhs_binding_->size * sizeof(ObjectId);
    return memory;
}
