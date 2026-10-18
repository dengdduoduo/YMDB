// Portions Copyright (c) YugabyteDB, Inc.

#include "yb/mdb/query/optimizer/rdf_model/plan/triple_plan.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <utility>

#include "graph_models/rdf_model/rdf_model.h"
#include "graph_models/rdf_model/rdf_object_id.h"
#include "query/executor/binding_iter/docdb_scan.h"
#include "query/executor/binding_iter/scan_ranges/scan_range.h"
#include "query/exceptions.h"
#include "yb/mdb/model/rdf_yb_model.h"
#include "yb/mdb/query/optimizer/rdf_model/plan/docdb_leapfrog_plan.h"
#include "yb/mdb/storage/docdb_key_encoder.h"

namespace SPARQL {

TriplePlan::TriplePlan(
    Id subject, Id predicate, Id object, int tablet_perm_index, std::string tablet_id)
    : subject(subject),
      predicate(predicate),
      object(object),
      subject_assigned(subject.is_OID()),
      predicate_assigned(predicate.is_OID()),
      object_assigned(object.is_OID()),
      tablet_perm_index_(tablet_perm_index),
      tablet_id_(std::move(tablet_id)) {
  if (subject == predicate) {
    if (subject == object) {
      index = Index::EQUAL_SPO;
    } else {
      index = Index::EQUAL_SP;
    }
  } else if (subject == object) {
    index = Index::EQUAL_SO;
  } else if (predicate == object) {
    index = Index::EQUAL_PO;
  } else {
    index = Index::NORMAL;
  }
}

void TriplePlan::print(std::ostream& os, int indent) const {
    for (int i = 0; i < indent; ++i) {
        os << ' ';
    }
    os << "Triple(" << subject;
    os << ", " << predicate;
    os << ", " << object;
    os << ")";
    os << ", estimated cost: " << estimate_cost() << '\n';
}


int TriplePlan::scan_permutation() const {
    if (!tablet_id_.empty()) return tablet_perm_index_;
    if (index != Index::NORMAL) return 0;
    if (subject_assigned) return !predicate_assigned && object_assigned ? 2 : 0;
    if (predicate_assigned) return 1;
    return object_assigned ? 2 : 0;
}

double TriplePlan::estimate_cost() const {
    return estimate_cost_for_input(1);
}

double TriplePlan::estimate_cost_for_input(double input_rows) const {
    const auto& model = yb::mdb::RdfYBModel::Instance();
    const auto* stats = model.GetStatistics();
    if (stats == nullptr || !model.UsePartitionLoad()) {
        return (input_rows > 1 ? input_rows : 1) * estimate_output_size();
    }

    const int perm = scan_permutation();
    if (perm < 0 || perm >= 6) return estimate_output_size();
    std::array<uint64_t, 3> columns;
    yb::mdb::DocKeyEncoder::Permute(perm, 0, 1, 2, &columns[0], &columns[1], &columns[2]);
    const size_t position = columns[0];
    const Id* terms[] = {&subject, &predicate, &object};
    const bool assigned[] = {subject_assigned, predicate_assigned, object_assigned};
    const auto& frequencies = stats->term_counts[position];
    const double rows = static_cast<double>(stats->rows);
    const double distinct = static_cast<double>(std::max<size_t>(1, frequencies.size()));
    const double probes = std::max(0.0, input_rows);
    double candidates = rows;
    if (assigned[position]) candidates = rows / distinct;
    if (terms[position]->is_OID()) {
        auto value = terms[position]->get_OID().id;
        const auto mapped = g_pack_to_gmap.find(value);
        if (mapped != g_pack_to_gmap.end()) value = mapped->second;
        const auto found = frequencies.find(value);
        candidates = found == frequencies.end() ? 0 : static_cast<double>(found->second);
    }

    double load_cost;
    if (!tablet_id_.empty()) {
        // No per-tablet histogram is available; use an index-size upper bound.
        // LoadTablet happens once when the iterator is constructed, not per binding.
        load_cost = rows + 1;
    } else if (terms[position]->is_OID()) {
        // An empty fixed partition currently falls back to a full index load.
        load_cost = (candidates == 0 ? rows : candidates) + 1;
    } else if (assigned[position]) {
        const double partitions = std::min(probes, distinct);
        const double limit = static_cast<double>(DocDBScan::kMaxDynamicPartitions);
        load_cost = std::min(partitions, limit) * (candidates + 1);
        if (partitions > limit) load_cost += rows + 1;
    } else {
        load_cost = rows + 1;
    }
    // SeekRange bounds c0 only. Later columns are checked while visiting its rows.
    return load_cost + probes * (1 + candidates + estimate_output_size());
}


bool TriplePlan::estimate_index_join(
    const TriplePlan& input, double* output_rows, double* rhs_cost) const {
    const auto& model = yb::mdb::RdfYBModel::Instance();
    const auto* stats = model.GetStatistics();
    if (stats == nullptr || !model.UsePartitionLoad() || model.HasTransaction()) return false;
    if (!input.subject.is_var() || !input.object.is_var() || !input.predicate.is_OID()
        || input.subject_assigned || input.object_assigned || input.subject == input.object
        || !subject.is_var() || !object.is_var() || !predicate.is_OID() || subject == object
        || !tablet_id_.empty()) return false;
    if (!input.tablet_id_.empty() && input.tablet_perm_index_ != 1) return false;
    const bool forward = input.object == subject && input.subject != object
        && subject_assigned && !object_assigned;
    const bool reverse = input.subject == object && input.object != subject
        && object_assigned && !subject_assigned;
    if (!forward && !reverse) return false;
    const auto storage_id = [](Id term) {
        const auto id = term.get_OID().id;
        const auto mapped = g_pack_to_gmap.find(id);
        return mapped == g_pack_to_gmap.end() ? id : mapped->second;
    };
    const auto& input_pairs = forward ? stats->predicate_object_counts
                                     : stats->predicate_subject_counts;
    const auto keys = input_pairs.find(storage_id(input.predicate));
    if (keys == input_pairs.end()) return false;
    const auto& output_pairs = forward ? stats->predicate_subject_counts
                                      : stats->predicate_object_counts;
    const auto matches = output_pairs.find(storage_id(predicate));
    double probes = 0, joined = 0, partition_load = 0;
    for (const auto& [key, frequency] : keys->second) {
        probes += frequency;
        if (matches != output_pairs.end()) {
          const auto found = matches->second.find(key);
          if (found != matches->second.end())
            joined += static_cast<double>(frequency) * found->second;
        }
        if (forward) {
            const auto subject = stats->term_counts[0].find(key);
            // Nonempty partitions cache once; empty ones are reread per binding.
            partition_load += subject == stats->term_counts[0].end()
                ? static_cast<double>(frequency) : static_cast<double>(subject->second) + 1;
        }
    }
    double load_cost = 0;
    if (probes > 0 && forward && !model.IsLoaded(0)) {
        const double distinct = keys->second.size();
        const double limit = DocDBScan::kMaxDynamicPartitions;
        // The first 64 keys and partial-cache reuse are unknown. Mean load work
        // is heuristic; the complete key intersection above is not order-dependent.
        load_cost = partition_load * std::min(1.0, limit / distinct);
        if (distinct > limit) load_cost += static_cast<double>(stats->rows) + 1;
    } else if (probes > 0 && reverse) {
        const auto count = stats->term_counts[1].find(storage_id(predicate));
        load_cost = count == stats->term_counts[1].end()
            ? (model.IsLoaded(1) ? 1 : static_cast<double>(stats->rows) + 2)
            : static_cast<double>(count->second) + 1;
    }
    *output_rows = joined;
    // Exact P/S or P/O prefix seeks visit matches, then materialize their rows.
    *rhs_cost = load_cost + probes + 2 * joined;
    return true;
}

double TriplePlan::estimate_output_size() const {
    const auto version = yb::mdb::RdfYBModel::Instance().StatisticsVersion();
    if (!cached_output_estimation_is_valid || cached_statistics_version != version) {
        cached_statistics_version = version;
        cached_output_estimation = _estimate_output_size();
        cached_output_estimation_is_valid = true;
    }

    return cached_output_estimation;
}


double TriplePlan::_estimate_output_size() const {
    if (yb::mdb::RdfYBModel::Instance().GetStatistics() != nullptr) {
        return estimate_with_docdb();
    }
    constexpr double H1 = 0.01;
    constexpr double H2 = 0.00001;
    constexpr double H3 = 0.0000001;

    int terms = 0;
    int assigned_vars = 0;

    terms += static_cast<int>(subject.is_OID());
    terms += static_cast<int>(predicate.is_OID());
    terms += static_cast<int>(object.is_OID());

    assigned_vars += static_cast<int>(subject.is_var() && subject_assigned);
    assigned_vars += static_cast<int>(predicate.is_var() && predicate_assigned);
    assigned_vars += static_cast<int>(object.is_var() && object_assigned);

    // Uncollected statistics are unknown, not proof of an empty relation.
    const auto& catalog = rdf_model.catalog;

    switch (terms) {
    case 3: {
        return 1;
    }
    case 2: {
        auto docdb_estimation = estimate_with_docdb();
        if (assigned_vars == 1) {
            return docdb_estimation * H1;
        } else {
            return docdb_estimation;
        }
    }
    case 1: {
        double predicate_count;
        if (predicate.is_OID()) {
          predicate_count = static_cast<double>(
              catalog.get_predicate_count(predicate.get_OID().id).value_or(1000));
        } else {
          predicate_count = estimate_with_docdb();
        }
        switch (assigned_vars) {
        case 2:
            return predicate_count * H2;
        case 1:
            return predicate_count * H1;
        default:
            return predicate_count;
        }
    }
    case 0: {
        double total_count;
        switch (index) {
        case Index::EQUAL_SPO:
            total_count = static_cast<double>(catalog.get_equal_spo_count().value_or(1000));
            break;
        case Index::EQUAL_SP:
            total_count = static_cast<double>(catalog.get_equal_sp_count().value_or(1000));
            break;
        case Index::EQUAL_SO:
            total_count = static_cast<double>(catalog.get_equal_so_count().value_or(1000));
            break;
        case Index::EQUAL_PO:
            total_count = static_cast<double>(catalog.get_equal_po_count().value_or(1000));
            break;
        default:
            total_count = static_cast<double>(catalog.get_triples_count().value_or(1000));
            break;
        }
        switch (assigned_vars) {
        case 3:
            return H3;
        case 2:
            return total_count * H2;
        case 1:
            return total_count * H1;
        default:
            return total_count;
        }
    }
    }

    assert(false);
    return 0;
}

double TriplePlan::estimate_with_docdb() const {
    const auto* stats = yb::mdb::RdfYBModel::Instance().GetStatistics();
    if (stats == nullptr) return 1000.0;
    if (stats->rows == 0) return 0;

    const Id* terms[] = {&subject, &predicate, &object};
    const bool assigned[] = {subject_assigned, predicate_assigned, object_assigned};
    uint64_t values[3]{};
    for (size_t i = 0; i < 3; ++i) {
        if (!terms[i]->is_OID()) continue;
        values[i] = terms[i]->get_OID().id;
        const auto mapped = g_pack_to_gmap.find(values[i]);
        if (mapped != g_pack_to_gmap.end()) values[i] = mapped->second;
    }

    double result = static_cast<double>(stats->rows);
    const bool fixed_po = predicate.is_OID() && object.is_OID();
    const bool fixed_sp = subject.is_OID() && predicate.is_OID() && !fixed_po;
    if (fixed_po) {
        const auto predicate_it = stats->predicate_object_counts.find(values[1]);
        if (predicate_it == stats->predicate_object_counts.end()) return 0;
        const auto object_it = predicate_it->second.find(values[2]);
        if (object_it == predicate_it->second.end()) return 0;
        result = static_cast<double>(object_it->second);
    } else if (fixed_sp) {
        const auto predicate_it = stats->predicate_subject_counts.find(values[1]);
        if (predicate_it == stats->predicate_subject_counts.end()) return 0;
        const auto subject_it = predicate_it->second.find(values[0]);
        if (subject_it == predicate_it->second.end()) return 0;
        result = static_cast<double>(subject_it->second);
    } else {
        if (index == Index::EQUAL_SP) result = stats->equal_rows[0];
        if (index == Index::EQUAL_SO) result = stats->equal_rows[1];
        if (index == Index::EQUAL_PO) result = stats->equal_rows[2];
        if (index == Index::EQUAL_SPO) result = stats->equal_rows[3];
    }
    std::set<VarId> bound_variables;
    int constants = 0;
    for (size_t i = 0; i < 3; ++i) {
        if (terms[i]->is_OID()) {
            ++constants;
            // The selected joint frequency already includes its two constants,
            // even when their values are equal. Fully bound triples keep P/O.
            if ((fixed_po && i != 0) || (fixed_sp && i != 2)) continue;
            const auto it = stats->term_counts[i].find(values[i]);
            if (it == stats->term_counts[i].end()) return 0;
            // Remaining constant/equality combinations assume independence.
            result *= static_cast<double>(it->second) / stats->rows;
        } else if (assigned[i] && bound_variables.insert(terms[i]->get_var()).second) {
            result /= std::max<size_t>(1, stats->term_counts[i].size());
        }
    }
    if (constants == 3) result = std::min(1.0, result);
    return result;
}

void TriplePlan::set_input_vars(const std::set<VarId>& input_vars) {
    auto previous_assigned_count = static_cast<int>(subject_assigned)
                                 + static_cast<int>(predicate_assigned)
                                 + static_cast<int>(object_assigned);

    set_input_var(input_vars, subject,   &subject_assigned);
    set_input_var(input_vars, predicate, &predicate_assigned);
    set_input_var(input_vars, object,    &object_assigned);

    auto after_assigned_count = static_cast<int>(subject_assigned)
                              + static_cast<int>(predicate_assigned)
                              + static_cast<int>(object_assigned);

    if (previous_assigned_count != after_assigned_count) {
        cached_output_estimation_is_valid = false;
    }
}


std::set<VarId> TriplePlan::get_vars() const {
    std::set<VarId> result;
    if (subject.is_var() && !subject_assigned) {
        result.insert(subject.get_var());
    }
    if (predicate.is_var() && !predicate_assigned) {
        result.insert(predicate.get_var());
    }
    if (object.is_var() && !object_assigned) {
        result.insert(object.get_var());
    }

    return result;
}

std::unique_ptr<BindingIter> TriplePlan::get_binding_iter() const {
    auto& model = yb::mdb::RdfYBModel::Instance();
    std::array<std::unique_ptr<ScanRange>, 3> ranges;
    bool check_c0_eq_c1 = false;
    bool check_c1_eq_c2 = false;
    bool check_c0_eq_c2 = false;
    int perm_index = scan_permutation();

    if (!tablet_id_.empty()) {
        if (tablet_perm_index_ < 0 || tablet_perm_index_ >= 6) {
            throw LogicException("Invalid tablet scan permutation");
        }
        // Join reordering can bind new variables, but the anchor must keep the
        // fragment's physical table so its tablet constraint cannot be bypassed.
        const std::array<Id, 3> terms{subject, predicate, object};
        const std::array<bool, 3> assigned{
            subject_assigned, predicate_assigned, object_assigned};
        std::array<uint64_t, 3> columns;
        yb::mdb::DocKeyEncoder::Permute(
            perm_index, 0, 1, 2, &columns[0], &columns[1], &columns[2]);
        for (size_t i = 0; i < ranges.size(); ++i) {
            ranges[i] = ScanRange::get(terms[columns[i]], assigned[columns[i]]);
        }
        check_c0_eq_c1 = terms[columns[0]] == terms[columns[1]];
        check_c1_eq_c2 = terms[columns[1]] == terms[columns[2]];
        check_c0_eq_c2 = terms[columns[0]] == terms[columns[2]];
    } else {
      switch (index) {
    case Index::EQUAL_SPO:
        ranges[0] = ScanRange::get(subject, subject_assigned);
        ranges[1] = ScanRange::get(subject, subject_assigned);
        ranges[2] = ScanRange::get(subject, subject_assigned);
        check_c0_eq_c1 = true;
        check_c1_eq_c2 = true;
        break;

    case Index::EQUAL_SP:
        ranges[0] = ScanRange::get(subject, subject_assigned);
        ranges[1] = ScanRange::get(subject, subject_assigned);
        ranges[2] = ScanRange::get(object, object_assigned);
        check_c0_eq_c1 = true;
        break;

    case Index::EQUAL_SO:
        ranges[0] = ScanRange::get(subject, subject_assigned);
        ranges[1] = ScanRange::get(predicate, predicate_assigned);
        ranges[2] = ScanRange::get(subject, subject_assigned);
        check_c0_eq_c2 = true;
        break;

    case Index::EQUAL_PO:
        ranges[0] = ScanRange::get(subject, subject_assigned);
        ranges[1] = ScanRange::get(predicate, predicate_assigned);
        ranges[2] = ScanRange::get(predicate, predicate_assigned);
        check_c1_eq_c2 = true;
        break;

    case Index::NORMAL:
        if (subject_assigned) {
            if (predicate_assigned) {
                ranges[0] = ScanRange::get(subject, subject_assigned);
                ranges[1] = ScanRange::get(predicate, predicate_assigned);
                ranges[2] = ScanRange::get(object, object_assigned);
            } else {
                if (object_assigned) {
                    ranges[0] = ScanRange::get(object, object_assigned);
                    ranges[1] = ScanRange::get(subject, subject_assigned);
                    ranges[2] = ScanRange::get(predicate, predicate_assigned);
                } else {
                    ranges[0] = ScanRange::get(subject, subject_assigned);
                    ranges[1] = ScanRange::get(predicate, predicate_assigned);
                    ranges[2] = ScanRange::get(object, object_assigned);
                }
            }
        } else {
            if (predicate_assigned) {
                ranges[0] = ScanRange::get(predicate, predicate_assigned);
                ranges[1] = ScanRange::get(object, object_assigned);
                ranges[2] = ScanRange::get(subject, subject_assigned);
            } else {
                if (object_assigned) {
                    ranges[0] = ScanRange::get(object, object_assigned);
                    ranges[1] = ScanRange::get(subject, subject_assigned);
                    ranges[2] = ScanRange::get(predicate, predicate_assigned);
                } else {
                    ranges[0] = ScanRange::get(subject, subject_assigned);
                    ranges[1] = ScanRange::get(predicate, predicate_assigned);
                    ranges[2] = ScanRange::get(object, object_assigned);
                }
            }
        }
        break;
    }
    }

    yb::mdb::DocDBRangeCache* cache = nullptr;
    yb::Status load_status = yb::Status::OK();
    int dynamic_c0_var_id = -1;
    if (tablet_id_.empty()) {
        const Id* c0_term = nullptr;
        bool c0_assigned = false;
        if (perm_index == 0) {
          c0_term = &subject;
          c0_assigned = subject_assigned;
        } else if (perm_index == 1) {
          c0_term = &predicate;
          c0_assigned = predicate_assigned;
        } else if (perm_index == 2) {
          c0_term = &object;
          c0_assigned = object_assigned;
        }
        if (c0_assigned && c0_term != nullptr && c0_term->is_var()) {
          dynamic_c0_var_id = c0_term->get_var().id;
        }
    }

    uint64_t assigned_c0 = 0;
    bool c0_known = false;
    switch (perm_index) {
    case 0:
        if (subject_assigned && subject.is_OID()) {
            assigned_c0 = subject.get_OID().id;
            c0_known = true;
        }
        break;
    case 1:
        if (predicate_assigned && predicate.is_OID()) {
            assigned_c0 = predicate.get_OID().id;
            c0_known = true;
        }
        break;
    case 2:
        if (object_assigned && object.is_OID()) {
            assigned_c0 = object.get_OID().id;
            c0_known = true;
        }
        break;
    }

    if (!tablet_id_.empty() && tablet_perm_index_ == perm_index) {
        load_status = model.LoadTablet(perm_index, tablet_id_);
        cache = &model.GetTabletCache(perm_index, tablet_id_);
    }

    if (cache == nullptr && load_status.ok() && c0_known && model.UsePartitionLoad()) {
        load_status = model.LoadPartition(perm_index, assigned_c0);
        if (load_status.ok() && model.GetPartitionCache(perm_index, assigned_c0).Size() > 0) {
            cache = &model.GetPartitionCache(perm_index, assigned_c0);
        }
    }

    // A failed targeted read must not fall back to a seemingly successful empty scan.
    if (cache == nullptr && load_status.ok() && dynamic_c0_var_id < 0) {
        if (!model.IsLoaded(perm_index)) {
            load_status = model.LoadFromDocDB(perm_index);
            if (load_status.ok()) {
                model.SetLoaded(perm_index);
            }
        }
        cache = &model.GetCache(perm_index);
    }
    if (cache == nullptr) cache = &model.GetCache(perm_index);

    return std::make_unique<DocDBScan>(
        *cache, perm_index,
        std::move(ranges), 3,
        subject, predicate, object,
        subject_assigned, predicate_assigned, object_assigned,
        check_c0_eq_c1, check_c1_eq_c2, check_c0_eq_c2,
        &model, dynamic_c0_var_id, std::move(load_status));
}

bool TriplePlan::get_leapfrog_iter(
    std::vector<std::unique_ptr<::LeapfrogIter>>& scans,
    std::vector<VarId>& order,
    uint_fast32_t& enumeration_level) const {
    return AppendDocDBLeapfrogScan(*this, &scans, order, enumeration_level);
}

}  // namespace SPARQL
