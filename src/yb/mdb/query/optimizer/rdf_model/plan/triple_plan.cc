#include "triple_plan.h"

#include <cassert>
#include <algorithm>
#include "graph_models/rdf_model/rdf_object_id.h"
#include "yb/mdb/storage/docdb_read_context.h"

#include "graph_models/rdf_model/rdf_model.h"
#include "query/executor/binding_iter/docdb_scan.h"
#include "query/executor/binding_iter/scan_ranges/scan_range.h"
#include "query/exceptions.h"
#include "yb/mdb/model/rdf_yb_model.h"

using namespace SPARQL;

TriplePlan::TriplePlan(Id subject, Id predicate, Id object) :
    subject            (subject),
    predicate          (predicate),
    object             (object),
    subject_assigned   (subject.is_OID()),
    predicate_assigned (predicate.is_OID()),
    object_assigned    (object.is_OID())
{
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


double TriplePlan::estimate_cost() const {
    return estimate_output_size();
}


double TriplePlan::estimate_output_size() const {
    if (!cached_output_estimation_is_valid) {
        cached_output_estimation = _estimate_output_size();
        cached_output_estimation_is_valid = true;
    }

    return cached_output_estimation;
}


double TriplePlan::_estimate_output_size() const {
    if (auto* context = yb::mdb::DocDBReadContextScope::Current()) {
        double count = context->statistics ? context->statistics->triples : 1000.0;
        if (predicate.is_OID() && context->statistics) {
            const auto packed = predicate.get_OID().id;
            const auto id = g_pack_to_gmap.find(packed);
            const auto logical = id == g_pack_to_gmap.end() ? packed : id->second;
            const auto found = context->statistics->predicates.find(logical);
            if (found != context->statistics->predicates.end()) count = found->second;
            else if (context->statistics->predicates_complete) count = 0;
        }
        if (subject_assigned) count /= context->statistics
            ? std::max(1.0, context->statistics->subjects.Estimate()) : 100.0;
        if (object_assigned) count /= context->statistics
            ? std::max(1.0, context->statistics->objects.Estimate()) : 100.0;
        if (subject_assigned && predicate_assigned && object_assigned) return std::min(1.0, count);
        return count;
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
            predicate_count = static_cast<double>(catalog.get_predicate_count(predicate.get_OID().id));
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
            total_count = static_cast<double>(catalog.get_equal_spo_count());
            break;
        case Index::EQUAL_SP:
            total_count = static_cast<double>(catalog.get_equal_sp_count());
            break;
        case Index::EQUAL_SO:
            total_count = static_cast<double>(catalog.get_equal_so_count());
            break;
        case Index::EQUAL_PO:
            total_count = static_cast<double>(catalog.get_equal_po_count());
            break;
        default:
            total_count = static_cast<double>(catalog.get_triples_count());
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
    return 1000.0;
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
    int perm_index = 0;

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
                    perm_index = 2;
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
                perm_index = 1;
            } else {
                if (object_assigned) {
                    ranges[0] = ScanRange::get(object, object_assigned);
                    ranges[1] = ScanRange::get(subject, subject_assigned);
                    ranges[2] = ScanRange::get(predicate, predicate_assigned);
                    perm_index = 2;
                } else {
                    ranges[0] = ScanRange::get(subject, subject_assigned);
                    ranges[1] = ScanRange::get(predicate, predicate_assigned);
                    ranges[2] = ScanRange::get(object, object_assigned);
                }
            }
        }
        break;
    }

    yb::mdb::DocDBRangeCache* cache = nullptr;

    if (model.IsConnected() && model.UseStreamingReads()) {
        auto* read_context = yb::mdb::DocDBReadContextScope::Current();
        if (!read_context) throw QueryException("Streaming scan requires a query read context");
        return std::make_unique<DocDBScan>(
            nullptr, perm_index, std::move(ranges), 3,
            subject, predicate, object, subject_assigned, predicate_assigned, object_assigned,
            check_c0_eq_c1, check_c1_eq_c2, check_c0_eq_c2,
            model.GetTable(perm_index), read_context);
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

    if (c0_known && model.UsePartitionLoad()) {
        (void)model.LoadPartition(perm_index, assigned_c0);
        if (model.GetPartitionCache(perm_index, assigned_c0).Size() > 0) {
            cache = &model.GetPartitionCache(perm_index, assigned_c0);
        }
    }

    if (cache == nullptr) {
        if (!model.IsLoaded(perm_index)) {
            (void)model.LoadFromDocDB(perm_index);
            model.SetLoaded(perm_index);
        }
        cache = &model.GetCache(perm_index);
    }

    return std::make_unique<DocDBScan>(
        cache, perm_index,
        std::move(ranges), 3,
        subject, predicate, object,
        subject_assigned, predicate_assigned, object_assigned,
        check_c0_eq_c1, check_c1_eq_c2, check_c0_eq_c2);
}

bool TriplePlan::get_leapfrog_iter(std::vector<std::unique_ptr<::LeapfrogIter>>&,
                                    std::vector<VarId>&,
                                    uint_fast32_t&) const {
    return false;
}
