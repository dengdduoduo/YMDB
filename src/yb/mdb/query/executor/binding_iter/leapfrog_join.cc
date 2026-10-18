// Portions Copyright (c) YugabyteDB, Inc.

#include "yb/mdb/query/executor/binding_iter/leapfrog_join.h"

#include <algorithm>
#include <set>

using yb::Status;

bool LeapfrogJoin::Observe(LeapfrogIter& iter) {
  set_status(iter.status());
  if (!status().ok()) level = -1;
  return status().ok();
}

bool LeapfrogJoin::CheckInterruption() {
  for (const auto& iter : leapfrog_iters) {
    if (!iter) {
      set_status(STATUS(InvalidArgument, "Null Leapfrog child"));
      level = -1;
      return false;
    }
    if (iter->interruption_requested && *iter->interruption_requested) {
      set_status(STATUS(Aborted, "Leapfrog join interrupted"));
      level = -1;
      return false;
    }
  }
  return status().ok();
}

bool LeapfrogJoin::ValidateLayout() {
  if (leapfrog_iters.empty() || enumeration_level < 0 ||
      static_cast<size_t>(enumeration_level) > var_order.size()) {
    set_status(STATUS(InvalidArgument, "Invalid Leapfrog join layout"));
    return false;
  }
  std::set<VarId> variables, enumerated;
  for (const auto var : var_order) {
    if (var.id >= parent_binding->size || !variables.insert(var).second) {
      set_status(STATUS(InvalidArgument, "Invalid or repeated Leapfrog output variable"));
      return false;
    }
  }
  iters_for_var.resize(enumeration_level);
  for (auto& iter : leapfrog_iters) {
    if (!iter) {
      set_status(STATUS(InvalidArgument, "Null Leapfrog child"));
      return false;
    }
    int previous = -1;
    for (const auto var : iter->get_intersection_vars()) {
      const auto found = std::find(var_order.begin(), var_order.end(), var);
      const int position = static_cast<int>(found - var_order.begin());
      if (found == var_order.end() || position >= enumeration_level || position <= previous) {
        set_status(STATUS(InvalidArgument, "Leapfrog child keys disagree with variable order"));
        return false;
      }
      previous = position;
      iters_for_var[position].push_back(iter.get());
    }
    for (const auto var : iter->get_enumeration_vars()) {
      const auto found = std::find(var_order.begin(), var_order.end(), var);
      if (found == var_order.end() || found - var_order.begin() < enumeration_level ||
          !enumerated.insert(var).second) {
        set_status(STATUS(InvalidArgument, "Leapfrog enumeration variables must have one owner"));
        return false;
      }
    }
  }
  for (const auto& inputs : iters_for_var) {
    if (inputs.empty()) {
      set_status(STATUS(InvalidArgument, "Leapfrog intersection variable has no child"));
      return false;
    }
  }
  if (enumerated.size() + enumeration_level != var_order.size()) {
    set_status(STATUS(InvalidArgument, "Leapfrog output variable has no child"));
    return false;
  }
  return true;
}

void LeapfrogJoin::_begin(Binding& input) {
  parent_binding = &input;
  level = -1;
  enumeration_ready_ = false;
  iters_for_var.clear();
  if (status().ok() && ValidateLayout()) Open();
}

void LeapfrogJoin::Open() {
  if (!CheckInterruption()) return;
  bool all_nonempty = true;
  // Open every child: an empty relation must not mask a later child's load error.
  for (auto& iter : leapfrog_iters) {
    const bool nonempty = iter->open_terms(*parent_binding);
    if (!Observe(*iter)) return;
    all_nonempty &= nonempty;
  }
  if (all_nonempty) down();
}

void LeapfrogJoin::_reset() {
  level = -1;
  enumeration_ready_ = false;
  if (!status().ok()) return;
  if (parent_binding == nullptr) {
    set_status(STATUS(InvalidArgument, "Leapfrog reset before begin"));
    return;
  }
  Open();
}

bool LeapfrogJoin::_next() {
  if (!status().ok() || !CheckInterruption()) return false;
  while (level >= 0) {
    if (level < enumeration_level) {
      if (find_intersection_for_current_level()) {
        down();
      } else {
        if (!status().ok()) return false;
        up();
        if (!AdvanceCurrentLevel()) return false;
      }
      continue;
    }
    if (enumeration_ready_) {
      for (size_t i = 0; i < leapfrog_iters.size(); ++i) {
        const bool found = leapfrog_iters[i]->next_enumeration(*parent_binding);
        if (!Observe(*leapfrog_iters[i])) return false;
        if (!found) continue;
        for (size_t j = 0; j < i; ++j) {
          leapfrog_iters[j]->reset_enumeration();
          if (!Observe(*leapfrog_iters[j])) return false;
          const bool first = leapfrog_iters[j]->next_enumeration(*parent_binding);
          if (!Observe(*leapfrog_iters[j])) return false;
          if (!first) {
            set_status(STATUS(IllegalState, "Leapfrog enumeration changed while resetting"));
            level = -1;
            return false;
          }
        }
        return true;
      }
    }
    // Enumeration does not descend the child tries, so only the join level changes here.
    --level;
    enumeration_ready_ = false;
    if (!AdvanceCurrentLevel()) return false;
  }
  return false;
}

bool LeapfrogJoin::AdvanceCurrentLevel() {
  while (level >= 0 && status().ok()) {
    auto& iter = *iters_for_var[level].back();
    const bool found = iter.next();
    if (!Observe(iter)) return false;
    if (found) return true;
    up();
  }
  return false;
}

void LeapfrogJoin::up() {
  for (auto* iter : iters_for_var[level]) {
    iter->up();
    if (!Observe(*iter)) return;
  }
  --level;
}

void LeapfrogJoin::down() {
  ++level;
  if (level < enumeration_level) {
    for (auto* iter : iters_for_var[level]) {
      iter->down();
      if (!Observe(*iter)) return;
    }
    auto& inputs = iters_for_var[level];
    std::sort(inputs.begin(), inputs.end(), [](const auto* lhs, const auto* rhs) {
      return lhs->get_key() < rhs->get_key();
    });
  } else {
    enumeration_ready_ = true;
    for (size_t i = 0; i < leapfrog_iters.size(); ++i) {
      leapfrog_iters[i]->begin_enumeration();
      if (!Observe(*leapfrog_iters[i])) return;
      if (i != 0) {
        const bool first = leapfrog_iters[i]->next_enumeration(*parent_binding);
        if (!Observe(*leapfrog_iters[i])) return;
        enumeration_ready_ &= first;
      }
    }
  }
}

bool LeapfrogJoin::find_intersection_for_current_level() {
  size_t position = 0;
  auto& inputs = iters_for_var[level];
  auto min = inputs.front()->get_key();
  auto max = inputs.back()->get_key();
  while (min != max) {
    if (!CheckInterruption()) return false;
    ++seeks;
    const bool found = inputs[position]->seek(max);
    if (!Observe(*inputs[position]) || !found) return false;
    max = inputs[position]->get_key();
    position = (position + 1) % inputs.size();
    min = inputs[position]->get_key();
  }
  parent_binding->add(var_order[level], ObjectId(min));
  return true;
}

void LeapfrogJoin::assign_nulls() {
  if (parent_binding == nullptr || !status().ok()) return;
  for (const auto var : var_order) parent_binding->add(var, ObjectId::get_null());
}

void LeapfrogJoin::print(std::ostream& os, int indent, bool stats) const {
  if (stats) {
    os << std::string(indent, ' ') << "[begin: " << stat_begin << " next: " << stat_next
       << " reset: " << stat_reset << " results: " << results << " seeks: " << seeks << "]\n";
  }
  os << std::string(indent, ' ') << "LeapfrogJoin(";
  for (int i = 0; i < enumeration_level && static_cast<size_t>(i) < var_order.size(); ++i) {
    if (i != 0) os << ' ';
    os << var_order[i];
  }
  os << ")\n";
  for (const auto& iter : leapfrog_iters) {
    if (!iter) continue;
    os << std::string(indent + 2, ' ') << iter->get_iter_name() << "(ranges:";
    for (const auto& range : iter->initial_ranges) {
      os << ' ';
      if (range) range->print(os);
    }
    for (const auto var : iter->get_intersection_vars()) os << ' ' << var;
    for (const auto var : iter->get_enumeration_vars()) os << ' ' << var;
    os << ")\n";
  }
}
