// Portions Copyright (c) YugabyteDB, Inc.

#include "yb/mdb/query/executor/slice.h"

void Slice::_begin(Binding& _parent_binding) {
  parent_binding = &_parent_binding;
  child_iter->begin(_parent_binding);
  set_status_from(*child_iter);
  count = 0;
  position = 0;
}

void Slice::_reset() {
  child_iter->reset();
  set_status_from(*child_iter);
  position = 0;
  count = 0;
}

bool Slice::_next() {
  if (!status().ok()) return false;

  while (position < offset) {
    const bool has_next = child_iter->next();
    set_status_from(*child_iter);
    if (!has_next || !status().ok()) return false;
    position++;
  }

  if (count >= limit) return false;
  const bool has_next = child_iter->next();
  set_status_from(*child_iter);
  if (!has_next || !status().ok()) return false;
  count++;
  return true;
}

void Slice::assign_nulls() { child_iter->assign_nulls(); }

void Slice::print(std::ostream& os, int indent, bool stats) const {
  if (stats) {
    print_generic_stats(os, indent);
  }
  os << std::string(indent, ' ') << "Slice(";
  auto printed = false;
  if (offset != 0) {
    printed = true;
    os << "offset: " << offset;
  }

  if (limit != UINT64_MAX) {
    if (printed) os << ", ";
    printed = true;
    os << "limit: " << limit;
  }

  os << ")\n";
  child_iter->print(os, indent + 2, stats);
}
