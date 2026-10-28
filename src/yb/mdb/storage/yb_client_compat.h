#pragma once

#include "yb/client/session.h"

namespace yb::mdb {

// Newer clients allocate operations in the session arena; older clients expose
// parameterless factories that manage operation storage themselves. Keep the
// expressions dependent so unavailable APIs are not instantiated.
template <class Table, class Session>
auto NewMdbInsertOp(const Table& table, const Session& session) {
  if constexpr (requires { table.NewInsertOp(session->arena()); }) {
    return table.NewInsertOp(session->arena());
  } else {
    return table.NewInsertOp();
  }
}

template <class Table, class Session>
auto NewMdbReadOp(const Table& table, const Session& session) {
  if constexpr (requires { table.NewReadOp(session->arena()); }) {
    return table.NewReadOp(session->arena());
  } else {
    return table.NewReadOp();
  }
}

}  // namespace yb::mdb
