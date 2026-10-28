// Copyright (c) YugabyteDB, Inc.
#pragma once

#include <memory>
#include <utility>

#include "yb/client/transaction.h"
#include "yb/client/transaction_manager.h"

namespace yb::mdb {
// A synchronous flush can return before the RPC callback releases its Batcher
// and transaction. Keep the manager alive through that last transaction
// destructor, which still aborts handles in the manager's RPC registry.
template <class... Args>
client::YBTransactionPtr MakeManagedTransaction(
    const std::shared_ptr<client::TransactionManager>& manager, Args&&... args) {
  return client::YBTransactionPtr(
      new client::YBTransaction(manager.get(), std::forward<Args>(args)...),
      [keep_alive = manager](client::YBTransaction* transaction) mutable {
        delete transaction;
        // Weak transaction references must not retain a stopped manager.
        keep_alive.reset();
      });
}
}  // namespace yb::mdb
