// Copyright (c) YugabyteDB, Inc.

#include "yb/mdb/storage/docdb_read_context.h"
#include "yb/mdb/storage/managed_transaction.h"

#include "yb/client/client.h"
#include "yb/client/session.h"
#include "yb/client/transaction.h"
#include "yb/client/transaction_manager.h"
#include "yb/common/transaction.h"
#include <chrono>

namespace yb::mdb {
namespace {
thread_local DocDBReadContext* active_read_context = nullptr;
}  // namespace

Result<std::unique_ptr<DocDBReadContext>> DocDBReadContext::Create(
    client::YBClient* client, const std::shared_ptr<client::TransactionManager>& manager) {
  if (!client || !manager) return STATUS(IllegalState, "MDB read context requires a connected model");
  auto context = std::unique_ptr<DocDBReadContext>(new DocDBReadContext());
  auto transaction = MakeManagedTransaction(manager);
  // Fix the timestamp before the first scan so later indexes and pages cannot
  // move the query's view. This is a historical snapshot, not a latest-read promise.
  RETURN_NOT_OK(transaction->Init(IsolationLevel::SNAPSHOT_ISOLATION,
                                  ReadHybridTime::SingleTime(manager->Now())));
  context->transaction_ = std::move(transaction);
  context->session = client->NewSession(MonoDelta::FromSeconds(30));
  context->session->SetTransaction(context->transaction_);
  return context;
}

DocDBReadContext::~DocDBReadContext() {
  if (session) session->SetTransaction(nullptr);
  if (transaction_) transaction_->Abort();
}

Result<ChildTransactionDataPB> DocDBReadContext::ExportChild() {
  if (cancelled.load()) return STATUS(Aborted, "MDB query cancelled before worker start");
  if (CoarseMonoClock::Now() >= deadline) return STATUS(TimedOut, "MDB worker snapshot expired");
  auto future = transaction_->PrepareChildFuture(client::ForceConsistentRead::kTrue, deadline);
  if (future.wait_until(ToSteady(deadline)) != std::future_status::ready)
    return STATUS(TimedOut, "MDB child snapshot preparation timed out");
  return future.get();
}

Status DocDBReadContext::ApplyChild(const ChildTransactionResultPB& result) {
  RETURN_NOT_OK(transaction_->ApplyChildResult(result));
  if (transaction_->IsRestartRequired())
    return STATUS(TryAgain, "MDB worker snapshot requires whole-query restart");
  return Status::OK();
}

DocDBReadContextScope::DocDBReadContextScope(DocDBReadContext& context)
    : previous_(active_read_context) {
  active_read_context = &context;
}

DocDBReadContextScope::~DocDBReadContextScope() { active_read_context = previous_; }

DocDBReadContext* DocDBReadContextScope::Current() { return active_read_context; }

}  // namespace yb::mdb
