// Copyright (c) YugabyteDB, Inc.
#include "yb/mdb/query/distributed/fragment_worker.h"

#include <algorithm>
#include <array>
#include <unistd.h>

#include "yb/client/client.h"
#include "yb/client/session.h"
#include "yb/client/table.h"
#include "yb/client/table_handle.h"
#include "yb/client/transaction.h"
#include "yb/client/transaction_manager.h"
#include "yb/client/yb_table_name.h"
#include "yb/common/transaction.h"
#include "yb/mdb/model/rdf_yb_model.h"
#include "yb/mdb/query/distributed/fragment_plan.h"
#include "yb/mdb/query/distributed/fragment_transport.h"
#include "yb/mdb/storage/docdb_key_encoder.h"
#include "yb/mdb/storage/docdb_scan_cursor.h"
#include "yb/mdb/storage/managed_transaction.h"
#include "yb/server/clock.h"
#include "yb/server/hybrid_clock.h"

namespace yb::mdb {
namespace {

class Worker {
 public:
  ~Worker() {
    levels_.clear();
    if (session_) session_->SetTransaction(nullptr);
    session_.reset();
    transaction_.reset();
    if (manager_) manager_->Shutdown();
    // Drain callbacks while the client and stopped manager still exist.
    if (client_) client_->Shutdown();
  }

  Status Open(const FragmentOpenPB& open) {
    RETURN_NOT_OK(ValidateFragmentOpen(open));
    open_ = open;
    deadline_ = CoarseMonoClock::Now() + MonoDelta::FromMilliseconds(open.timeout_ms());
    clock_ = make_scoped_refptr<server::HybridClock>();
    RETURN_NOT_OK(clock_->Init());
    client_ = VERIFY_RESULT(client::YBClientBuilder()
        .add_master_server_addr(open.master_addresses())
        .default_admin_operation_timeout(MonoDelta::FromMilliseconds(open.timeout_ms()))
        .default_rpc_timeout(MonoDelta::FromMilliseconds(open.timeout_ms()))
        .Build(static_cast<rpc::Messenger*>(nullptr), clock_));
    for (int perm = 0; perm < 6; ++perm) {
      RETURN_NOT_OK(tables_[perm].Open(client::YBTableName(
          YQL_DATABASE_CQL, open.keyspace(), RdfYBModel::kTableNames[perm]), client_.get()));
      if (tables_[perm].table()->id() != open.table_ids(perm))
        return STATUS(Corruption, "MDB table generation changed before worker open");
    }
    manager_ = std::make_shared<client::TransactionManager>(
        client_.get(), scoped_refptr<server::Clock>(client_->Clock()), client::LocalTabletFilter());
    auto data = VERIFY_RESULT(client::ChildTransactionData::FromPB(open.transaction()));
    transaction_ = MakeManagedTransaction(manager_, std::move(data));
    session_ = client_->NewSession(MonoDelta::FromMilliseconds(open.timeout_ms()));
    session_->SetTransaction(transaction_);
    levels_.resize(open.triples_size());
    bindings_.resize(open.triples_size() + 1, std::vector<uint64_t>(open.variable_count(), 0));
    return Status::OK();
  }

  Status Next(FragmentResponsePB* response) {
    uint32_t rows = 0;
    // A work quantum permits empty nonterminal responses without monopolizing Gather.
    for (size_t work = 0; work < 4096 && rows < open_.batch_rows(); ++work) {
      if (CoarseMonoClock::Now() >= deadline_)
        return STATUS(TimedOut, "MDB worker execution deadline expired");
      if (session_->IsRestartRequired())
        return STATUS(TryAgain, "MDB child read view requires restart");
      auto& level = levels_[depth_];
      if (!level.open) RETURN_NOT_OK(OpenLevel());
      if (level.position == level.batch.rows.size()) {
        if (level.batch.eof) {
          level.cursor.Close();
          level.open = false;
          // One completed page stays in its existing level buffer. Never
          // retain a tail page as if it were the complete probe result.
          if (!level.reusable) level.batch = {};
          level.position = 0;
          if (depth_ == 0) {
            auto result = VERIFY_RESULT(transaction_->FinishChild());
            if (transaction_->IsRestartRequired())
              return STATUS(TryAgain, "MDB child read view requires restart");
            *response->mutable_transaction_result() = std::move(result);
            response->set_eof(true);
            break;
          }
          --depth_;
          continue;
        }
        const auto before = level.cursor.rpc_calls();
        level.batch = VERIFY_RESULT(level.cursor.NextBatch());
        ++level.pages;
        level.reusable = depth_ != 0 && level.pages == 1 && level.batch.eof;
        read_operations_ += level.cursor.rpc_calls() - before;
        level.position = 0;
        if (level.batch.rows.empty()) continue;
      }
      const auto& stored = level.batch.rows[level.position++];
      std::array<uint64_t, 3> triple;
      DocKeyEncoder::ReversePermute(level.perm, stored[0], stored[1], stored[2],
                                   &triple[0], &triple[1], &triple[2]);
      auto& next = bindings_[depth_ + 1];
      next = bindings_[depth_];
      bool matches = true;
      for (int i = 0; i < 3; ++i) {
        if (!triple[i]) return STATUS(Corruption, "Zero is not a persistent RDF term ID");
        const auto& term = open_.triples(depth_).terms(i);
        if (term.has_constant()) matches &= term.constant() == triple[i];
        else {
          auto& value = next[term.variable()];
          if (value && value != triple[i]) matches = false;
          value = triple[i];
        }
      }
      if (!matches) continue;
      if (depth_ + 1 == static_cast<size_t>(open_.triples_size())) {
        for (auto variable : open_.projection()) response->add_values(next[variable]);
        ++rows;
      } else {
        ++depth_;
      }
    }
    response->set_rows(rows);
    response->set_read_operations(read_operations_);
    return Status::OK();
  }

  CoarseTimePoint deadline() const { return deadline_; }

 private:
  struct Level {
    DocDBScanCursor cursor;
    DocDBScanBatch batch;
    size_t position = 0;
    int perm = 0;
    bool open = false;
    // This state and one decoded page fit the existing per-level reservation.
    std::array<uint64_t, 3> probe_key{};
    size_t pages = 0;
    bool reusable = false;
  };

  Status OpenLevel() {
    auto& level = levels_[depth_];
    const auto& triple = open_.triples(depth_);
    std::array<uint64_t, 3> known{};
    for (int i = 0; i < 3; ++i) {
      const auto& term = triple.terms(i);
      known[i] = term.has_constant() ? term.constant() : bindings_[depth_][term.variable()];
    }
    // Only the anchor owns a partition. Every inner probe is free to cross tablets.
    const int perm = depth_ == 0 ? 0 : known[0] ? 0 : known[2] ? 2 : known[1] ? 1 : 0;
    // Only this fragment's immutable child read view owns the buffer. Rebind
    // every incoming row normally, even when its raw probe rows are reused.
    if (depth_ != 0 && level.reusable && level.perm == perm && level.probe_key == known) {
      level.position = 0;
      level.open = true;
      return Status::OK();
    }
    level.perm = perm;
    level.probe_key = known;
    level.pages = 0;
    level.reusable = false;
    std::array<uint64_t, 3> keys{};
    DocKeyEncoder::Permute(level.perm, known[0], known[1], known[2], &keys[0], &keys[1], &keys[2]);
    DocDBScanSpec spec;
    for (int i = 0; i < 3; ++i) if (keys[i]) spec.columns[i] = {keys[i], keys[i]};
    if (keys[0]) spec.hash_key = keys[0];
    if (depth_ == 0) {
      spec.partition_start = open_.partition_start();
      spec.partition_end = open_.partition_end();
    }
    spec.page_rows = open_.page_rows();
    spec.page_bytes = open_.page_rows() * sizeof(DocDBScanBatch::Row);
    spec.deadline = deadline_;
    spec.push_exact_filters = true;
    RETURN_NOT_OK(level.cursor.Open(&tables_[level.perm], session_, std::move(spec)));
    level.batch = {};
    level.position = 0;
    level.open = true;
    return Status::OK();
  }

  FragmentOpenPB open_;
  CoarseTimePoint deadline_;
  scoped_refptr<server::Clock> clock_;
  std::unique_ptr<client::YBClient> client_;
  std::shared_ptr<client::TransactionManager> manager_;
  client::YBTransactionPtr transaction_;
  client::YBSessionPtr session_;
  std::array<client::TableHandle, 6> tables_;
  std::vector<Level> levels_;
  std::vector<std::vector<uint64_t>> bindings_;
  size_t depth_ = 0;
  uint64_t read_operations_ = 0;
};

}  // namespace

Status RunFragmentWorker(int input_fd, int output_fd) {
  RETURN_NOT_OK(SetFragmentNonblocking(input_fd));
  RETURN_NOT_OK(SetFragmentNonblocking(output_fd));
  Worker worker;
  auto deadline = CoarseMonoClock::Now() + MonoDelta::FromSeconds(30);
  FragmentRequestPB first;
  uint64_t sequence = 0;
  for (;;) {
    FragmentRequestPB request;
    RETURN_NOT_OK(ReadFragmentFrame(input_fd, &request, deadline));
    FragmentResponsePB response;
    response.set_version(kFragmentVersion);
    response.set_query_id(request.query_id());
    response.set_fragment_id(request.fragment_id());
    response.set_attempt_id(request.attempt_id());
    response.set_stream_id(request.stream_id());
    response.set_batch_seq(request.batch_seq());
    response.set_worker_pid(getpid());
    auto handle = [&]() -> Status {
      if (request.version() != kFragmentVersion || request.query_id().empty() ||
          request.query_id().size() > 64 || request.attempt_id() != 1 ||
          request.stream_id() != request.fragment_id() || request.batch_seq() != sequence)
        return STATUS(InvalidArgument, "MDB worker envelope/version/sequence mismatch");
      if (sequence == 0) {
        if (!request.has_open()) return STATUS(InvalidArgument, "MDB worker requires OPEN");
        first = request;
        RETURN_NOT_OK(worker.Open(request.open()));
        deadline = worker.deadline();
        return Status::OK();
      }
      if (request.has_open() || request.query_id() != first.query_id() ||
          request.fragment_id() != first.fragment_id())
        return STATUS(InvalidArgument, "MDB worker stream identity changed");
      return worker.Next(&response);
    };
    auto status = handle();
    if (!status.ok()) {
      response.clear_values();
      response.set_rows(0);
      response.clear_transaction_result();
      response.set_error(status.ToString());
      response.set_eof(true);
    }
    RETURN_NOT_OK(WriteFragmentFrame(output_fd, response, deadline));
    if (response.eof()) return status;
    ++sequence;
  }
}

}  // namespace yb::mdb
