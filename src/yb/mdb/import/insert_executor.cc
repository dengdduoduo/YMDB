// Copyright (c) YugabyteDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//

#include "yb/mdb/import/insert_executor.h"


#include "yb/mdb/import/nt_parser.h"
#include "yb/mdb/model/rdf_yb_model.h"
#include "yb/util/logging.h"
#include "yb/util/scope_exit.h"
#include "yb/util/uuid.h"

namespace yb::mdb {

Result<std::string> InsertDataExecutor::ExtractTripleBlock(const std::string& query) {
  size_t position = 0;
  const auto skip = [&] {
    while (position < query.size()) {
      if (query[position] == '#') {
        while (position < query.size() && query[position] != '\n') ++position;
      } else if (std::string_view(" \t\r\n").find(query[position]) != std::string_view::npos) {
        ++position;
      } else {
        break;
      }
    }
  };
  const auto keyword = [&](std::string_view expected) {
    skip();
    for (char c : expected) {
      if (position >= query.size()) return false;
      auto actual = query[position++];
      if (actual >= 'a' && actual <= 'z') actual -= 'a' - 'A';
      if (actual != c) return false;
    }
    return true;
  };
  if (!keyword("INSERT")) return STATUS(InvalidArgument, "Invalid INSERT DATA envelope");
  if (position == query.size() || std::string_view(" \t\r\n#").find(query[position]) ==
      std::string_view::npos) return STATUS(InvalidArgument, "Invalid INSERT DATA envelope");
  if (!keyword("DATA")) return STATUS(InvalidArgument, "Invalid INSERT DATA envelope");
  skip();
  if (position >= query.size() || query[position] != '{') {
    return STATUS(InvalidArgument, "Invalid INSERT DATA envelope");
  }
  const auto start = ++position;
  bool quoted = false;
  bool iri = false;
  bool escaped = false;
  bool comment = false;
  size_t end = std::string::npos;
  for (; position < query.size(); ++position) {
    const auto c = query[position];
    if (comment) { if (c == '\n') comment = false; continue; }
    if (escaped) { escaped = false; continue; }
    if ((quoted || iri) && c == '\\') { escaped = true; continue; }
    if (quoted) { if (c == '"') quoted = false; continue; }
    if (iri) { if (c == '>') iri = false; continue; }
    if (c == '#') {
      comment = true;
    } else if (c == '"') {
      quoted = true;
    } else if (c == '<') {
      iri = true;
    } else if (c == '}') {
      end = position;
      break;
    } else if (c == '{') {
      return STATUS(NotSupported, "Nested INSERT DATA blocks are not supported");
    }
  }
  if (end == std::string::npos) return STATUS(InvalidArgument, "Unclosed INSERT DATA block");
  position = end + 1;
  skip();
  if (position != query.size()) return STATUS(InvalidArgument, "Invalid INSERT DATA envelope");
  return query.substr(start, end - start);

}

Status InsertDataExecutor::Execute(const std::string& insert_data_query,
                                   IdMap& id_map,
                                   ImportStats& stats,
                                   const std::function<Status(uint64_t, const RdfTerm&)>& persist) {
  const auto triple_block = VERIFY_RESULT(ExtractTripleBlock(insert_data_query));

  auto parsed = NtParser::ParseSequence(triple_block);
  if (!parsed.ok()) {
    ++stats.errors;
    return parsed.status();
  }
  auto triples = std::move(*parsed);
  const auto scope = Uuid::Generate().ToHexString();
  for (auto& triple : triples) ScopeBlankNodes(&triple, scope);
  stats.triples_parsed = static_cast<int64_t>(triples.size());
  if (triples.empty()) return Status::OK();

  auto& model = RdfYBModel::Instance();
  if (!model.IsConnected()) return STATUS(IllegalState, "RDF model is not connected");
  if (model.QueryReadTime()) return STATUS(IllegalState, "Read snapshot is active");
  if (model.HasTransaction()) {
    return STATUS(IllegalState, "INSERT cannot replace a transaction owned by its caller");
  }
  if (!model.Dictionary() && !persist) {
    return STATUS(NotSupported,
        "Legacy INSERT requires a persistent term callback; "
        "standalone INSERT requires dictionary v2 in a fresh namespace");
  }
  bool use_txn = model.UseTransactions();
  auto transaction_cleanup = ScopeExit([&] {
    if (use_txn && model.HasTransaction()) (void)model.AbortTransaction();
  });
  if (use_txn) {
    RETURN_NOT_OK(model.BeginTransaction());
  }

  for (const auto& triple : triples) {
    uint64_t subj_id = VERIFY_RESULT(model.ResolveTerm(
        id_map, {triple.sub_type, triple.sub, "", ""}));
    uint64_t pred_id = VERIFY_RESULT(model.ResolveTerm(
        id_map, {triple.pred_type, triple.pred, "", ""}));
    uint64_t obj_id = VERIFY_RESULT(model.ResolveTerm(id_map, ObjectTerm(triple)));

    if (persist) {
      RETURN_NOT_OK(persist(subj_id, {triple.sub_type, triple.sub, "", ""}));
      RETURN_NOT_OK(persist(pred_id, {triple.pred_type, triple.pred, "", ""}));
      RETURN_NOT_OK(persist(obj_id, ObjectTerm(triple)));
    }

    auto status = model.WriteTripleToDocDB(subj_id, pred_id, obj_id);
    if (!status.ok()) {
      if (use_txn) {
        (void)model.AbortTransaction();
      }
      stats.errors++;
      LOG(WARNING) << "WriteTripleToDocDB failed: " << status;
      return status;
    }
    stats.triples_written++;

    if (use_txn && stats.triples_written % 50 == 0) {
      RETURN_NOT_OK(model.CommitTransaction());
      RETURN_NOT_OK(model.BeginTransaction());
    } else if (!use_txn && stats.triples_written % 50 == 0) {
      RETURN_NOT_OK(model.Flush());
    }
  }

  if (use_txn) {
    RETURN_NOT_OK(model.CommitTransaction());
  } else {
    RETURN_NOT_OK(model.Flush());
  }
  return Status::OK();
}

}  // namespace yb::mdb
