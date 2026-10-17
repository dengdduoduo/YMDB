#include "yb/mdb/import/insert_executor.h"

#include <sstream>

#include "yb/mdb/import/nt_parser.h"
#include "yb/mdb/model/rdf_yb_model.h"
#include "yb/util/logging.h"
#include "yb/util/scope_exit.h"

namespace yb::mdb {

std::string InsertDataExecutor::ExtractTripleBlock(const std::string& query) {
  size_t open_brace = query.find('{');
  size_t close_brace = query.rfind('}');
  if (open_brace == std::string::npos || close_brace == std::string::npos ||
      close_brace <= open_brace) {
    return "";
  }
  return query.substr(open_brace + 1, close_brace - open_brace - 1);
}

Status InsertDataExecutor::Execute(const std::string& insert_data_query,
                                   IdMap& id_map,
                                   ImportStats& stats) {
  std::string triple_block = ExtractTripleBlock(insert_data_query);
  if (triple_block.empty()) {
    return STATUS(InvalidArgument, "INSERT DATA has no triple block");
  }

  std::vector<Triple> triples;
  std::istringstream stream(triple_block);
  std::string line;

  while (std::getline(stream, line, '.')) {
    size_t start = line.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) continue;
    line = line.substr(start);
    line += " .";

    auto result = NtParser::ParseLine(line);
    if (!result.ok()) {
      stats.errors++;
      LOG(WARNING) << "INSERT DATA parse error: " << result.status();
      continue;
    }
    triples.push_back(std::move(*result));
    stats.triples_parsed++;
  }

  if (triples.empty()) {
    return STATUS(InvalidArgument, "No valid triples in INSERT DATA");
  }

  auto& model = RdfYBModel::Instance();
  bool use_txn = model.UseTransactions();
  if (use_txn) {
    RETURN_NOT_OK(model.BeginTransaction());
  }
  auto abort_on_exit = ScopeExit([&model, use_txn] {
    if (use_txn) (void)model.AbortTransaction();
  });

  for (const auto& triple : triples) {
    uint64_t subj_id = VERIFY_RESULT(model.ResolveTerm(id_map, triple.sub));
    uint64_t pred_id = VERIFY_RESULT(model.ResolveTerm(id_map, triple.pred));
    uint64_t obj_id = VERIFY_RESULT(model.ResolveTerm(id_map, triple.obj));

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
    } else if (!use_txn && stats.triples_written % 5000 == 0) {
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
