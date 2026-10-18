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

#include "yb/mdb/import/import_pipeline.h"

#include <fstream>
#include <sstream>

#include "yb/mdb/import/csv_parser.h"
#include "yb/mdb/import/nt_parser.h"
#include "yb/mdb/import/rdf_xml_parser.h"
#include "yb/mdb/model/rdf_yb_model.h"
#include "yb/util/logging.h"
#include "yb/util/uuid.h"

namespace yb::mdb {

Status ImportPipeline::BatchWrite(const std::vector<Triple>& triples) {
  auto& model = RdfYBModel::Instance();
  if (!model.IsConnected()) return STATUS(IllegalState, "RDF model is not connected");
  if (model.QueryReadTime()) return STATUS(IllegalState, "Read snapshot is active");
  if (!triples.empty() && !model.Dictionary()) {
    return STATUS(NotSupported,
        "Standalone imports require dictionary v2 in a fresh namespace; "
        "use the legacy CLI importer for an existing legacy namespace");
  }

  for (auto triple : triples) {
    ScopeBlankNodes(&triple, document_scope_);
    uint64_t subj_id = VERIFY_RESULT(model.ResolveTerm(
        id_map_, {triple.sub_type, triple.sub, "", ""}));
    uint64_t pred_id = VERIFY_RESULT(model.ResolveTerm(
        id_map_, {triple.pred_type, triple.pred, "", ""}));

    uint64_t obj_id = VERIFY_RESULT(model.ResolveTerm(id_map_, ObjectTerm(triple)));

    RETURN_NOT_OK(model.WriteTripleToDocDB(subj_id, pred_id, obj_id));
    stats_.triples_written++;
    // Dictionary RPCs between rows must not age a large pending write batch.
    if (stats_.triples_written % 50 == 0) RETURN_NOT_OK(model.Flush());
  }

  return Status::OK();
}

Result<ImportStats> ImportPipeline::ImportNtFile(const std::string& file_path) {
  std::vector<Triple> triples;
  RETURN_NOT_OK(NtParser::ParseFile(file_path, triples, stats_));
  RETURN_NOT_OK(BatchWrite(triples));
  RETURN_NOT_OK(RdfYBModel::Instance().Flush());
  return stats_;
}

Result<ImportStats> ImportPipeline::ImportCsvFile(const std::string& file_path) {
  CsvParseConfig config;
  auto triples_result = CsvParser::ParseFile(file_path, config);
  if (!triples_result.ok()) {
    return triples_result.status();
  }

  std::vector<Triple> triples = std::move(*triples_result);
  stats_.lines_read = static_cast<int64_t>(triples.size());
  stats_.triples_parsed = static_cast<int64_t>(triples.size());

  RETURN_NOT_OK(BatchWrite(triples));

  RETURN_NOT_OK(RdfYBModel::Instance().Flush());
  return stats_;
}

Result<ImportStats> ImportPipeline::ImportRdfXmlFile(const std::string& file_path) {
  auto triples_result = RdfXmlParser::ParseFile(file_path);
  if (!triples_result.ok()) {
    return triples_result.status();
  }

  std::vector<Triple> triples = std::move(*triples_result);
  stats_.triples_parsed = static_cast<int64_t>(triples.size());

  RETURN_NOT_OK(BatchWrite(triples));

  RETURN_NOT_OK(RdfYBModel::Instance().Flush());
  return stats_;
}

Result<ImportStats> ImportPipeline::ImportFile(const std::string& file_path) {
  stats_ = ImportStats{};
  document_scope_ = Uuid::Generate().ToHexString();

  size_t dot_pos = file_path.rfind('.');
  if (dot_pos == std::string::npos) {
    return STATUS(NotSupported, "Cannot detect file format: " + file_path);
  }

  std::string ext = file_path.substr(dot_pos);

  if (ext == ".nt") {
    return ImportNtFile(file_path);
  } else if (ext == ".csv") {
    return ImportCsvFile(file_path);
  } else if (ext == ".rdf" || ext == ".xml") {
    return ImportRdfXmlFile(file_path);
  }

  return STATUS(NotSupported, "Unsupported file format: " + ext);
}

Result<ImportStats> ImportPipeline::ImportTriples(const std::vector<Triple>& triples) {
  stats_ = ImportStats{};
  document_scope_ = Uuid::Generate().ToHexString();
  stats_.triples_parsed = static_cast<int64_t>(triples.size());

  RETURN_NOT_OK(BatchWrite(triples));

  RETURN_NOT_OK(RdfYBModel::Instance().Flush());
  return stats_;
}

}  // namespace yb::mdb
