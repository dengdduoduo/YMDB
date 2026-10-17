#include "yb/mdb/import/import_pipeline.h"

#include <fstream>
#include <sstream>

#include "yb/mdb/import/csv_parser.h"
#include "yb/mdb/import/nt_parser.h"
#include "yb/mdb/import/rdf_xml_parser.h"
#include "yb/mdb/model/rdf_yb_model.h"
#include "yb/util/logging.h"

namespace yb::mdb {

Status ImportPipeline::BatchWrite(const std::vector<Triple>& triples) {
  auto& model = RdfYBModel::Instance();

  for (const auto& triple : triples) {
    uint64_t subj_id = VERIFY_RESULT(model.ResolveTerm(id_map_, triple.sub));
    uint64_t pred_id = VERIFY_RESULT(model.ResolveTerm(id_map_, triple.pred));

    std::string obj_key = triple.obj;
    if (triple.obj_type == TripleTermType::LITERAL && !triple.object_type.empty()) {
      obj_key = triple.obj + "|" + triple.object_type;
    }
    uint64_t obj_id = VERIFY_RESULT(model.ResolveTerm(id_map_, obj_key));

    RETURN_NOT_OK(model.WriteTripleToDocDB(subj_id, pred_id, obj_id));
  }

  // Complete buffered writes before reporting this batch as written.
  RETURN_NOT_OK(model.Flush());
  stats_.triples_written += static_cast<int64_t>(triples.size());
  return Status::OK();
}

Result<ImportStats> ImportPipeline::ImportNtFile(const std::string& file_path) {
  std::ifstream file(file_path);
  if (!file.is_open()) {
    return STATUS(IOError, "Cannot open file: " + file_path);
  }

  std::string line;
  int line_num = 0;
  std::vector<Triple> batch;

  while (std::getline(file, line)) {
    line_num++;
    stats_.lines_read++;

    if (line.empty() || line[0] == '#') {
      continue;
    }

    auto result = NtParser::ParseLine(line);
    if (!result.ok()) {
      stats_.errors++;
      LOG(WARNING) << "Line " << line_num << ": " << result.status();
      continue;
    }

    batch.push_back(std::move(*result));
    stats_.triples_parsed++;

    if (batch.size() >= 500) {
      RETURN_NOT_OK(BatchWrite(batch));
      batch.clear();
    }
  }

  if (!batch.empty()) {
    RETURN_NOT_OK(BatchWrite(batch));
  }

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

  return stats_;
}

Result<ImportStats> ImportPipeline::ImportFile(const std::string& file_path) {
  stats_ = ImportStats{};

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
  stats_.triples_parsed = static_cast<int64_t>(triples.size());

  RETURN_NOT_OK(BatchWrite(triples));

  return stats_;
}

}  // namespace yb::mdb
