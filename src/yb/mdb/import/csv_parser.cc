#include "yb/mdb/import/csv_parser.h"

#include <fstream>
#include <sstream>

#include "yb/util/logging.h"
#include "yb/util/status.h"

namespace yb::mdb {

std::vector<std::string> CsvParser::SplitLine(const std::string& line,
                                              char delimiter, char quote) {
  std::vector<std::string> fields;
  std::string current;
  bool in_quotes = false;

  for (size_t i = 0; i < line.size(); ++i) {
    char ch = line[i];

    if (ch == quote) {
      if (in_quotes && i + 1 < line.size() && line[i + 1] == quote) {
        current += quote;
        i++;
      } else {
        if (!in_quotes && current.empty()) {
          in_quotes = true;
          continue;
        }
        if (in_quotes) {
          in_quotes = false;
          continue;
        }
      }
    } else if (ch == delimiter && !in_quotes) {
      fields.push_back(current);
      current.clear();
    } else {
      current += ch;
    }
  }

  fields.push_back(current);
  return fields;
}

Result<Triple> CsvParser::FieldsToTriple(const std::vector<std::string>& fields) {
  if (fields.size() < 3) {
    return STATUS(InvalidArgument, "CSV row has fewer than 3 fields");
  }

  Triple triple;

  const std::string& subj = fields[0];
  triple.sub = subj;
  triple.sub_type = TripleTermType::URI;

  const std::string& pred = fields[1];
  triple.pred = pred;
  triple.pred_type = TripleTermType::URI;

  const std::string& obj = fields[2];
  if (obj.size() >= 7 && obj.compare(0, 7, "http://") == 0) {
    triple.obj = obj;
    triple.obj_type = TripleTermType::URI;
  } else if (obj.size() >= 8 && obj.compare(0, 8, "https://") == 0) {
    triple.obj = obj;
    triple.obj_type = TripleTermType::URI;
  } else {
    triple.obj = obj;
    triple.obj_type = TripleTermType::LITERAL;
  }

  return triple;
}

Result<Triple> CsvParser::ParseLine(const std::string& line,
                                    const CsvParseConfig& config) {
  auto fields = SplitLine(line, config.delimiter, config.quote);
  return FieldsToTriple(fields);
}

Result<std::vector<Triple>> CsvParser::ParseFile(const std::string& file_path,
                                                 const CsvParseConfig& config) {
  std::ifstream file(file_path);
  if (!file.is_open()) {
    return STATUS(IOError, "Cannot open file: " + file_path);
  }

  std::vector<Triple> triples;
  std::string line;
  int line_num = 0;

  while (std::getline(file, line)) {
    line_num++;

    if (line.empty()) continue;

    if (config.has_header && line_num == 1) {
      continue;
    }

    auto result = ParseLine(line, config);
    if (!result.ok()) {
      LOG(WARNING) << "CSV line " << line_num << ": " << result.status();
      continue;
    }

    triples.push_back(std::move(*result));
  }

  return triples;
}

}  // namespace yb::mdb
