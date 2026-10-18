#pragma once

#include <string>
#include <vector>

#include "yb/mdb/import/import.h"
#include "yb/util/result.h"

namespace yb::mdb {

struct CsvParseConfig {
  char delimiter = ',';
  char quote = '"';
  bool has_header = true;
};

class CsvParser {
 public:
  static Result<std::vector<Triple>> ParseFile(const std::string& file_path,
                                               const CsvParseConfig& config = {});

  static Result<Triple> ParseLine(const std::string& line,
                                  const CsvParseConfig& config = {});

 private:
  static std::vector<std::string> SplitLine(const std::string& line,
                                            char delimiter, char quote);
  static Result<Triple> FieldsToTriple(const std::vector<std::string>& fields);
};

}  // namespace yb::mdb
