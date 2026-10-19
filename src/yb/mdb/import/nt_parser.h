#pragma once

#include <string>
#include <vector>

#include "yb/mdb/import/import.h"
#include "yb/util/result.h"

namespace yb::mdb {

class NtParser {
 public:
  static Status ParseFile(const std::string& file_path,
                          std::vector<Triple>& triples,
                          ImportStats& stats);

  static Result<Triple> ParseLine(const std::string& line);
  static Result<std::vector<std::string>> SplitStatements(const std::string& text);

 private:
  static std::string ParseUri(std::string_view token);
  static std::string ParseBlankNode(std::string_view token);
  static Result<std::pair<std::string, std::string>> ParseLiteral(std::string_view token);
  static std::vector<std::string> TokenizeLine(const std::string& line);
};

}  // namespace yb::mdb
