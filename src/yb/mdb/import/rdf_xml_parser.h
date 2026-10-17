#pragma once

#include <map>
#include <string>
#include <vector>

#include "yb/mdb/import/import.h"
#include "yb/util/result.h"

namespace yb::mdb {

class RdfXmlParser {
 public:
  static Result<std::vector<Triple>> ParseFile(const std::string& file_path);

  static Result<std::vector<Triple>> ParseString(const std::string& xml_content);

 private:
  using NamespaceMap = std::map<std::string, std::string>;

  static NamespaceMap ExtractNamespaces(const std::string& xml_content);

  static std::string ResolvePrefixedName(const std::string& prefixed,
                                         const NamespaceMap& ns_map);

  static std::vector<Triple> ParseDescription(
      std::string_view element,
      const NamespaceMap& ns_map);

  static std::string ExtractAttrValue(std::string_view element,
                                      const std::string& attr_name);
};

}  // namespace yb::mdb
