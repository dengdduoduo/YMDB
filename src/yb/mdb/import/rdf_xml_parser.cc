#include "yb/mdb/import/rdf_xml_parser.h"

#include <fstream>
#include <sstream>

#include "yb/util/logging.h"
#include "yb/util/status.h"

namespace yb::mdb {

RdfXmlParser::NamespaceMap RdfXmlParser::ExtractNamespaces(
    const std::string& xml_content) {
  NamespaceMap ns_map;

  size_t pos = 0;
  while (true) {
    pos = xml_content.find("xmlns:", pos);
    if (pos == std::string::npos) break;

    size_t eq = xml_content.find('=', pos);
    if (eq == std::string::npos) break;

    size_t prefix_end = eq - 1;
    while (prefix_end > pos && xml_content[prefix_end] == ' ') prefix_end--;

    std::string prefix = xml_content.substr(pos + 6, prefix_end - pos - 6 + 1);

    size_t quote_start = xml_content.find('"', eq + 1);
    if (quote_start == std::string::npos) {
      quote_start = xml_content.find('\'', eq + 1);
    }
    if (quote_start == std::string::npos) break;

    char quote_char = xml_content[quote_start];
    size_t quote_end = xml_content.find(quote_char, quote_start + 1);
    if (quote_end == std::string::npos) break;

    std::string uri = xml_content.substr(quote_start + 1, quote_end - quote_start - 1);

    if (!uri.empty() && uri.back() != '/') {
      uri += '/';
    }

    ns_map[prefix] = uri;
    pos = quote_end + 1;
  }

  return ns_map;
}

std::string RdfXmlParser::ResolvePrefixedName(const std::string& prefixed,
                                              const NamespaceMap& ns_map) {
  size_t colon = prefixed.find(':');
  if (colon == std::string::npos) {
    return prefixed;
  }

  std::string prefix = prefixed.substr(0, colon);
  std::string local = prefixed.substr(colon + 1);

  auto it = ns_map.find(prefix);
  if (it != ns_map.end()) {
    return it->second + local;
  }

  return prefixed;
}

std::string RdfXmlParser::ExtractAttrValue(std::string_view element,
                                           const std::string& attr_name) {
  std::string search = attr_name + "=\"";
  size_t pos = element.find(search);
  if (pos == std::string::npos) {
    search = attr_name + "='";
    pos = element.find(search);
    if (pos == std::string::npos) return "";
  }

  pos += search.size();
  char quote = element[pos - 1];
  size_t end = element.find(quote, pos);
  if (end == std::string::npos) return "";

  return std::string(element.substr(pos, end - pos));
}

std::vector<Triple> RdfXmlParser::ParseDescription(
    std::string_view element,
    const NamespaceMap& ns_map) {
  std::vector<Triple> triples;

  std::string about = ExtractAttrValue(element, "rdf:about");
  if (about.empty()) {
    about = ExtractAttrValue(element, "rdf:nodeID");
  }

  if (about.empty()) return triples;

  size_t end_tag = element.find('>');
  if (end_tag == std::string::npos) return triples;

  std::string_view body = element.substr(end_tag + 1);
  size_t close_desc = body.find("</rdf:Description>");
  if (close_desc == std::string::npos) {
    close_desc = body.find("/>");
    if (close_desc == std::string::npos) return triples;
  }
  body = body.substr(0, close_desc);

  size_t pos = 0;
  while (pos < body.size()) {
    size_t tag_start = body.find('<', pos);
    if (tag_start == std::string::npos) break;

    if (tag_start + 1 < body.size() && body[tag_start + 1] == '/') {
      pos = tag_start + 1;
      continue;
    }

    size_t tag_end = body.find('>', tag_start + 1);
    if (tag_end == std::string::npos) break;

    std::string_view tag = body.substr(tag_start, tag_end - tag_start + 1);

    size_t name_end_space = tag.find(' ', 1);
    size_t name_end_close = tag.find('>', 1);
    size_t name_end = std::min(name_end_space, name_end_close);

    std::string prefixed_name(tag.substr(1, name_end - 1));
    if (!prefixed_name.empty() && prefixed_name.back() == '/') {
      prefixed_name.pop_back();
    }
    std::string uri = ResolvePrefixedName(prefixed_name, ns_map);

    bool self_closing = (tag.size() >= 2 && tag[tag.size() - 2] == '/');
    if (self_closing) {
      std::string resource = ExtractAttrValue(tag, "rdf:resource");

      if (!resource.empty()) {
        Triple triple;
        triple.sub = about;
        triple.sub_type = TripleTermType::URI;
        triple.pred = uri;
        triple.pred_type = TripleTermType::URI;
        triple.obj = resource;
        triple.obj_type = TripleTermType::URI;
        triples.push_back(triple);
      }

      pos = tag_end + 1;
      continue;
    }

    std::string closing = "</" + prefixed_name + ">";
    size_t close_pos = body.find(closing, tag_end);
    if (close_pos == std::string::npos) {
      pos = tag_end + 1;
      continue;
    }

    std::string_view content = body.substr(tag_end + 1, close_pos - tag_end - 1);

    std::string datatype = ExtractAttrValue(tag, "rdf:datatype");

    while (!content.empty() && (content.front() == ' ' || content.front() == '\t' ||
                                content.front() == '\n' || content.front() == '\r')) {
      content.remove_prefix(1);
    }
    while (!content.empty() && (content.back() == ' ' || content.back() == '\t' ||
                                content.back() == '\n' || content.back() == '\r')) {
      content.remove_suffix(1);
    }

    Triple triple;
    triple.sub = about;
    triple.sub_type = TripleTermType::URI;
    triple.pred = uri;
    triple.pred_type = TripleTermType::URI;
    triple.obj = std::string(content);
    triple.obj_type = TripleTermType::LITERAL;
    if (!datatype.empty()) {
      triple.object_type = datatype;
    }
    triples.push_back(triple);

    pos = close_pos + closing.size();
  }

  return triples;
}

Result<std::vector<Triple>> RdfXmlParser::ParseString(
    const std::string& xml_content) {
  NamespaceMap ns_map = ExtractNamespaces(xml_content);

  std::vector<Triple> all_triples;

  size_t pos = 0;
  while (true) {
    size_t desc = xml_content.find("<rdf:Description", pos);
    if (desc == std::string::npos) break;

    size_t desc_end = xml_content.find("</rdf:Description>", desc);
    if (desc_end == std::string::npos) {
      desc_end = xml_content.find("/>", desc);
      if (desc_end != std::string::npos) desc_end += 2;
    } else {
      desc_end += std::string("</rdf:Description>").size();
    }

    if (desc_end == std::string::npos) break;

    std::string_view element(xml_content.data() + desc, desc_end - desc);

    auto triples = ParseDescription(element, ns_map);
    all_triples.insert(all_triples.end(), triples.begin(), triples.end());

    pos = desc_end;
  }

  if (all_triples.empty()) {
    return STATUS(InvalidArgument, "No triples found in RDF/XML content");
  }

  return all_triples;
}

Result<std::vector<Triple>> RdfXmlParser::ParseFile(
    const std::string& file_path) {
  std::ifstream file(file_path);
  if (!file.is_open()) {
    return STATUS(IOError, "Cannot open file: " + file_path);
  }

  std::stringstream buffer;
  buffer << file.rdbuf();
  return ParseString(buffer.str());
}

}  // namespace yb::mdb
