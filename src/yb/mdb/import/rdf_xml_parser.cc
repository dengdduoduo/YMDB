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

#include "yb/mdb/import/rdf_xml_parser.h"

#include <libxml/parser.h>
#include <libxml/tree.h>
#include <libxml/uri.h>

#include <fstream>
#include <limits>
#include <memory>
#include <sstream>
#include <string_view>


namespace yb::mdb {
namespace {

constexpr const char* kRdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
constexpr const char* kXml = "http://www.w3.org/XML/1998/namespace";

std::string Text(const xmlChar* text) {
  return text ? reinterpret_cast<const char*>(text) : "";
}

std::string Take(xmlChar* text) {
  const auto value = Text(text);
  xmlFree(text);
  return value;
}

bool Is(xmlNode* node, const char* name) {
  return Text(node->name) == name && node->ns && Text(node->ns->href) == kRdf;
}

std::string Attribute(xmlNode* node, const char* name, const char* ns = kRdf) {
  return Take(xmlGetNsProp(node, BAD_CAST name, BAD_CAST ns));
}

bool Has(xmlNode* node, const char* name) {
  return xmlHasNsProp(node, BAD_CAST name, BAD_CAST kRdf) != nullptr;
}

Status Attributes(xmlNode* node, std::initializer_list<std::string_view> allowed) {
  for (auto* attr = node->properties; attr; attr = attr->next) {
    const auto ns = attr->ns ? Text(attr->ns->href) : "";
    const auto name = Text(attr->name);
    if (ns == kXml && (name == "lang" || name == "base" || name == "space")) continue;
    bool accepted = false;
    for (auto field : allowed) accepted |= ns == kRdf && name == field;
    if (!accepted) return STATUS(NotSupported, "Unsupported RDF/XML attribute: " + name);
  }
  return Status::OK();
}

Result<std::string> Iri(xmlNode* node, const std::string& value) {
  const auto base = Take(xmlNodeGetBase(node->doc, node));
  const auto resolved = base.empty() ? value :
      Take(xmlBuildURI(BAD_CAST value.c_str(), BAD_CAST base.c_str()));
  if (resolved.empty() || resolved.find(':') == std::string::npos) {
    return STATUS(InvalidArgument, "RDF/XML IRI needs an absolute value or xml:base");
  }
  return resolved;
}

Status StructuralText(xmlNode* node) {
  if (node->type == XML_COMMENT_NODE || node->type == XML_PI_NODE) return Status::OK();
  if (node->type == XML_TEXT_NODE && Text(node->content).find_first_not_of(" \t\r\n") ==
      std::string::npos) return Status::OK();
  return STATUS(InvalidArgument, "Unexpected text or node in RDF/XML structure");
}

}  // namespace

Result<std::vector<Triple>> RdfXmlParser::ParseString(const std::string& xml_content) {
  if (xml_content.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
    return STATUS(InvalidArgument, "RDF/XML document is too large");
  }
  std::unique_ptr<xmlDoc, decltype(&xmlFreeDoc)> document(xmlReadMemory(
      xml_content.data(), static_cast<int>(xml_content.size()), nullptr, nullptr,
      XML_PARSE_NONET | XML_PARSE_NOERROR | XML_PARSE_NOWARNING), xmlFreeDoc);
  if (!document) return STATUS(InvalidArgument, "Malformed RDF/XML document");
  // Entity declarations and external subsets are outside the supported import subset.
  if (document->intSubset || document->extSubset) {
    return STATUS(NotSupported, "RDF/XML DTD declarations are not supported");
  }
  auto* root = xmlDocGetRootElement(document.get());
  if (!root || !Is(root, "RDF")) return STATUS(NotSupported, "Expected rdf:RDF root");
  RETURN_NOT_OK(Attributes(root, {}));
  std::vector<Triple> triples;
  for (auto* description = root->children; description; description = description->next) {
    if (description->type != XML_ELEMENT_NODE) {
      RETURN_NOT_OK(StructuralText(description));
      continue;
    }
    if (!Is(description, "Description")) {
      return STATUS(NotSupported, "Only rdf:Description nodes are supported");
    }
    RETURN_NOT_OK(Attributes(description, {"about", "nodeID"}));
    const bool about = Has(description, "about");
    const bool blank = Has(description, "nodeID");
    if (about == blank) return STATUS(InvalidArgument, "Expected one RDF subject identity");
    Triple subject;
    subject.sub_type = blank ? TripleTermType::BLANK : TripleTermType::URI;
    subject.sub = blank ? "_:" + Attribute(description, "nodeID") :
        VERIFY_RESULT(Iri(description, Attribute(description, "about")));
    if (blank && subject.sub.size() == 2) return STATUS(InvalidArgument, "Empty RDF nodeID");
    for (auto* property = description->children; property; property = property->next) {
      if (property->type != XML_ELEMENT_NODE) {
        RETURN_NOT_OK(StructuralText(property));
        continue;
      }
      RETURN_NOT_OK(Attributes(property, {"resource", "nodeID", "datatype"}));
      if (!property->ns || !property->ns->href) {
        return STATUS(InvalidArgument, "RDF property needs a namespace");
      }
      const auto local = Text(property->name);
      if (Text(property->ns->href) == kRdf &&
          (local == "Description" || local == "RDF" || local == "li")) {
        return STATUS(NotSupported, "Unsupported RDF/XML property element");
      }
      const bool resource = Has(property, "resource");
      const bool node_id = Has(property, "nodeID");
      const bool datatype = Has(property, "datatype");
      if (static_cast<int>(resource) + node_id + datatype > 1) {
        return STATUS(InvalidArgument, "Conflicting RDF property identities");
      }
      for (auto* child = property->children; child; child = child->next) {
        if (child->type != XML_TEXT_NODE && child->type != XML_CDATA_SECTION_NODE &&
            child->type != XML_COMMENT_NODE && child->type != XML_PI_NODE) {
          return STATUS(NotSupported, "Nested RDF/XML property values are not supported");
        }
      }
      const auto content = Take(xmlNodeGetContent(property));
      auto triple = subject;
      triple.pred = Text(property->ns->href) + local;
      triple.pred_type = TripleTermType::URI;
      if (resource || node_id) {
        if (!content.empty()) {
          return STATUS(InvalidArgument, "Resource property has literal content");
        }
        triple.obj_type = node_id ? TripleTermType::BLANK : TripleTermType::URI;
        triple.obj = node_id ? "_:" + Attribute(property, "nodeID") :
            VERIFY_RESULT(Iri(property, Attribute(property, "resource")));
        if (node_id && triple.obj.size() == 2) return STATUS(InvalidArgument, "Empty RDF nodeID");
      } else {
        triple.obj_type = TripleTermType::LITERAL;
        triple.obj = content;
        if (datatype) {
          triple.object_type = VERIFY_RESULT(Iri(property, Attribute(property, "datatype")));
        } else {
          triple.object_language = Take(xmlNodeGetLang(property));
        }
      }
      triples.push_back(std::move(triple));
    }
  }
  return triples;
}

Result<std::vector<Triple>> RdfXmlParser::ParseFile(const std::string& file_path) {
  std::ifstream file(file_path);
  if (!file.is_open()) return STATUS(IOError, "Cannot open file: " + file_path);
  std::ostringstream buffer;
  buffer << file.rdbuf();
  if (file.bad()) return STATUS(IOError, "Failed reading file: " + file_path);
  return ParseString(buffer.str());
}

}  // namespace yb::mdb
