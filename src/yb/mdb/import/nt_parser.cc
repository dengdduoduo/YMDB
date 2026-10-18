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

#include "yb/mdb/import/nt_parser.h"

#include <fstream>
#include <utility>

#include "yb/util/status.h"

namespace yb::mdb {
namespace {

bool Alpha(char c) { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'); }
bool Digit(char c) { return c >= '0' && c <= '9'; }

class Cursor {
 public:
  explicit Cursor(std::string_view input) : input_(input) {}

  void Skip() {
    while (position_ < input_.size()) {
      const auto c = input_[position_];
      if (c == ' ' || c == '\t' || c == '\r' || c == '\n') {
        ++position_;
      } else if (c == '#') {
        while (position_ < input_.size() && input_[position_] != '\n') ++position_;
      } else {
        break;
      }
    }
  }

  bool End() { Skip(); return position_ == input_.size(); }

  Result<Triple> Parse(bool optional_final_dot = false) {
    Triple triple;
    RETURN_NOT_OK(Resource(&triple.sub, &triple.sub_type));
    Skip();
    RETURN_NOT_OK(Resource(&triple.pred, &triple.pred_type));
    if (triple.pred_type != TripleTermType::URI) {
      return STATUS(InvalidArgument, "RDF predicate must be an IRI");
    }
    Skip();
    if (Peek() == '"') {
      triple.obj_type = TripleTermType::LITERAL;
      triple.obj = VERIFY_RESULT(Delimited('"', false));
      if (Peek() == '@') {
        ++position_;
        const auto start = position_;
        while (Alpha(Peek())) ++position_;
        if (position_ == start) return STATUS(InvalidArgument, "Empty language tag");
        while (Peek() == '-') {
          ++position_;
          const auto component = position_;
          while (Alpha(Peek()) || Digit(Peek())) ++position_;
          if (component == position_) return STATUS(InvalidArgument, "Invalid language tag");
        }
        triple.object_language = std::string(input_.substr(start, position_ - start));
      } else if (Peek() == '^') {
        ++position_;
        if (Peek() != '^') return STATUS(InvalidArgument, "Expected datatype marker");
        ++position_;
        TripleTermType kind;
        RETURN_NOT_OK(Resource(&triple.object_type, &kind));
        if (kind != TripleTermType::URI) return STATUS(InvalidArgument, "Invalid datatype IRI");
      }
    } else {
      RETURN_NOT_OK(Resource(&triple.obj, &triple.obj_type));
    }
    Skip();
    if (Peek() != '.') {
      if (optional_final_dot && End()) return triple;
      return STATUS(InvalidArgument, "Expected RDF triple terminator");
    }
    ++position_;
    return triple;
  }

 private:
  char Peek() const { return position_ < input_.size() ? input_[position_] : '\0'; }

  Status Unicode(std::string* text, int count) {
    uint32_t code = 0;
    for (int i = 0; i < count; ++i) {
      const auto c = Peek();
      int digit = Digit(c) ? c - '0' :
          (c >= 'a' && c <= 'f' ? c - 'a' + 10 : (c >= 'A' && c <= 'F' ? c - 'A' + 10 : -1));
      if (digit < 0) return STATUS(InvalidArgument, "Invalid Unicode escape");
      code = code * 16 + digit;
      ++position_;
    }
    if (code > 0x10ffff || (code >= 0xd800 && code <= 0xdfff)) {
      return STATUS(InvalidArgument, "Invalid Unicode scalar value");
    }
    if (code <= 0x7f) {
      text->push_back(static_cast<char>(code));
    } else {
      if (code > 0xffff) text->push_back(static_cast<char>(0xf0 | (code >> 18)));
      else if (code > 0x7ff) text->push_back(static_cast<char>(0xe0 | (code >> 12)));
      else
        text->push_back(static_cast<char>(0xc0 | (code >> 6)));
      if (code > 0xffff) text->push_back(static_cast<char>(0x80 | ((code >> 12) & 0x3f)));
      if (code > 0x7ff) text->push_back(static_cast<char>(0x80 | ((code >> 6) & 0x3f)));
      text->push_back(static_cast<char>(0x80 | (code & 0x3f)));
    }
    return Status::OK();
  }

  Result<std::string> Delimited(char delimiter, bool iri) {
    ++position_;
    std::string text;
    while (position_ < input_.size()) {
      const auto c = input_[position_++];
      if (c == delimiter) return text;
      if (c == '\n' || c == '\r') return STATUS(InvalidArgument, "Unescaped newline in RDF term");
      if (c == '\\') {
        const auto escape = Peek();
        if (!escape) return STATUS(InvalidArgument, "Truncated RDF escape");
        ++position_;
        if (escape == 'u' || escape == 'U') {
          RETURN_NOT_OK(Unicode(&text, escape == 'u' ? 4 : 8));
        } else {
          if (iri) return STATUS(InvalidArgument, "IRI escape must be Unicode");
          switch (escape) {
            case 't': text += '\t'; break;
            case 'b': text += '\b'; break;
            case 'n': text += '\n'; break;
            case 'r': text += '\r'; break;
            case 'f': text += '\f'; break;
            case '"': text += '"'; break;
            case '\'': text += '\''; break;
            case '\\': text += '\\'; break;
            default: return STATUS(InvalidArgument, "Unknown RDF escape");
          }
        }
      } else {
        if (iri && (static_cast<unsigned char>(c) <= 0x20 ||
            std::string_view("<>\"{}|^`").find(c) != std::string_view::npos)) {
          return STATUS(InvalidArgument, "Invalid IRI character");
        }
        text += c;
      }
    }
    return STATUS(InvalidArgument, "Unterminated RDF term");
  }

  Status Resource(std::string* text, TripleTermType* kind) {
    if (Peek() == '<') {
      *kind = TripleTermType::URI;
      *text = VERIFY_RESULT(Delimited('>', true));
      const auto colon = text->find(':');
      if (colon == std::string::npos || colon == 0 || !Alpha((*text)[0])) {
        return STATUS(InvalidArgument, "Expected absolute IRI");
      }
      return Status::OK();
    }
    if (Peek() != '_' || position_ + 1 >= input_.size() || input_[position_ + 1] != ':') {
      return STATUS(InvalidArgument, "Expected absolute IRI or labelled blank node");
    }
    *kind = TripleTermType::BLANK;
    const auto start = position_;
    position_ += 2;
    if (!(Alpha(Peek()) || Digit(Peek()) || Peek() == '_')) {
      return STATUS(NotSupported, "Unsupported or invalid blank node label");
    }
    while (Alpha(Peek()) || Digit(Peek()) || Peek() == '_' || Peek() == '-' || Peek() == '.') {
      ++position_;
    }
    // A trailing period belongs to the triple, not the blank node label.
    while (position_ > start + 2 && input_[position_ - 1] == '.') --position_;
    *text = std::string(input_.substr(start, position_ - start));
    return Status::OK();
  }

  std::string_view input_;
  size_t position_ = 0;
};

}  // namespace

Result<Triple> NtParser::ParseLine(const std::string& line) {
  if (line.find_first_of("\r\n") != std::string::npos) {
    return STATUS(InvalidArgument, "N-Triples statements must occupy one line");
  }
  Cursor cursor(line);
  if (cursor.End()) return STATUS(InvalidArgument, "No triple on this line");
  auto triple = VERIFY_RESULT(cursor.Parse());
  if (!cursor.End()) return STATUS(InvalidArgument, "Unexpected data after RDF triple");
  return triple;
}

Result<std::vector<Triple>> NtParser::ParseSequence(std::string_view text) {
  Cursor cursor(text);
  std::vector<Triple> triples;
  while (!cursor.End()) triples.push_back(VERIFY_RESULT(cursor.Parse(true)));
  return triples;
}

Status NtParser::ParseFile(
    const std::string& file_path, std::vector<Triple>& triples, ImportStats& stats) {
  std::ifstream file(file_path);
  if (!file.is_open()) return STATUS(IOError, "Cannot open file: " + file_path);
  std::vector<Triple> parsed;
  std::string line;
  while (std::getline(file, line)) {
    ++stats.lines_read;
    if (!line.empty() && line.back() == '\r') line.pop_back();
    const auto start = line.find_first_not_of(" \t");
    if (start == std::string::npos || line[start] == '#') continue;
    auto result = ParseLine(line);
    if (!result.ok()) { ++stats.errors; return result.status(); }
    parsed.push_back(std::move(*result));
  }
  if (file.bad()) return STATUS(IOError, "Failed reading file: " + file_path);
  stats.triples_parsed += static_cast<int64_t>(parsed.size());
  triples = std::move(parsed);
  return Status::OK();
}

}  // namespace yb::mdb
