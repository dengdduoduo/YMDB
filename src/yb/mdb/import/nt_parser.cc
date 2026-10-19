#include "yb/mdb/import/nt_parser.h"

#include <fstream>
#include <sstream>
#include <regex>
#include <cctype>
#include "misc/unicode_escape.h"

#include "yb/util/logging.h"
#include "yb/util/status.h"

namespace yb::mdb {

std::vector<std::string> NtParser::TokenizeLine(const std::string& line) {
  std::vector<std::string> tokens;
  std::string current;
  bool in_quotes = false;
  bool escaped = false;

  for (size_t i = 0; i < line.size(); ++i) {
    char ch = line[i];

    if (in_quotes && ch == '\\' && !escaped) {
      escaped = true;
      current += ch;
      continue;
    }
    if (in_quotes && escaped) {
      escaped = false;
      current += ch;
      continue;
    }
    if (ch == '#' && !in_quotes && current.empty()) break;
    if (ch == '"' && !in_quotes) {
      in_quotes = true;
      current += ch;
    } else if (ch == '"' && in_quotes) {
      in_quotes = false;
      current += ch;
    } else if (std::isspace(static_cast<unsigned char>(ch)) && !in_quotes) {
      if (!current.empty()) {
        tokens.push_back(current);
        current.clear();
      }
    } else {
      current += ch;
    }
  }

  if (!current.empty()) {
    tokens.push_back(current);
  }

  return tokens;
}

std::string NtParser::ParseUri(std::string_view token) {
  if (token.size() < 2) return std::string(token);
  if (token.front() == '<' && token.back() == '>') {
    return std::string(token.substr(1, token.size() - 2));
  }
  return std::string(token);
}

std::string NtParser::ParseBlankNode(std::string_view token) {
  return std::string(token);
}

Result<std::pair<std::string, std::string>> NtParser::ParseLiteral(std::string_view token) {
  std::string value;
  size_t pos = 1;
  for (; pos < token.size() && token[pos] != '"'; ++pos) {
    char c = token[pos];
    if (c != '\\') { value += c; continue; }
    if (++pos == token.size()) return STATUS(InvalidArgument, "Incomplete RDF escape");
    switch (token[pos]) {
      case 't': value += '\t'; break;
      case 'n': value += '\n'; break;
      case 'r': value += '\r'; break;
      case 'b': value += '\b'; break;
      case 'f': value += '\f'; break;
      case '"': value += '"'; break;
      case '\\': value += '\\'; break;
      case 'u': case 'U': {
        const size_t digits = token[pos] == 'u' ? 4 : 8;
        if (pos + digits >= token.size()) return STATUS(InvalidArgument, "Incomplete Unicode escape");
        uint32_t code = 0;
        for (size_t i = 1; i <= digits; ++i) {
          char h = token[pos + i];
          if (!UnicodeEscape::is_hex(h)) return STATUS(InvalidArgument, "Invalid Unicode escape");
          code = code * 16 + static_cast<uint32_t>(h <= '9' ? h - '0' :
              (h >= 'a' ? h - 'a' : h - 'A') + 10);
        }
        if (code > 0x10FFFF || (code >= 0xD800 && code <= 0xDFFF)) {
          return STATUS(InvalidArgument, "Invalid Unicode scalar");
        }
        value += UnicodeEscape::read_codepoint(token.data() + pos, token.data() + token.size(),
                                               static_cast<uint8_t>(digits));
        pos += digits;
        break;
      }
      default: return STATUS(InvalidArgument, "Unsupported RDF escape");
    }
  }
  if (pos == token.size()) return STATUS(InvalidArgument, "Unterminated RDF literal");
  auto suffix = token.substr(pos + 1);
  std::string qualifier;
  if (!suffix.empty()) {
    if (suffix[0] == '@') {
      static const std::regex language("[A-Za-z]+(-[A-Za-z0-9]+)*");
      if (!std::regex_match(std::string(suffix.substr(1)), language)) {
        return STATUS(InvalidArgument, "Invalid RDF language tag");
      }
      qualifier = std::string(suffix);  // Explicitly distinguish language from datatype.
    } else if (suffix.size() >= 5 && suffix.substr(0, 3) == "^^<" && suffix.back() == '>') {
      qualifier = std::string(suffix.substr(3, suffix.size() - 4));
      if (qualifier.find(':') == std::string::npos) return STATUS(InvalidArgument, "Datatype must be an absolute IRI");
    } else {
      return STATUS(InvalidArgument, "Invalid RDF literal suffix");
    }
  }
  return std::make_pair(std::move(value), std::move(qualifier));
}

Result<std::vector<std::string>> NtParser::SplitStatements(const std::string& text) {
  std::vector<std::string> statements;
  bool quoted = false, iri = false, escaped = false;
  size_t start = 0;
  for (size_t pos = 0; pos < text.size(); ++pos) {
    const char c = text[pos];
    if (escaped) { escaped = false; continue; }
    if (quoted && c == '\\') { escaped = true; continue; }
    if (!iri && c == '"') quoted = !quoted;
    if (!quoted && c == '<') iri = true;
    if (!quoted && c == '>') iri = false;
    if (!quoted && !iri && c == '.' &&
        (pos + 1 == text.size() || std::isspace(static_cast<unsigned char>(text[pos + 1])))) {
      auto statement = text.substr(start, pos + 1 - start);
      if (statement.find_first_not_of(" \t\r\n") != std::string::npos) statements.push_back(statement);
      start = pos + 1;
    }
  }
  if (quoted || iri || text.find_first_not_of(" \t\r\n", start) != std::string::npos) {
    return STATUS(InvalidArgument, "INSERT DATA requires complete RDF statements");
  }
  return statements;
}

Result<Triple> NtParser::ParseLine(const std::string& line) {
  std::vector<std::string> tokens = TokenizeLine(line);

  if (tokens.size() != 4) {
    return STATUS(InvalidArgument, "N-Triples line has fewer than 4 tokens");
  }

  if (tokens.back() != ".") {
    return STATUS(InvalidArgument, "N-Triples line does not end with '.'");
  }

  Triple triple;

  const std::string& subj_token = tokens[0];
  if (subj_token.front() == '<') {
    triple.sub = ParseUri(subj_token);
    triple.sub_type = TripleTermType::URI;
  } else if (subj_token.size() >= 2 && subj_token[0] == '_' && subj_token[1] == ':') {
    triple.sub = ParseBlankNode(subj_token);
    triple.sub_type = TripleTermType::BLANK;
  } else {
    return STATUS(InvalidArgument, "Invalid subject token: " + subj_token);
  }

  const std::string& pred_token = tokens[1];
  if (pred_token.front() == '<') {
    triple.pred = ParseUri(pred_token);
    triple.pred_type = TripleTermType::URI;
  } else {
    return STATUS(InvalidArgument, "Invalid predicate token: " + pred_token);
  }

  const std::string& obj_token = tokens[2];
  if (obj_token.front() == '<') {
    triple.obj = ParseUri(obj_token);
    triple.obj_type = TripleTermType::URI;
  } else if (obj_token.size() >= 2 && obj_token[0] == '_' && obj_token[1] == ':') {
    triple.obj = ParseBlankNode(obj_token);
    triple.obj_type = TripleTermType::BLANK;
  } else if (obj_token.front() == '"') {
    auto [value, type_info] = VERIFY_RESULT(ParseLiteral(obj_token));
    triple.obj = value;
    triple.obj_type = TripleTermType::LITERAL;
    triple.object_type = type_info;
  } else {
    return STATUS(InvalidArgument, "Invalid object token: " + obj_token);
  }

  return triple;
}

Status NtParser::ParseFile(const std::string& file_path,
                           std::vector<Triple>& triples,
                           ImportStats& stats) {
  std::ifstream file(file_path);
  if (!file.is_open()) {
    return STATUS(IOError, "Cannot open file: " + file_path);
  }

  triples.clear();

  std::string line;
  int line_num = 0;

  while (std::getline(file, line)) {
    line_num++;
    stats.lines_read++;

    if (line.empty() || line[0] == '#') {
      continue;
    }

    auto result = ParseLine(line);
    if (!result.ok()) {
      stats.errors++;
      LOG(WARNING) << "Line " << line_num << ": " << result.status();
      continue;
    }

    triples.push_back(std::move(*result));
    stats.triples_parsed++;
  }

  return Status::OK();
}

}  // namespace yb::mdb
