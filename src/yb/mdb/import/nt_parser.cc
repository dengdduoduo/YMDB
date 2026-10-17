#include "yb/mdb/import/nt_parser.h"

#include <fstream>
#include <sstream>

#include "yb/util/logging.h"
#include "yb/util/status.h"

namespace yb::mdb {

std::vector<std::string> NtParser::TokenizeLine(const std::string& line) {
  std::vector<std::string> tokens;
  std::string current;
  bool in_quotes = false;

  for (size_t i = 0; i < line.size(); ++i) {
    char ch = line[i];

    if (ch == '"' && !in_quotes) {
      in_quotes = true;
      current += ch;
    } else if (ch == '"' && in_quotes) {
      in_quotes = false;
      current += ch;
    } else if ((ch == ' ' || ch == '\t') && !in_quotes) {
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

std::pair<std::string, std::string> NtParser::ParseLiteral(std::string_view token) {
  std::pair<std::string, std::string> result;

  size_t end_quote = token.find('"', 1);
  while (end_quote != std::string_view::npos) {
    if (end_quote + 1 < token.size() && token[end_quote + 1] == '"') {
      end_quote = token.find('"', end_quote + 2);
      continue;
    }
    break;
  }

  if (end_quote == std::string_view::npos) {
    result.first = std::string(token.substr(1, token.size() - 1));
  } else {
    result.first = std::string(token.substr(1, end_quote - 1));
  }

  size_t after_quote = end_quote + 1;
  if (after_quote < token.size()) {
    std::string_view suffix = token.substr(after_quote);
    if (suffix.size() >= 2 && suffix[0] == '@') {
      result.second = std::string(suffix.substr(1));
    } else if (suffix.size() >= 2 && suffix[0] == '^' && suffix[1] == '^') {
      std::string_view type_part = suffix.substr(2);
      if (type_part.size() >= 2 && type_part.front() == '<' && type_part.back() == '>') {
        result.second = std::string(type_part.substr(1, type_part.size() - 2));
      } else {
        result.second = std::string(type_part);
      }
    }
  }

  return result;
}

Result<Triple> NtParser::ParseLine(const std::string& line) {
  std::vector<std::string> tokens = TokenizeLine(line);

  if (tokens.size() < 4) {
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
    auto [value, type_info] = ParseLiteral(obj_token);
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
