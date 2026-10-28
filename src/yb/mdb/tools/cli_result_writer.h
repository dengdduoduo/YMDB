#pragma once

#include <cstdio>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include "yb/mdb/adapter/mdb_adapter.h"

// The staging file is published only after the adapter reports success.
class MdbCliResultWriter {
 public:
  MdbCliResultWriter(bool json, uint64_t limit) : json_(json), limit_(limit), spool_(std::tmpfile()) {
    if (!spool_) throw std::runtime_error("Cannot create MDB result staging file");
  }
  ~MdbCliResultWriter() { std::fclose(spool_); }

  static const YBCMdbResultCallbacks* Callbacks() {
    static const YBCMdbResultCallbacks callbacks = {
      [](YBCMdbResultKind kind, const YBCMdbColumn* cols, size_t size, void* ctx) {
        static_cast<MdbCliResultWriter*>(ctx)->Begin(kind, cols, size);
      },
      [](const YBCMdbTerm* terms, size_t size, void* ctx) {
        static_cast<MdbCliResultWriter*>(ctx)->Row(terms, size);
      },
      [](int value, void* ctx) { static_cast<MdbCliResultWriter*>(ctx)->Boolean(value); },
      [](const YBCMdbTerm* terms, void* ctx) {
        static_cast<MdbCliResultWriter*>(ctx)->Triple(terms);
      },
      [](void* ctx) { static_cast<MdbCliResultWriter*>(ctx)->End(); }
    };
    return &callbacks;
  }

  int Publish() {
    if (std::fflush(spool_) || std::fseek(spool_, 0, SEEK_SET)) return 1;
    char buffer[8192];
    for (;;) {
      auto count = std::fread(buffer, 1, sizeof(buffer), spool_);
      std::cout.write(buffer, count);
      if (count < sizeof(buffer)) break;
    }
    return std::ferror(spool_) || !std::cout.good() ? 1 : 0;
  }

 private:
  static std::string Quote(const std::string& value) {
    std::string out = "\"";
    const char* hex = "0123456789abcdef";
    for (unsigned char c : value) {
      if (c == '"' || c == '\\') { out += '\\'; out += c; }
      else if (c < 32) { out += "\\u00"; out += hex[c >> 4]; out += hex[c & 15]; }
      else out += c;
    }
    return out + "\"";
  }
  static std::string JsonTerm(const YBCMdbTerm& term) {
    const char* type = term.kind == YBC_MDB_IRI ? "uri" :
        term.kind == YBC_MDB_BLANK ? "bnode" : "literal";
    auto out = std::string("{\"type\":") + Quote(type) + ",\"value\":" + Quote(term.value);
    if (*term.language) out += ",\"xml:lang\":" + Quote(term.language);
    else if (*term.datatype) out += ",\"datatype\":" + Quote(term.datatype);
    return out + "}";
  }
  static std::string RdfTerm(const YBCMdbTerm& term) {
    if (term.kind == YBC_MDB_IRI) return "<" + std::string(term.value) + ">";
    if (term.kind == YBC_MDB_BLANK) return "_:" + std::string(term.value);
    auto out = Quote(term.value);
    if (*term.language) out += "@" + std::string(term.language);
    else if (*term.datatype) out += "^^<" + std::string(term.datatype) + ">";
    return out;
  }
  void Write(const std::string& value) {
    if (value.size() > limit_ || bytes_ > limit_ - value.size())
      throw std::runtime_error("MDB result staging failed or exceeded its byte limit");
    if (std::fwrite(value.data(), 1, value.size(), spool_) != value.size())
      throw std::runtime_error("MDB result staging failed");
    bytes_ += value.size();
  }
  void Begin(YBCMdbResultKind kind, const YBCMdbColumn* columns, size_t count) {
    kind_ = kind;
    for (size_t i = 0; i < count; ++i) {
      names_.emplace_back(columns[i].name);
      internal_.emplace_back(columns[i].internal_name);
    }
    if (!json_) return;
    if (kind == YBC_MDB_GRAPH) Write("{\"type\":\"graph\",\"triples\":[");
    else if (kind == YBC_MDB_BINDINGS) {
      Write("{\"head\":{\"vars\":[");
      for (size_t i = 0; i < names_.size(); ++i) {
        if (i) Write(",");
        Write(Quote(names_[i]));
      }
      Write("]},\"results\":{\"bindings\":[");
    }
  }
  void Row(const YBCMdbTerm* terms, size_t count) {
    if (json_) {
      if (rows_) Write(",");
      Write("{");
      bool first = true;
      for (size_t i = 0; i < count; ++i) if (terms[i].kind != YBC_MDB_UNBOUND) {
        if (!first) Write(",");
        first = false;
        Write(Quote(names_[i]) + ":" + JsonTerm(terms[i]));
      }
      Write("}");
    } else {
      if (rows_) Write("---\n");
      if (!count) Write("{}\n");
      for (size_t i = 0; i < count; ++i) {
        auto value = terms[i].kind == YBC_MDB_UNBOUND ? std::string("UNDEF") :
            terms[i].kind == YBC_MDB_BLANK ? "_:" + std::string(terms[i].value) :
            std::string(terms[i].value);
        Write(internal_[i] + "\t" + value + "\n");
      }
    }
    ++rows_;
  }
  void Boolean(int value) {
    Write(json_ ? std::string("{\"head\":{},\"boolean\":") + (value ? "true}" : "false}") :
                  std::string("boolean\t") + (value ? "true\n" : "false\n"));
  }
  void Triple(const YBCMdbTerm* terms) {
    if (json_) {
      if (rows_) Write(",");
      Write("[" + JsonTerm(terms[0]) + "," + JsonTerm(terms[1]) + "," + JsonTerm(terms[2]) + "]");
    } else Write(RdfTerm(terms[0]) + " " + RdfTerm(terms[1]) + " " + RdfTerm(terms[2]) + " .\n");
    ++rows_;
  }
  void End() {
    if (!json_) return;
    if (kind_ == YBC_MDB_BINDINGS) Write("]}}");
    else if (kind_ == YBC_MDB_GRAPH) Write("]}");
    Write("\n");
  }

  bool json_;
  uint64_t limit_, bytes_ = 0, rows_ = 0;
  FILE* spool_;
  YBCMdbResultKind kind_ = YBC_MDB_BINDINGS;
  std::vector<std::string> names_, internal_;
};
