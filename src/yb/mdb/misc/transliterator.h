#pragma once
#include <string>
struct Transliterator {
  static std::string transliterate(const std::string& s) { return s; }
  static std::string lowercase(const std::string& s) {
    std::string r = s;
    for (auto& c : r) c = tolower(c);
    return r;
  }
  static std::string substr(const std::string& s, size_t start, size_t length) {
    if (start >= s.size()) return "";
    return s.substr(start, length);
  }
  static std::string uppercase(const std::string& s) {
    std::string r = s;
    for (auto& c : r) c = toupper(c);
    return r;
  }
};
inline std::string transliterate(const std::string& s) { return Transliterator::transliterate(s); }
