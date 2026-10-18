#pragma once
#include <stdexcept>
#include <string>
struct InterruptedException : std::runtime_error {
  InterruptedException() : std::runtime_error("interrupted") {}
};
struct NotSupportedException : std::runtime_error {
  NotSupportedException(const std::string& s) : std::runtime_error(s) {}
};
struct LogicException : std::runtime_error {
  LogicException(const std::string& s) : std::runtime_error(s) {}
};
struct QueryException : std::runtime_error {
  QueryException(const std::string& s) : std::runtime_error(s) {}
};
struct QueryParsingException : QueryException {
  QueryParsingException(const std::string& s) : QueryException(s), line_(0), col_(0) {}
  QueryParsingException(const std::string& s, int l, int c) : QueryException(s), line_(l), col_(c) {}
  int line_, col_;
};
struct QuerySemanticException : QueryException {
  QuerySemanticException(const std::string& s) : QueryException(s) {}
};
