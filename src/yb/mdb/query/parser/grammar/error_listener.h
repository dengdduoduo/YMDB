#pragma once

#include "antlr4-runtime.h"

#include "query/exceptions.h"

namespace antlr4 {

class MyErrorListener : public BaseErrorListener {
public:
    void syntaxError(
        Recognizer* /* recognizer */,
        Token* offendingSymbol,
        size_t line,
        size_t charPositionInLine,
        const std::string& /*msg*/,
        std::exception_ptr /* e */
    ) override
    {
        std::string new_msg = "mismatched input `" + offendingSymbol->getText() + '`';

        throw QueryParsingException(new_msg, static_cast<int>(line), static_cast<int>(charPositionInLine));
    }
};

} // namespace antlr4
