#pragma once

#include "function/table/call_functions.h"

namespace kuzu {
namespace fts_extension {

static std::unordered_set<std::string> stopWordsSet;

struct StringSplitNonStopWordsFunction : function::CallFunction {
    static constexpr const char* name = "STRING_SPLIT_NON_STOP_WORDS";

    static function::function_set getFunctionSet();
};

} // namespace fts_extension
} // namespace kuzu
