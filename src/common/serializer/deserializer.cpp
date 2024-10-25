#include "common/serializer/deserializer.h"

namespace kuzu {
namespace common {

template<>
void Deserializer::deserializeValue(std::string& value) {
    uint64_t valueLength = 0;
    deserializeValue(valueLength);
    value.resize(valueLength);
    reader->read(reinterpret_cast<uint8_t*>(value.data()), valueLength);
}

void Deserializer::validateDebuggingInfo(std::string& value, const std::string& expectedVal) {
#if defined(KUZU_DESER_DEBUG) && (defined(KUZU_RUNTIME_CHECKS) || !defined(NDEBUG))
    deserializeValue<std::string>(value);
    KU_ASSERT(value == expectedVal);
#endif
    // DO NOTHING
    KU_UNUSED(value);
    KU_UNUSED(expectedVal);
}

void Deserializer::deserializeCaseInsensitiveSet(common::case_insensitve_set_t& values) {
    uint64_t setSize = 0;
    deserializeValue(setSize);
    for (auto i = 0u; i < setSize; i++) {
        std::string value;
        deserializeValue(value);
        values.insert(value);
    }
}

} // namespace common
} // namespace kuzu
