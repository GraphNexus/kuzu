#include "common/serializer/serializer.h"

#include "common/assert.h"

namespace kuzu {
namespace common {

template<>
void Serializer::serializeValue(const std::string& value) {
    uint64_t valueLength = value.length();
    writer->write((uint8_t*)&valueLength, sizeof(uint64_t));
    writer->write((uint8_t*)value.data(), valueLength);
}

void Serializer::writeDebuggingInfo(const std::string& value) {
#if defined(KUZU_DESER_DEBUG) && (defined(KUZU_RUNTIME_CHECKS) || !defined(NDEBUG))
    serializeValue<std::string>(value);
#endif
    // DO NOTHING
    KU_UNUSED(value);
}

void Serializer::serializeCaseInsensitiveSet(const common::case_insensitve_set_t& values) {
    uint64_t setSize = values.size();
    serializeValue(setSize);
    for (const auto& value : values) {
        serializeValue(value);
    }
}

} // namespace common
} // namespace kuzu
