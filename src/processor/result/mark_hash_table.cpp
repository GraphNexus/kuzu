#include "processor/result/mark_hash_table.h"

namespace kuzu {
namespace processor {

PatternCreationInfoTable::PatternCreationInfoTable(storage::MemoryManager& memoryManager,
    std::vector<common::LogicalType> keyTypes, std::vector<common::LogicalType> payloadTypes,
    uint64_t numEntriesToAllocate, FactorizedTableSchema tableSchema)
    : AggregateHashTable(memoryManager, std::move(keyTypes), std::move(payloadTypes),
          std::vector<std::unique_ptr<function::AggregateFunction>>{} /* empty aggregates */,
          std::vector<common::LogicalType>{} /* empty distinct agg key*/, numEntriesToAllocate,
          std::move(tableSchema)) {}

uint64_t PatternCreationInfoTable::matchFTEntries(const std::vector<common::ValueVector*>& flatKeyVectors,
    const std::vector<common::ValueVector*>& unFlatKeyVectors, uint64_t numMayMatches,
    uint64_t numNoMatches) {
    KU_ASSERT(unFlatKeyVectors.empty());
    numNoMatches = AggregateHashTable::matchFTEntries(flatKeyVectors, unFlatKeyVectors,
        numMayMatches, numNoMatches);
    KU_ASSERT(numMayMatches <= 1);
    // If we found the entry for the target key, we set tuple to the key tuple. Otherwise, simply
    // set tuple to nullptr.
    tuple = numMayMatches != 0 ? hashSlotsToUpdateAggState[mayMatchIdxes[0]]->entry : nullptr;
    return numNoMatches;
}

} // namespace processor
} // namespace kuzu
