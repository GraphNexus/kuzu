#include "processor/operator/aggregate/aggregate_hash_table.h"

#pragma once

namespace kuzu {
namespace processor {

struct PatternCreationInfo {
    uint8_t* tuple;
    bool isDistinct;

    common::nodeID_t getNodeID(common::executor_id_t matchExecutorID) const {
        auto ftColIndex = matchExecutorID;
        return *(common::nodeID_t*)(tuple + ftColIndex * sizeof(common::nodeID_t));
    }

    void updateID(common::executor_id_t executorID, common::executor_info executorInfo,
        common::nodeID_t nodeID) {
        if (!executorInfo.contains(executorID)) {
            return;
        }
        auto ftColIndex = executorInfo.at(executorID);
        *(common::nodeID_t*)(tuple + ftColIndex * sizeof(common::nodeID_t)) = nodeID;
    }
};

class PatternCreationInfoTable : public AggregateHashTable {
public:
    PatternCreationInfoTable(storage::MemoryManager& memoryManager,
        std::vector<common::LogicalType> keyTypes, std::vector<common::LogicalType> payloadTypes,
        uint64_t numEntriesToAllocate, FactorizedTableSchema tableSchema);

    PatternCreationInfo append(const std::vector<common::ValueVector*>& keyVectors) {
        KU_ASSERT(!keyVectors.empty());
        resizeHashTableIfNecessary(1);
        computeVectorHashes(keyVectors, std::vector<common::ValueVector*>{});
        findHashSlots(keyVectors, std::vector<common::ValueVector*>{},
            std::vector<common::ValueVector*>{}, keyVectors[0]->state.get());
        auto nodeIDColOffset = factorizedTable->getTableSchema()->getColOffset(keyVectors.size());
        return tuple == nullptr ? PatternCreationInfo{factorizedTable->getTuple(
                                                          factorizedTable->getNumTuples() - 1) +
                                                          nodeIDColOffset,
                                      true} :
                                  PatternCreationInfo{tuple + nodeIDColOffset, false};
    }

    uint64_t matchFTEntries(const std::vector<common::ValueVector*>& flatKeyVectors,
        const std::vector<common::ValueVector*>& unFlatKeyVectors, uint64_t numMayMatches,
        uint64_t numNoMatches) override;

private:
    std::unordered_set<uint64_t> onMatchSlotIdxes;
    uint8_t* tuple;
};

} // namespace processor
} // namespace kuzu
