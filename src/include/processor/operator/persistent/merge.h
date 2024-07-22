#pragma once

#include "insert_executor.h"
#include "processor/operator/aggregate/hash_aggregate.h"
#include "processor/operator/physical_operator.h"
#include "processor/result/mark_hash_table.h"
#include "set_executor.h"

namespace kuzu {
namespace processor {

struct MergePrintInfo final : OPPrintInfo {
    binder::expression_vector pattern;
    std::vector<binder::expression_pair> onCreate;
    std::vector<binder::expression_pair> onMatch;

    MergePrintInfo(binder::expression_vector pattern, std::vector<binder::expression_pair> onCreate,
        std::vector<binder::expression_pair> onMatch)
        : pattern(std::move(pattern)), onCreate(std::move(onCreate)), onMatch(std::move(onMatch)) {}

    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<MergePrintInfo>(new MergePrintInfo(*this));
    }

private:
    MergePrintInfo(const MergePrintInfo& other)
        : OPPrintInfo(other), pattern(other.pattern), onCreate(other.onCreate),
          onMatch(other.onMatch) {}
};

struct MergeLocalState {
    std::vector<common::ValueVector*> keyVectors;
    std::vector<common::ValueVector*> unflatKeys;
    std::vector<common::ValueVector*> dependentKeys;
    common::DataChunkState* state;
    std::unique_ptr<MarkHashTable> hashTable;

    void init(ResultSet& resultSet, main::ClientContext* context, HashAggregateInfo& info) {
        std::vector<common::LogicalType> types;
        for (auto& dataPos : info.flatKeysPos) {
            auto keyVector = resultSet.getValueVector(dataPos).get();
            types.push_back(keyVector->dataType.copy());
            keyVectors.push_back(keyVector);
            state = keyVector->state.get();
        }
        hashTable = std::make_unique<MarkHashTable>(*context->getMemoryManager(), std::move(types),
            std::vector<common::LogicalType>{}, 0, std::move(info.tableSchema));
    }

    void append(uint64_t multiplicity) const {
        hashTable->append(keyVectors, unflatKeys, dependentKeys, state,
            std::vector<AggregateInput>{}, multiplicity);
    }
};

struct MergeInfo : HashAggregateInfo {};

class Merge : public PhysicalOperator {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::MERGE;

public:
    Merge(const DataPos& existenceMark, std::vector<NodeInsertExecutor> nodeInsertExecutors,
        std::vector<RelInsertExecutor> relInsertExecutors,
        std::vector<std::unique_ptr<NodeSetExecutor>> onCreateNodeSetExecutors,
        std::vector<std::unique_ptr<RelSetExecutor>> onCreateRelSetExecutors,
        std::vector<std::unique_ptr<NodeSetExecutor>> onMatchNodeSetExecutors,
        std::vector<std::unique_ptr<RelSetExecutor>> onMatchRelSetExecutors, MergeInfo info,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, std::move(child), id, std::move(printInfo)},
          existenceMark{existenceMark}, nodeInsertExecutors{std::move(nodeInsertExecutors)},
          relInsertExecutors{std::move(relInsertExecutors)},
          onCreateNodeSetExecutors{std::move(onCreateNodeSetExecutors)},
          onCreateRelSetExecutors{std::move(onCreateRelSetExecutors)},
          onMatchNodeSetExecutors{std::move(onMatchNodeSetExecutors)},
          onMatchRelSetExecutors{std::move(onMatchRelSetExecutors)}, info{std::move(info)} {}

    bool isParallel() const final { return false; }

    void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final;

private:
    DataPos existenceMark;
    common::ValueVector* existenceVector = nullptr;

    std::vector<NodeInsertExecutor> nodeInsertExecutors;
    std::vector<RelInsertExecutor> relInsertExecutors;

    std::vector<std::unique_ptr<NodeSetExecutor>> onCreateNodeSetExecutors;
    std::vector<std::unique_ptr<RelSetExecutor>> onCreateRelSetExecutors;

    std::vector<std::unique_ptr<NodeSetExecutor>> onMatchNodeSetExecutors;
    std::vector<std::unique_ptr<RelSetExecutor>> onMatchRelSetExecutors;

    MergeInfo info;
    MergeLocalState localState;
    bool hasInserted = false;
};

} // namespace processor
} // namespace kuzu
