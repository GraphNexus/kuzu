#include "processor/operator/persistent/rel_batch_insert.h"

#include "common/exception/message.h"
#include "common/string_format.h"
#include "processor/result/factorized_table_util.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/storage_utils.h"
#include "storage/store/column_chunk_data.h"
#include "storage/store/rel_table.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::string RelBatchInsertPrintInfo::toString() const {
    std::string result = "Table Name: ";
    result += tableName;
    return result;
}

void RelBatchInsert::initLocalStateInternal(ResultSet* /*resultSet_*/, ExecutionContext* context) {
    localState = std::make_unique<RelBatchInsertLocalState>();
    const auto relInfo = info->ptrCast<RelBatchInsertInfo>();
    localState->chunkedGroup =
        std::make_unique<ChunkedCSRNodeGroup>(*context->clientContext->getMemoryManager(),
            relInfo->columnTypes, relInfo->compressionEnabled, 0, 0, ResidencyState::IN_MEMORY);
    const auto nbrTableID =
        relInfo->tableEntry->constCast<RelTableCatalogEntry>().getNbrTableID(relInfo->direction);
    const auto relTableID = relInfo->tableEntry->getTableID();
    // TODO(Guodong): Get rid of the hard-coded nbr and rel column ID 0/1.
    localState->chunkedGroup->getColumnChunk(0).getData().cast<InternalIDChunkData>().setTableID(
        nbrTableID);
    localState->chunkedGroup->getColumnChunk(1).getData().cast<InternalIDChunkData>().setTableID(
        relTableID);
    const auto relLocalState = localState->ptrCast<RelBatchInsertLocalState>();
    relLocalState->dummyAllNullDataChunk = std::make_unique<DataChunk>(relInfo->columnTypes.size());
    for (auto i = 0u; i < relInfo->columnTypes.size(); i++) {
        auto valueVector = std::make_shared<ValueVector>(relInfo->columnTypes[i].copy(),
            context->clientContext->getMemoryManager());
        valueVector->setAllNull();
        relLocalState->dummyAllNullDataChunk->insert(i, std::move(valueVector));
    }
    relLocalState->errorHandler =
        std::make_unique<BatchInsertErrorHandler>(context, info->ignoreErrors);
}

void RelBatchInsert::executeInternal(ExecutionContext* context) {
    const auto relInfo = info->ptrCast<RelBatchInsertInfo>();
    const auto relTable = sharedState->table->ptrCast<RelTable>();
    const auto relLocalState = localState->ptrCast<RelBatchInsertLocalState>();
    while (true) {
        relLocalState->nodeGroupIdx =
            partitionerSharedState->getNextPartition(relInfo->partitioningIdx);
        if (relLocalState->nodeGroupIdx == INVALID_PARTITION_IDX) {
            // No more partitions left in the partitioning buffer.
            break;
        }
        // TODO(Guodong): We need to handle the concurrency between COPY and other insertions into
        // the same node group.
        auto& nodeGroup =
            relTable->getOrCreateNodeGroup(relLocalState->nodeGroupIdx, relInfo->direction)
                ->cast<CSRNodeGroup>();
        appendNodeGroup(context->clientContext->getTx(), nodeGroup, *relInfo, *relLocalState,
            *sharedState, *partitionerSharedState);
    }
}

void RelBatchInsert::appendNodeGroup(transaction::Transaction* transaction, CSRNodeGroup& nodeGroup,
    const RelBatchInsertInfo& relInfo, RelBatchInsertLocalState& localState,
    BatchInsertSharedState& sharedState, const PartitionerSharedState& partitionerSharedState) {
    const auto nodeGroupIdx = localState.nodeGroupIdx;
    auto& partitioningBuffer =
        partitionerSharedState.getPartitionBuffer(relInfo.partitioningIdx, localState.nodeGroupIdx);
    const auto startNodeOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    for (auto& chunkedGroup : partitioningBuffer.getChunkedGroups()) {
        setOffsetToWithinNodeGroup(
            chunkedGroup->getColumnChunk(relInfo.boundNodeOffsetColumnID).getData(),
            startNodeOffset);
    }
    // Calculate num of source nodes in this node group.
    // This will be used to set the num of values of the node group.
    const auto numNodes = std::min(StorageConstants::NODE_GROUP_SIZE,
        partitionerSharedState.maxNodeOffsets[relInfo.partitioningIdx] - startNodeOffset + 1);
    const auto isNewNodeGroup = nodeGroup.isEmpty();
    // We optimistically flush new node group directly to disk in gapped CSR format.
    // There is no benefit of leaving gaps for existing node groups, which is kept in memory.
    const auto leaveGaps = isNewNodeGroup;
    populateCSRHeaderAndRowIdx(transaction, partitioningBuffer, startNodeOffset, relInfo,
        localState, numNodes, leaveGaps);
    const auto& csrHeader = localState.chunkedGroup->cast<ChunkedCSRNodeGroup>().getCSRHeader();
    const auto maxSize = csrHeader.getEndCSROffset(numNodes - 1);
    for (auto& chunkedGroup : partitioningBuffer.getChunkedGroups()) {
        sharedState.incrementNumRows(
            chunkedGroup->getNumRows() -
            chunkedGroup->getNumDeletions(transaction, 0, chunkedGroup->getNumRows()));
        localState.chunkedGroup->write(*chunkedGroup, relInfo.boundNodeOffsetColumnID);
        chunkedGroup->commitDelete(0, chunkedGroup->getNumRows(), transaction->getCommitTS());
    }
    // Reset num of rows in the chunked group to fill gaps at the end of the node group.
    auto numGapsAtEnd = maxSize - localState.chunkedGroup->getNumRows();
    KU_ASSERT(localState.chunkedGroup->getCapacity() >= maxSize);
    while (numGapsAtEnd > 0) {
        const auto numGapsToFill = std::min(numGapsAtEnd, DEFAULT_VECTOR_CAPACITY);
        localState.dummyAllNullDataChunk->state->getSelVectorUnsafe().setSelSize(numGapsToFill);
        std::vector<ValueVector*> dummyVectors;
        for (auto i = 0u; i < relInfo.columnTypes.size(); i++) {
            dummyVectors.push_back(localState.dummyAllNullDataChunk->getValueVector(i).get());
        }
        const auto numGapsFilled = localState.chunkedGroup->append(&transaction::DUMMY_TRANSACTION,
            dummyVectors, 0, numGapsToFill);
        KU_ASSERT(numGapsFilled == numGapsToFill);
        numGapsAtEnd -= numGapsFilled;
    }
    KU_ASSERT(localState.chunkedGroup->getNumRows() == maxSize);
    localState.chunkedGroup->finalize();
    if (isNewNodeGroup) {
        auto flushedChunkedGroup = localState.chunkedGroup->flushAsNewChunkedNodeGroup(transaction,
            *sharedState.table->getDataFH());
        nodeGroup.setPersistentChunkedGroup(std::move(flushedChunkedGroup));
    } else {
        nodeGroup.appendChunkedCSRGroup(transaction,
            localState.chunkedGroup->cast<ChunkedCSRNodeGroup>());
    }
    localState.chunkedGroup->resetToEmpty();
}

void RelBatchInsert::populateCSRHeaderAndRowIdx(const transaction::Transaction* transaction,
    InMemChunkedNodeGroupCollection& partition, offset_t startNodeOffset,
    const RelBatchInsertInfo& relInfo, RelBatchInsertLocalState& localState, offset_t numNodes,
    bool leaveGaps) {
    auto& csrNodeGroup = localState.chunkedGroup->cast<ChunkedCSRNodeGroup>();
    auto& csrHeader = csrNodeGroup.getCSRHeader();
    csrHeader.setNumValues(numNodes);
    // Populate lengths for each node and check multiplicity constraint.
    populateCSRLengths(transaction, csrHeader, numNodes, startNodeOffset, partition, relInfo,
        *localState.errorHandler);
    const auto rightCSROffsetOfRegions = csrHeader.populateStartCSROffsetsFromLength(leaveGaps);
    // Resize csr data column chunks.
    const auto csrChunkCapacity = rightCSROffsetOfRegions.back() + 1;
    localState.chunkedGroup->resizeChunks(csrChunkCapacity);
    localState.chunkedGroup->resetToAllNull();

    for (auto& chunkedGroup : partition.getChunkedGroups()) {
        auto& offsetChunk = chunkedGroup->getColumnChunk(relInfo.boundNodeOffsetColumnID);
        // We reuse bound node offset column to store row idx for each rel in the node group.
        setRowIdxFromCSROffsets(transaction, offsetChunk.getData(), csrHeader.offset->getData(),
            *chunkedGroup);
    }
    csrHeader.finalizeCSRRegionEndOffsets(rightCSROffsetOfRegions);
    KU_ASSERT(csrHeader.sanityCheck());
}

void RelBatchInsert::populateCSRLengths(const transaction::Transaction* transaction,
    const ChunkedCSRHeader& csrHeader, offset_t numNodes, offset_t startNodeOffset,
    InMemChunkedNodeGroupCollection& partition, const RelBatchInsertInfo& relInfo,
    BatchInsertErrorHandler& errorHandler) {
    const column_id_t boundNodeOffsetColumn = relInfo.boundNodeOffsetColumnID;

    auto& relTableEntry = relInfo.tableEntry->constCast<RelTableCatalogEntry>();
    const bool checkMultiplicityConstraint = relTableEntry.isSingleMultiplicity(relInfo.direction);

    KU_ASSERT(numNodes == csrHeader.length->getNumValues() &&
              numNodes == csrHeader.offset->getNumValues());
    const auto lengthData = reinterpret_cast<length_t*>(csrHeader.length->getData().getData());
    std::fill(lengthData, lengthData + numNodes, 0);
    for (auto& chunkedGroup : partition.getChunkedGroups()) {
        auto& offsetChunk = chunkedGroup->getColumnChunk(boundNodeOffsetColumn);
        for (auto i = 0u; i < offsetChunk.getNumValues(); i++) {
            const auto nodeOffset = offsetChunk.getData().getValue<offset_t>(i);
            KU_ASSERT(nodeOffset < numNodes);
            const bool skipCurrentRow =
                checkMultiplicityConstraint &&
                !checkRelMultiplicityConstraint(csrHeader, nodeOffset - startNodeOffset);
            if (skipCurrentRow) {
                errorHandler.handleError(ExceptionMessage::violateRelMultiplicityConstraint(
                    relInfo.tableEntry->getName(), std::to_string(nodeOffset),
                    RelDataDirectionUtils::relDirectionToString(relInfo.direction)));
                chunkedGroup->delete_(transaction, i);
            } else {
                lengthData[nodeOffset]++;
            }
        }
    }
}

void RelBatchInsert::setOffsetToWithinNodeGroup(ColumnChunkData& chunk, offset_t startOffset) {
    KU_ASSERT(chunk.getDataType().getPhysicalType() == PhysicalTypeID::INTERNAL_ID);
    const auto offsets = reinterpret_cast<offset_t*>(chunk.getData());
    for (auto i = 0u; i < chunk.getNumValues(); i++) {
        offsets[i] -= startOffset;
    }
}

void RelBatchInsert::setRowIdxFromCSROffsets(const transaction::Transaction* transaction,
    ColumnChunkData& rowIdxChunk, ColumnChunkData& csrOffsetChunk, ChunkedNodeGroup& nodeGroup) {
    KU_ASSERT(rowIdxChunk.getDataType().getPhysicalType() == PhysicalTypeID::INTERNAL_ID);
    for (auto i = 0u; i < rowIdxChunk.getNumValues(); i++) {
        if (!nodeGroup.isDeleted(transaction, i)) {
            const auto nodeOffset = rowIdxChunk.getValue<offset_t>(i);
            const auto csrOffset = csrOffsetChunk.getValue<offset_t>(nodeOffset);
            rowIdxChunk.setValue<offset_t>(csrOffset, i);
            // Increment current csr offset for nodeOffset by 1.
            csrOffsetChunk.setValue<offset_t>(csrOffset + 1, nodeOffset);
        }
    }
}

bool RelBatchInsert::checkRelMultiplicityConstraint(const ChunkedCSRHeader& csrHeader,
    offset_t chunkOffset) {
    return (csrHeader.length->getData().getValue<length_t>(chunkOffset) < 1);
}

void RelBatchInsert::finalizeInternal(ExecutionContext* context) {
    const auto relInfo = info->ptrCast<RelBatchInsertInfo>();
    if (relInfo->direction == RelDataDirection::BWD) {
        KU_ASSERT(
            relInfo->partitioningIdx == partitionerSharedState->partitioningBuffers.size() - 1);

        auto outputMsg = stringFormat("{} tuples have been copied to the {} table.",
            sharedState->getNumRows(), info->tableEntry->getName());
        FactorizedTableUtils::appendStringToTable(sharedState->fTable.get(), outputMsg,
            context->clientContext->getMemoryManager());

        const auto warningCount =
            context->clientContext->getWarningContextUnsafe().getWarningCount(context->queryID);
        if (warningCount > 0) {
            auto warningMsg =
                stringFormat("{} warnings encountered during copy. Use 'CALL "
                             "show_warnings() RETURN *' to view the actual warnings. Query ID: {}",
                    warningCount, context->queryID);
            FactorizedTableUtils::appendStringToTable(sharedState->fTable.get(), warningMsg,
                context->clientContext->getMemoryManager());
        }
    }
    sharedState->numRows.store(0);
    sharedState->table->cast<RelTable>().setHasChanges();
    partitionerSharedState->resetState();
    partitionerSharedState->partitioningBuffers[relInfo->partitioningIdx].reset();
}

} // namespace processor
} // namespace kuzu
