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
    relLocalState->errorHandler->flushStoredErrors();
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
    const auto erroneousRows = populateCSRHeaderAndRowIdx(partitioningBuffer, startNodeOffset,
        relInfo, localState, numNodes, leaveGaps);
    const auto& csrHeader = localState.chunkedGroup->cast<ChunkedCSRNodeGroup>().getCSRHeader();
    const auto maxSize = csrHeader.getEndCSROffset(numNodes - 1);
    for (auto& chunkedGroup : partitioningBuffer.getChunkedGroups()) {
        sharedState.incrementNumRows(
            chunkedGroup->getNumRows() -
            chunkedGroup->getNumDeletions(transaction, 0, chunkedGroup->getNumRows()));
        localState.chunkedGroup->write(*chunkedGroup, relInfo.boundNodeOffsetColumnID);
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
    // only write the output data to the rel table
    ChunkedCSRNodeGroup sliceToWriteToDisk(localState.chunkedGroup->cast<ChunkedCSRNodeGroup>(),
        relInfo.outputDataColumns);
    if (isNewNodeGroup) {
        auto flushedChunkedGroup = sliceToWriteToDisk.flushAsNewChunkedNodeGroup(transaction,
            *sharedState.table->getDataFH());
        nodeGroup.setPersistentChunkedGroup(std::move(flushedChunkedGroup));
    } else {
        nodeGroup.appendChunkedCSRGroup(transaction, sliceToWriteToDisk);
    }
    localState.chunkedGroup->merge(sliceToWriteToDisk, relInfo.outputDataColumns);

    for (row_idx_t erroneousRow : erroneousRows) {
        nodeGroup.delete_(transaction, CSRNodeGroupScanSource::UNCOMMITTED, erroneousRow);
    }

    localState.chunkedGroup->resetToEmpty();
}

std::vector<row_idx_t> RelBatchInsert::populateCSRHeaderAndRowIdx(
    InMemChunkedNodeGroupCollection& partition, offset_t startNodeOffset,
    const RelBatchInsertInfo& relInfo, RelBatchInsertLocalState& localState, offset_t numNodes,
    bool leaveGaps) {
    auto& csrNodeGroup = localState.chunkedGroup->cast<ChunkedCSRNodeGroup>();
    auto& csrHeader = csrNodeGroup.getCSRHeader();
    csrHeader.setNumValues(numNodes);
    // Populate lengths for each node and check multiplicity constraint.
    auto erroneousRows = populateCSRLengths(csrHeader, numNodes, startNodeOffset, partition,
        relInfo, *localState.errorHandler);
    const auto rightCSROffsetOfRegions = csrHeader.populateStartCSROffsetsFromLength(leaveGaps);
    // Resize csr data column chunks.
    const auto csrChunkCapacity = rightCSROffsetOfRegions.back() + 1;
    localState.chunkedGroup->resizeChunks(csrChunkCapacity);
    localState.chunkedGroup->resetToAllNull();

    for (auto& chunkedGroup : partition.getChunkedGroups()) {
        auto& offsetChunk = chunkedGroup->getColumnChunk(relInfo.boundNodeOffsetColumnID);
        // We reuse bound node offset column to store row idx for each rel in the node group.
        setRowIdxFromCSROffsets(offsetChunk.getData(), csrHeader.offset->getData(), erroneousRows);
    }
    csrHeader.finalizeCSRRegionEndOffsets(rightCSROffsetOfRegions);
    KU_ASSERT(csrHeader.sanityCheck());

    return erroneousRows;
}

template<typename T>
static T getValueFromChunkedNodeGroup(const ChunkedNodeGroup& nodeGroup, column_id_t column,
    common::idx_t row) {
    return nodeGroup.getColumnChunk(column).getConstData().getValue<T>(row);
}

static std::optional<WarningSourceData> getWarningDataFromChunks(const ChunkedNodeGroup& nodeGroup,
    const std::vector<column_id_t> warningColumns, common::idx_t posInChunk) {
    std::optional<WarningSourceData> ret;
    if (!warningColumns.empty()) {
        KU_ASSERT(warningColumns.size() == CopyConstants::WARNING_METADATA_NUM_COLUMNS);
        KU_ASSERT(
            std::find_if(warningColumns.begin(), warningColumns.end(), [&nodeGroup](auto columnId) {
                return (columnId >= nodeGroup.getNumColumns());
            }) == warningColumns.end());
        ret = WarningSourceData{
            getValueFromChunkedNodeGroup<decltype(WarningSourceData::startByteOffset)>(nodeGroup,
                warningColumns[0], posInChunk),
            getValueFromChunkedNodeGroup<decltype(WarningSourceData::endByteOffset)>(nodeGroup,
                warningColumns[1], posInChunk),
            getValueFromChunkedNodeGroup<decltype(WarningSourceData::fileIdx)>(nodeGroup,
                warningColumns[2], posInChunk),
            getValueFromChunkedNodeGroup<decltype(WarningSourceData::blockIdx)>(nodeGroup,
                warningColumns[3], posInChunk),
            getValueFromChunkedNodeGroup<decltype(WarningSourceData::rowOffsetInBlock)>(nodeGroup,
                warningColumns[4], posInChunk)};
    }
    return ret;
}

std::vector<row_idx_t> RelBatchInsert::populateCSRLengths(const ChunkedCSRHeader& csrHeader,
    offset_t numNodes, offset_t startNodeOffset, InMemChunkedNodeGroupCollection& partition,
    const RelBatchInsertInfo& relInfo, BatchInsertErrorHandler& errorHandler) {
    const column_id_t boundNodeOffsetColumn = relInfo.boundNodeOffsetColumnID;

    auto& relTableEntry = relInfo.tableEntry->constCast<RelTableCatalogEntry>();
    const bool checkMultiplicityConstraint = relTableEntry.isSingleMultiplicity(relInfo.direction);

    KU_ASSERT(numNodes == csrHeader.length->getNumValues() &&
              numNodes == csrHeader.offset->getNumValues());
    const auto lengthData = reinterpret_cast<length_t*>(csrHeader.length->getData().getData());
    std::fill(lengthData, lengthData + numNodes, 0);

    std::vector<row_idx_t> erroneousRows;
    for (auto& chunkedGroup : partition.getChunkedGroups()) {
        auto& offsetChunk = chunkedGroup->getColumnChunk(boundNodeOffsetColumn);
        for (auto i = 0u; i < offsetChunk.getNumValues(); i++) {
            const auto nodeOffset = offsetChunk.getData().getValue<offset_t>(i);
            KU_ASSERT(nodeOffset < numNodes);
            const auto chunkOffset = nodeOffset - startNodeOffset;
            const bool skipCurrentRow = checkMultiplicityConstraint &&
                                        !checkRelMultiplicityConstraint(csrHeader, chunkOffset);
            if (skipCurrentRow) {
                errorHandler.handleError(
                    ExceptionMessage::violateRelMultiplicityConstraint(
                        relInfo.tableEntry->getName(), std::to_string(nodeOffset),
                        RelDataDirectionUtils::relDirectionToString(relInfo.direction)),
                    getWarningDataFromChunks(*chunkedGroup, relInfo.warningDataColumns,
                        chunkOffset));
                erroneousRows.push_back(i);
            } else {
                lengthData[nodeOffset]++;
            }
        }
    }

    return erroneousRows;
}

void RelBatchInsert::setOffsetToWithinNodeGroup(ColumnChunkData& chunk, offset_t startOffset) {
    KU_ASSERT(chunk.getDataType().getPhysicalType() == PhysicalTypeID::INTERNAL_ID);
    const auto offsets = reinterpret_cast<offset_t*>(chunk.getData());
    for (auto i = 0u; i < chunk.getNumValues(); i++) {
        offsets[i] -= startOffset;
    }
}

void RelBatchInsert::setRowIdxFromCSROffsets(ColumnChunkData& rowIdxChunk,
    ColumnChunkData& csrOffsetChunk, const std::vector<common::row_idx_t>& erroneousRows) {
    KU_ASSERT(rowIdxChunk.getDataType().getPhysicalType() == PhysicalTypeID::INTERNAL_ID);
    idx_t j = 0;
    for (auto i = 0u; i < rowIdxChunk.getNumValues(); i++) {
        if (j >= erroneousRows.size() || i < erroneousRows[j]) {
            const auto nodeOffset = rowIdxChunk.getValue<offset_t>(i);
            const auto csrOffset = csrOffsetChunk.getValue<offset_t>(nodeOffset);
            rowIdxChunk.setValue<offset_t>(csrOffset, i);
            // Increment current csr offset for nodeOffset by 1.
            csrOffsetChunk.setValue<offset_t>(csrOffset + 1, nodeOffset);
        } else {
            ++j;
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
