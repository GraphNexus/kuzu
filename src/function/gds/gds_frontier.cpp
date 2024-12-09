#include "function/gds/gds_frontier.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

FrontierMorselDispatcher::FrontierMorselDispatcher(uint64_t maxThreads)
    : tableID{INVALID_TABLE_ID}, maxOffset{INVALID_OFFSET}, maxThreads{maxThreads},
      morselSize(UINT64_MAX) {
    nextOffset.store(INVALID_OFFSET);
}

void FrontierMorselDispatcher::init(common::table_id_t _tableID, common::offset_t _maxOffset) {
    tableID = _tableID;
    maxOffset = _maxOffset;
    nextOffset.store(0u);
    // Frontier size calculation: The ideal scenario is to have k^2 many morsels where k
    // the number of maximum threads that could be working on this frontier. However, if
    // that is too small then we default to MIN_FRONTIER_MORSEL_SIZE.
    auto idealMorselSize =
        maxOffset / std::max(MIN_NUMBER_OF_FRONTIER_MORSELS, maxThreads * maxThreads);
    morselSize = std::max(MIN_FRONTIER_MORSEL_SIZE, idealMorselSize);
}

bool FrontierMorselDispatcher::getNextRangeMorsel(FrontierMorsel& frontierMorsel) {
    auto beginOffset = nextOffset.fetch_add(morselSize, std::memory_order_acq_rel);
    if (beginOffset >= maxOffset) {
        return false;
    }
    auto endOffset = beginOffset + morselSize > maxOffset ? maxOffset : beginOffset + morselSize;
    frontierMorsel.init(tableID, beginOffset, endOffset);
    return true;
}

void SparseFrontier::pinTableID(common::table_id_t tableID) {
    curTableID = tableID;
    if (!tableIDToOffsetMap.contains(tableID)) {
        tableIDToOffsetMap.insert({tableID, std::unordered_set<common::offset_t>{}});
    }
    curOffsetSet = &tableIDToOffsetMap.at(tableID);
}

void SparseFrontier::addNode(common::nodeID_t nodeID) {
    std::unique_lock<std::mutex> lck{mtx};
    pinTableID(nodeID.tableID);
    curOffsetSet->insert(nodeID.offset);
}

void SparseFrontier::addNodes(const std::vector<common::nodeID_t> nodeIDs) {
    if (!enabled_) {
        return;
    }
    KU_ASSERT(tableIDToOffsetMap.size() == 1);
    for (auto nodeID : nodeIDs) {
        curOffsetSet->insert(nodeID.offset);
    }
}

void SparseFrontier::checkSampleSize() {
    if (!enabled_) {
        return;
    }
    enabled_ = curOffsetSet->size() < SAMPLE_SIZE;
}

void SparseFrontier::mergeLocalFrontier(const SparseFrontier& localFrontier) {
    std::unique_lock<std::mutex> lck{mtx};
    KU_ASSERT(localFrontier.enabled());
    for (auto& offset : localFrontier.getOffsetSet()) {
        curOffsetSet->insert(offset);
    }
}

void SparseFrontier::mergeSparseFrontier(const SparseFrontier& other) {
    std::unique_lock<std::mutex> lck{mtx};
    if (!enabled()) {
        return;
    }
    if (!other.enabled()) {
        disable();
        return;
    }
    for (auto [tableID, offsetSet] : other.tableIDToOffsetMap) {
        pinTableID(tableID);
        curOffsetSet->insert(offsetSet.begin(), offsetSet.end());
    }
}

PathLengths::PathLengths(const common::table_id_map_t<common::offset_t>& numNodesMap_,
    storage::MemoryManager* mm)
    : GDSFrontier{numNodesMap_} {
    curIter.store(0);
    for (const auto& [tableID, numNodes] : numNodesMap_) {
        auto memBuffer = mm->allocateBuffer(false, numNodes * sizeof(frontier_entry_t));
        masks.insert({tableID, std::move(memBuffer)});
    }
}

void PathLengths::pinCurFrontierTableID(common::table_id_t tableID) {
    curFrontier.store(getMaskData(tableID), std::memory_order_relaxed);
    curTableID.store(tableID, std::memory_order_relaxed);
}

void PathLengths::pinNextFrontierTableID(common::table_id_t tableID) {
    nextFrontier.store(getMaskData(tableID), std::memory_order_relaxed);
}

WCCFrontier::WCCFrontier(const common::table_id_map_t<common::offset_t>& numNodesMap_,
    storage::MemoryManager* mm, bool inital_bool)
    : GDSFrontier{numNodesMap_} {
    for (const auto& [tableID, curNumNodes] : numNodesMap_) {
        numNodesMap[tableID] = curNumNodes;
        auto curActiveMemBuffer =
            mm->allocateBuffer(false, curNumNodes * sizeof(std::atomic<bool>));
        std::atomic<bool>* curActiveMemBufferPtr =
            reinterpret_cast<std::atomic<bool>*>(curActiveMemBuffer.get()->getData());
        // Cast a unique number to each node
        for (uint64_t i = 0; i < curNumNodes; ++i) {
            curActiveMemBufferPtr[i].store(inital_bool, std::memory_order_relaxed);
        }
        curActiveNodes.insert({tableID, std::move(curActiveMemBuffer)});
    }
}

void WCCFrontier::setActive(std::span<const common::nodeID_t> nodeIDs) {
    for (auto& nodeID : nodeIDs) {
        setCurActive(nodeID);
    }
}

void WCCFrontier::setCurActive(common::nodeID_t nodeID) {
    auto& memBuffer = curActiveNodes.find(nodeID.tableID)->second;
    std::atomic<bool>* memBufferPtr = reinterpret_cast<std::atomic<bool>*>(memBuffer->getData());
    memBufferPtr[nodeID.offset].store(static_cast<bool>(true), std::memory_order_relaxed);
}

bool PathLengthsInitVertexCompute::beginOnTable(common::table_id_t tableID) {
    pathLengths.pinTableID(tableID);
    return true;
}

void PathLengthsInitVertexCompute::vertexCompute(common::offset_t startOffset,
    common::offset_t endOffset, common::table_id_t) {
    auto frontier = pathLengths.getCurFrontier();
    for (auto i = startOffset; i < endOffset; ++i) {
        frontier[i].store(PathLengths::UNVISITED);
    }
}

FrontierPair::FrontierPair(std::shared_ptr<GDSFrontier> curFrontier,
    std::shared_ptr<GDSFrontier> nextFrontier, uint64_t maxThreadsForExec)
    : curDenseFrontier{curFrontier}, nextDenseFrontier{nextFrontier},
      morselDispatcher{maxThreadsForExec} {
    curSparseFrontier = std::make_shared<SparseFrontier>();
    nextSparseFrontier = std::make_shared<SparseFrontier>();
    vertexComputeCandidates = std::make_shared<SparseFrontier>();
    hasActiveNodesForNextIter_.store(false);
    curIter.store(0u);
}

void FrontierPair::beginNewIteration() {
    std::unique_lock<std::mutex> lck{mtx};
    curIter.fetch_add(1u);
    hasActiveNodesForNextIter_.store(false);
    vertexComputeCandidates->mergeSparseFrontier(*nextSparseFrontier);
    std::swap(curSparseFrontier, nextSparseFrontier);
    nextSparseFrontier->resetState();
    beginNewIterationInternalNoLock();
}

void FrontierPair::beginFrontierComputeBetweenTables(common::table_id_t curTableID,
    common::table_id_t nextTableID) {
    pinCurrFrontier(curTableID);
    pinNextFrontier(nextTableID);
    morselDispatcher.init(curTableID, curDenseFrontier->getNumNodes(curTableID));
}

bool FrontierPair::continueNextIter(uint16_t maxIter) {
    return hasActiveNodesForNextIter_.load(std::memory_order_relaxed) && getCurrentIter() < maxIter;
}

void FrontierPair::addNodeToNextDenseFrontier(common::nodeID_t nodeID) {
    nextDenseFrontier->setActive(nodeID);
}

void FrontierPair::addNodesToNextDenseFrontier(const std::vector<common::nodeID_t>& nodeIDs) {
    nextDenseFrontier->setActive(nodeIDs);
}

void FrontierPair::mergeLocalFrontier(const SparseFrontier& localFrontier) {
    std::unique_lock<std::mutex> lck{mtx};
    if (!nextSparseFrontier->enabled()) {
        return;
    }
    if (!localFrontier.enabled()) {
        nextSparseFrontier->disable();
        return;
    }
    nextSparseFrontier->mergeLocalFrontier(localFrontier);
}

bool FrontierPair::isCurFrontierSparse() {
    return curSparseFrontier->enabled();
}

void SinglePathLengthsFrontierPair::initRJFromSource(nodeID_t source) {
    pathLengths->pinNextFrontierTableID(source.tableID);
    pathLengths->setActive(source);
    nextSparseFrontier->addNode(source);
    hasActiveNodesForNextIter_.store(true);
}

void DoublePathLengthsFrontierPair::initRJFromSource(nodeID_t source) {
    nextDenseFrontier->ptrCast<PathLengths>()->pinNextFrontierTableID(source.tableID);
    nextDenseFrontier->ptrCast<PathLengths>()->setActive(source);
    nextSparseFrontier->addNode(source);
    hasActiveNodesForNextIter_.store(true);
}

void DoublePathLengthsFrontierPair::beginNewIterationInternalNoLock() {
    std::swap(curDenseFrontier, nextDenseFrontier);
    curDenseFrontier->ptrCast<PathLengths>()->incrementCurIter();
    nextDenseFrontier->ptrCast<PathLengths>()->incrementCurIter();
}

WCCFrontierPair::WCCFrontierPair(common::table_id_map_t<common::offset_t> numNodesMap,
    uint64_t totalNumNodes, uint64_t maxThreadsForExec, storage::MemoryManager* mm)
    : FrontierPair(nullptr, nullptr, maxThreadsForExec), numNodes{totalNumNodes} {
    uint64_t componentIDCounter = 0;
    updated = true;
    for (const auto& [tableID, curNumNodes] : numNodesMap) {
        numNodesMap[tableID] = curNumNodes;
        auto memBuffer = mm->allocateBuffer(false, curNumNodes * sizeof(std::atomic<uint64_t>));
        std::atomic<uint64_t>* memBufferPtr =
            reinterpret_cast<std::atomic<uint64_t>*>(memBuffer.get()->getData());
        // Cast a unique number to each node
        for (uint64_t i = 0; i < curNumNodes; ++i) {
            memBufferPtr[i].store(componentIDCounter, std::memory_order_relaxed);
            componentIDCounter++;
        }
        vertexValues.insert({tableID, std::move(memBuffer)});
    }
    curFrontier = std::make_shared<WCCFrontier>(numNodesMap, mm, true);
    nextFrontier = std::make_shared<WCCFrontier>(numNodesMap, mm, false);
    setActiveNodesForNextIter();
}

void WCCFrontierPair::beginNewIterationInternalNoLock() {
    updated = false;
};

uint64_t WCCFrontierPair::getComponentID(common::nodeID_t nodeID) const {
    return getComponentIDAtomic(nodeID).load(std::memory_order_relaxed);
}

void WCCFrontierPair::beginFrontierComputeBetweenTables(common::table_id_t curTableID,
    common::table_id_t) {
    morselDispatcher.init(curTableID, numNodes);
}

bool WCCFrontierPair::update(common::nodeID_t boundNodeID, common::nodeID_t nbrNodeID) {
    auto& boundComponentID = getComponentIDAtomic(boundNodeID);
    auto& nbrComponentID = getComponentIDAtomic(nbrNodeID);

    auto expectedComponentID = boundComponentID.load(std::memory_order_relaxed);
    auto actualComponentID = nbrComponentID.load(std::memory_order_relaxed);

    // If needs changing
    if (expectedComponentID < actualComponentID) {
        updated = true;
        nextFrontier->ptrCast<WCCFrontier>()->setActive(nbrNodeID);
        while (!nbrComponentID.compare_exchange_strong(actualComponentID, expectedComponentID,
            std::memory_order_relaxed, std::memory_order_relaxed)) {}
        return true;
    }
    return false;
}

std::atomic<uint64_t>& WCCFrontierPair::getComponentIDAtomic(common::nodeID_t nodeID) const {
    auto& memBuffer = vertexValues.find(nodeID.tableID)->second;
    std::atomic<uint64_t>* memBufferPtr =
        reinterpret_cast<std::atomic<uint64_t>*>(memBuffer->getData());
    return memBufferPtr[nodeID.offset];
}

static constexpr uint64_t EARLY_TERM_NUM_NODES_THRESHOLD = 100;

bool SPEdgeCompute::terminate(processor::NodeOffsetMaskMap& maskMap) {
    auto targetNumNodes = maskMap.getNumMaskedNode();
    if (targetNumNodes > EARLY_TERM_NUM_NODES_THRESHOLD) {
        // Skip checking if it's unlikely to early terminate.
        return false;
    }

    auto& frontier = frontierPair->getCurDenseFrontier();
    for (auto& [tableID, maxNumNodes] : frontier.getNumNodesMap()) {
        frontier.pinTableID(tableID);
        if (!maskMap.containsTableID(tableID)) {
            continue;
        }
        auto offsetMask = maskMap.getOffsetMask(tableID);
        for (auto offset = 0u; offset < maxNumNodes; ++offset) {
            if (frontier.isActive(offset)) {
                numNodesReached += offsetMask->isMasked(offset);
            }
        }
    }
    return numNodesReached == targetNumNodes;
}

} // namespace function
} // namespace kuzu
