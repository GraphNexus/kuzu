#include "processor/operator/persistent/merge.h"

#include "binder/expression/expression_util.h"

namespace kuzu {
namespace processor {

std::string MergePrintInfo::toString() const {
    std::string result = "Pattern: ";
    result += binder::ExpressionUtil::toString(pattern);
    if (!onMatch.empty()) {
        result += ", ON MATCH SET: " + binder::ExpressionUtil::toString(onMatch);
    }
    if (!onCreate.empty()) {
        result += ", ON CREATE SET: " + binder::ExpressionUtil::toString(onCreate);
    }
    return result;
}

void Merge::initLocalStateInternal(ResultSet* /*resultSet_*/, ExecutionContext* context) {
    existenceVector = resultSet->getValueVector(existenceMark).get();
    for (auto& executor : nodeInsertExecutors) {
        executor.init(resultSet, context);
    }
    for (auto& executor : relInsertExecutors) {
        executor.init(resultSet, context);
    }
    for (auto& executor : onCreateNodeSetExecutors) {
        executor->init(resultSet, context);
    }
    for (auto& executor : onCreateRelSetExecutors) {
        executor->init(resultSet, context);
    }
    for (auto& executor : onMatchNodeSetExecutors) {
        executor->init(resultSet, context);
    }
    for (auto& executor : onMatchRelSetExecutors) {
        executor->init(resultSet, context);
    }
    localState.init(*resultSet, context->clientContext, info);
}

void Merge::executeOnMatch(ExecutionContext* context) {
    for (auto& executor : onMatchNodeSetExecutors) {
        executor->set(context);
    }
    for (auto& executor : onMatchRelSetExecutors) {
        executor->set(context);
    }
}

bool Merge::hasPatternBeenCreated(PatternCreationInfo& insertedIDInfo) {
    if (!localState.keyVectors.empty()) {
        return !insertedIDInfo.isDistinct;
    } else {
        hasInserted = true;
        return false;
    }
}

void Merge::executeNoMatch(ExecutionContext* context) {
    PatternCreationInfo insertedIDInfo;
    if (!localState.keyVectors.empty()) {
        insertedIDInfo = localState.append();
    }
    if (hasPatternBeenCreated(insertedIDInfo)) {
        for (auto& executor : nodeInsertExecutors) {
            executor.skipInsert();
        }
        for (auto& executor : relInsertExecutors) {
            executor.skipInsert();
        }
        for (auto i = 0u; i < onMatchNodeSetExecutors.size(); i++) {
            auto& executor = onMatchNodeSetExecutors[i];
            auto nodeIDToSet = insertedIDInfo.getNodeID(i);
            executor->setNodeID(nodeIDToSet);
            executor->set(context);
        }
        for (auto i = 0u; i < onMatchRelSetExecutors.size(); i++) {
            auto& executor = onMatchRelSetExecutors[i];
            auto relIDToSet = insertedIDInfo.getNodeID(i + onMatchNodeSetExecutors.size());
            executor->setRelID(relIDToSet);
            executor->set(context);
        }
    } else {
        // do insert and on create
        for (auto i = 0u; i < nodeInsertExecutors.size(); i++) {
            auto& executor = nodeInsertExecutors[i];
            auto nodeID = executor.insert(context->clientContext->getTx());
            if (!localState.keyVectors.empty()) {
                insertedIDInfo.updateID(i, info.executorInfo, nodeID);
            }
        }
        for (auto i = 0u; i < relInsertExecutors.size(); i++) {
            auto& executor = relInsertExecutors[i];
            auto relID = executor.insert(context->clientContext->getTx());
            if (!localState.keyVectors.empty()) {
                insertedIDInfo.updateID(i + nodeInsertExecutors.size(), info.executorInfo, relID);
            }
        }
        for (auto& executor : onCreateNodeSetExecutors) {
            executor->set(context);
        }
        for (auto& executor : onCreateRelSetExecutors) {
            executor->set(context);
        }
    }
}

bool Merge::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    KU_ASSERT(existenceVector->state->getSelVector().getSelSize() == 1);
    auto pos = existenceVector->state->getSelVector()[0];
    auto patternExist = existenceVector->getValue<bool>(pos);
    if (patternExist) {
        executeOnMatch(context);
    } else {
        executeNoMatch(context);
    }
    return true;
}

std::unique_ptr<PhysicalOperator> Merge::clone() {
    return std::make_unique<Merge>(existenceMark, copyVector(nodeInsertExecutors),
        copyVector(relInsertExecutors), NodeSetExecutor::copy(onCreateNodeSetExecutors),
        RelSetExecutor::copy(onCreateRelSetExecutors),
        NodeSetExecutor::copy(onMatchNodeSetExecutors),
        RelSetExecutor::copy(onMatchRelSetExecutors), info.copy(), children[0]->clone(), id,
        printInfo->copy());
}

} // namespace processor
} // namespace kuzu
