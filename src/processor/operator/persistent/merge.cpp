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

bool Merge::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    if (!localState.keyVectors.empty()) {
        localState.append(resultSet->multiplicity);
    }
    KU_ASSERT(existenceVector->state->getSelVector().getSelSize() == 1);
    auto pos = existenceVector->state->getSelVector()[0];
    auto patternExist = existenceVector->getValue<bool>(pos);
    if (patternExist) {
        for (auto& executor : onMatchNodeSetExecutors) {
            executor->set(context);
        }
        for (auto& executor : onMatchRelSetExecutors) {
            executor->set(context);
        }
    } else {
        auto patternHasBeenCreated = false;
        auto ft = localState.hashTable->getFactorizedTable();
        if (!localState.keyVectors.empty()) {
            patternHasBeenCreated = !*(bool*)(ft->getTuple(ft->getNumTuples() - 1) +
                                              ft->getTableSchema()->getColOffset(
                                                  ft->getTableSchema()->getNumColumns() - 2));
        } else {
            patternHasBeenCreated = hasInserted;
            hasInserted = true;
        }
        if (patternHasBeenCreated) {
            for (auto& executor : nodeInsertExecutors) {
                executor.skipInsert();
            }
            for (auto& executor : relInsertExecutors) {
                executor.skipInsert();
            }
            for (auto& executor : onMatchNodeSetExecutors) {
                executor->set(context);
            }
            for (auto& executor : onMatchRelSetExecutors) {
                executor->set(context);
            }
        } else {
            // do insert and on create
            for (auto& executor : nodeInsertExecutors) {
                executor.insert(context->clientContext->getTx());
            }
            for (auto& executor : relInsertExecutors) {
                executor.insert(context->clientContext->getTx());
            }
            for (auto& executor : onCreateNodeSetExecutors) {
                executor->set(context);
            }
            for (auto& executor : onCreateRelSetExecutors) {
                executor->set(context);
            }
        }
    }
    return true;
}

std::unique_ptr<PhysicalOperator> Merge::clone() {
    return std::make_unique<Merge>(existenceMark, copyVector(nodeInsertExecutors),
        copyVector(relInsertExecutors), NodeSetExecutor::copy(onCreateNodeSetExecutors),
        RelSetExecutor::copy(onCreateRelSetExecutors),
        NodeSetExecutor::copy(onMatchNodeSetExecutors),
        RelSetExecutor::copy(onMatchRelSetExecutors), info, children[0]->clone(), id,
        printInfo->copy());
}

} // namespace processor
} // namespace kuzu
