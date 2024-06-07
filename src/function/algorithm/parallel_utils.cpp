#include "function/algorithm/parallel_utils.h"

#include "processor/processor_task.h"
#include "processor/result/factorized_table.h"

using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace graph {

ParallelUtils::ParallelUtils(InQueryCallInfo info,
    std::shared_ptr<InQueryCallSharedState> sharedState,
    std::shared_ptr<AlgorithmRunnerSharedState> algorithmRunnerSharedState,
    std::unique_ptr<ResultSetDescriptor> resultSetDescriptor, uint32_t operatorID,
    std::string expressions) {
    algorithmRunnerWorker = std::make_unique<AlgorithmRunnerWorker>(std::move(info), sharedState,
        algorithmRunnerSharedState, std::move(resultSetDescriptor), operatorID, expressions);
}

void ParallelUtils::doParallel(ExecutionContext* executionContext, table_func_t tableFunc) {
    auto taskScheduler = executionContext->clientContext->getTaskScheduler();
    algorithmRunnerWorker->setFuncToExecute(tableFunc);
    auto parallelUtilsTask =
        std::make_shared<ProcessorTask>(algorithmRunnerWorker.get(), executionContext);
    parallelUtilsTask->setSharedStateInitialized();
    taskScheduler->scheduleTaskAndWaitOrError(parallelUtilsTask, executionContext);
}

} // namespace graph
} // namespace kuzu
