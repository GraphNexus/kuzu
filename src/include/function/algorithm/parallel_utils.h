#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>

#include "common/task_system/task_scheduler.h"
#include "function/table_functions.h"
#include "ife_morsel.h"
#include "processor/operator/algorithm/algorithm_runner_worker.h"
#include "processor/operator/call/in_query_call.h"
#include "processor/operator/sink.h"

using namespace kuzu::processor;

namespace kuzu {
namespace graph {

class ParallelUtils {
public:
    explicit ParallelUtils(InQueryCallInfo info,
        std::shared_ptr<InQueryCallSharedState> sharedState,
        std::shared_ptr<AlgorithmRunnerSharedState> algorithmRunnerSharedState,
        std::unique_ptr<ResultSetDescriptor> resultSetDescriptor, uint32_t operatorID,
        std::string expressions);

    inline function::TableFuncSharedState* getFuncSharedState() {
        return algorithmRunnerWorker->getInQuerySharedState();
    }

    void doParallel(ExecutionContext* executionContext, function::table_func_t tableFunc);

private:
    std::unique_ptr<AlgorithmRunnerWorker> algorithmRunnerWorker;
};

} // namespace graph
} // namespace kuzu
