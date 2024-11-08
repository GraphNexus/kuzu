#pragma once

#include <optional>

#include "common/api.h"
#include "common/enums/extend_direction.h"
#include "common/types/types.h"

namespace kuzu {
namespace processor {
struct ExecutionContext;
}

namespace graph {
class Graph;
}

namespace function {
struct FrontierTaskSharedState;
struct RJCompState;
class VertexCompute;
struct VertexComputeTaskSharedState;
struct VertexComputeTaskInfo;
struct GDSComputeState;

class KUZU_API GDSUtils {
public:
    GDSUtils() = default;
    static void scheduleFrontierTask(common::table_id_t relTableID, graph::Graph* graph,
        common::ExtendDirection extendDirection, const function::GDSComputeState& gdsComputeState,
        processor::ExecutionContext* context, uint64_t maxThreads,
        std::optional<common::idx_t> edgePropertyIndex = std::nullopt);
    static void runFrontiersUntilConvergence(processor::ExecutionContext* executionContext,
        RJCompState& rjCompState, graph::Graph* graph, common::ExtendDirection extendDirection,
        uint64_t maxIters);
    static void runVertexComputeOnTable(common::table_id_t tableID, graph::Graph* graph,
        std::shared_ptr<VertexComputeTaskSharedState> sharedState,
        const VertexComputeTaskInfo& info, processor::ExecutionContext& context);
    static void runVertexComputeIteration(processor::ExecutionContext* executionContext,
        graph::Graph* graph, VertexCompute& vc,
        std::optional<std::vector<std::string>> propertiesToScan = std::nullopt);
};

} // namespace function
} // namespace kuzu
