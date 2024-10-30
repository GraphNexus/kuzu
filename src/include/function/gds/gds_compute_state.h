#pragma once
#include "function/gds/gds_frontier.h"

namespace kuzu {
namespace function {

struct GDSComputeState {
    std::unique_ptr<function::FrontierPair> frontierPair;
    std::unique_ptr<function::EdgeCompute> edgeCompute;

    GDSComputeState(std::unique_ptr<function::FrontierPair> frontierPair,
        std::unique_ptr<function::EdgeCompute> edgeCompute)
        : frontierPair{std::move(frontierPair)}, edgeCompute{std::move(edgeCompute)} {}
};

} // namespace function
} // namespace kuzu
