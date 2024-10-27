#pragma once

#include "math.h"

#include "binder/expression/expression_util.h"
#include "binder/expression/node_expression.h"
#include "function/gds/gds.h"
#include "function/gds/gds_frontier.h"
#include "function/gds_function.h"

namespace kuzu {
namespace fts_extension {

struct FTSBindData final : public function::GDSBindData {
    std::shared_ptr<binder::Expression> terms;
    // k: parameter controls the influence of term frequency saturation. It limits the effect of
    // additional occurrences of a term within a document.
    double_t k;
    // b: parameter controls the degree of length normalization by adjusting the influence of
    // document length.
    double_t b;
    uint64_t numDocs;
    double_t avgDocLen;

    FTSBindData(std::shared_ptr<binder::Expression> terms, std::shared_ptr<binder::Expression> docs,
        double_t k, double_t b, uint64_t numDocs, double_t avgDocLen)
        : GDSBindData{std::move(docs)}, terms{std::move(terms)}, k{k}, b{b}, numDocs{numDocs},
          avgDocLen{avgDocLen} {}
    FTSBindData(const FTSBindData& other)
        : GDSBindData{other}, terms{other.terms}, k{other.k}, b{other.b}, numDocs{other.numDocs},
          avgDocLen{other.avgDocLen} {}

    bool hasNodeInput() const override { return true; }
    std::shared_ptr<binder::Expression> getNodeInput() const override { return terms; }

    std::unique_ptr<GDSBindData> copy() const override {
        return std::make_unique<FTSBindData>(*this);
    }
};

struct FTSState {
    std::unique_ptr<function::FrontierPair> frontierPair;
    std::unique_ptr<function::EdgeCompute> edgeCompute;

    FTSState(std::unique_ptr<function::FrontierPair> frontierPair,
        std::unique_ptr<function::EdgeCompute> edgeCompute, common::table_id_t termTableID);
    void setNodeActive(common::nodeID_t sourceNodeID) const;
};

class FTSAlgorithm : public function::GDSAlgorithm {
public:
    static constexpr char SCORE_COLUMN_NAME[] = "score";
    static constexpr char TERM_FREQUENCY_PROP_NAME[] = "tf";

public:
    FTSAlgorithm() = default;
    FTSAlgorithm(const FTSAlgorithm& other) : GDSAlgorithm{other} {}

    /*
     * Inputs include the following:
     *
     * graph::ANY
     * srcNode::NODE
     * queryString: STRING
     */
    std::vector<common::LogicalTypeID> getParameterTypeIDs() const override {
        return {common::LogicalTypeID::ANY, common::LogicalTypeID::NODE,
            common::LogicalTypeID::DOUBLE, common::LogicalTypeID::DOUBLE,
            common::LogicalTypeID::UINT64, common::LogicalTypeID::DOUBLE};
    }

    void exec(processor::ExecutionContext* executionContext) override;

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<FTSAlgorithm>(*this);
    }

    binder::expression_vector getResultColumns(binder::Binder* binder) const override;

    void bind(const binder::expression_vector& params, binder::Binder* binder,
        graph::GraphEntry& graphEntry) override;
};

struct FTSFunction {
    static constexpr const char* name = "FTS";

    static function::function_set getFunctionSet();
};

} // namespace fts_extension
} // namespace kuzu
