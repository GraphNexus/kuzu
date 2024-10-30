#pragma once

#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "common/types/value/value.h"
#include "main/client_context.h"

namespace kuzu {
namespace fts_extension {

struct FTSBindData : public function::CallTableFuncBindData {
    std::string tableName;
    common::table_id_t tableID;
    std::string indexName;

    FTSBindData(std::string tableName, common::table_id_t tableID, std::string indexName,
        std::vector<common::LogicalType> returnTypes = {},
        std::vector<std::string> returnColumnNames = {})
        : function::CallTableFuncBindData{std::move(returnTypes), std::move(returnColumnNames),
              1 /* maxOffset */},
          tableName{std::move(tableName)}, tableID{tableID}, indexName{std::move(indexName)} {}

    std::string getTablePrefix() const { return common::stringFormat("{}_{}", tableID, indexName); }

    std::string getStopWordsTableName() const {
        return common::stringFormat("{}_stopwords", getTablePrefix());
    }

    std::string getDocTableName() const {
        return common::stringFormat("{}_docs", getTablePrefix());
    }

    std::string getTermsInDocInfoTableName() const {
        return common::stringFormat("{}_terms_in_doc", getTablePrefix());
    }

    std::string getDictTableName() const {
        return common::stringFormat("{}_dict", getTablePrefix());
    }

    std::string getTermsTableName() const {
        return common::stringFormat("{}_terms", getTablePrefix());
    }

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<FTSBindData>(*this);
    }
};

} // namespace fts_extension
} // namespace kuzu
