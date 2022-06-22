//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <cstdint>
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/logger.h"
#include "execution/expressions/abstract_expression.h"
#include "storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
 : AbstractExecutor(exec_ctx), 
   plan_(plan) {
}

void SeqScanExecutor::Init() {
    Catalog* catalog = exec_ctx_->GetCatalog();
    TableInfo* table_info = catalog->GetTable(plan_->GetTableOid());
    // fetch the raw pointer
    table_heap_ = table_info->table_.get();
    // table_heap_ = std::move(table_info->table_);
    // auto table_iterator = table_heap_->Begin(exec_ctx_->GetTransaction());
    idx_ = 0;
    auto table_indexes = catalog->GetTableIndexes(table_info->name_);
    for (auto table_index: table_indexes) {
        LOG_DEBUG("index size:%ld", table_index->key_size_);
    }

}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {

    // I think my solution sucks... There must be better solution.
    
    auto table_iterator = table_heap_->Begin(exec_ctx_->GetTransaction());
    uint32_t cnt = 0;
    while (cnt < idx_) {
        ++table_iterator;
        ++cnt;
    }
    ++idx_;
    if (table_iterator == table_heap_->End()) {
        return false;
    }

    // LOG_DEBUG("seq_scan: next");
    Tuple test_tuple = *table_iterator;
    // *tuple = *table_iterator;
    *rid = test_tuple.GetRid();

    // Catalog* catalog = exec_ctx_->GetCatalog();
    // auto table_info = catalog->GetTable(plan_->GetTableOid());

    std::vector<Value> values;
    for (size_t col = 0; col < plan_->OutputSchema()->GetColumnCount(); ++col) {
        values.push_back(test_tuple.GetValue(plan_->OutputSchema(), col));
    }
    *tuple = Tuple(values, plan_->OutputSchema());

    const AbstractExpression* expression = plan_->GetPredicate();
    // return true;

    if (expression == nullptr) {
        return true;
    }




    // if (expression->Evaluate(&test_tuple, &table_info->schema_).GetAs<bool>()) {
    if (expression->Evaluate(&test_tuple, plan_->OutputSchema()).GetAs<bool>()) {
        // LOG_DEBUG("tuple:%s", tuple->ToString(plan_->OutputSchema()).c_str());
        // LOG_DEBUG("rid:%s", rid->ToString().c_str());
        return true;
    }

    rid->Set(INVALID_PAGE_ID, 0);
    // LOG_DEBUG("seq_scan: predicate doesn't satisfy.");
    return true;
}

}  // namespace bustub
