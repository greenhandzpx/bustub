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
    auto table_iterator = table_heap_->Begin(exec_ctx_->GetTransaction());
    idx_ = 0;
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

    *tuple = *table_iterator;
    *rid = tuple->GetRid();

    const AbstractExpression* expression = plan_->GetPredicate();
    if (expression == nullptr) {
        return true;
    }
    return expression->Evaluate(tuple, plan_->OutputSchema()).GetAs<bool>();
}

}  // namespace bustub
