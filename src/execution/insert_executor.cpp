//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "common/config.h"
#include "execution/executor_factory.h"
#include "execution/executors/abstract_executor.h"
#include "execution/executors/insert_executor.h"
#include "storage/index/extendible_hash_table_index.h"
#include "storage/table/tuple.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan) {}

void InsertExecutor::Init() {
    Catalog* catalog = exec_ctx_->GetCatalog();
    TableInfo* table_info = catalog->GetTable(plan_->TableOid());
    // fetch the raw pointer
    table_heap_ = table_info->table_.get();

    if (plan_->IsRawInsert()) {
        // raw insert
        // then we don't need the 'next' process
        auto raw_values = plan_->RawValues();
        auto table_indexes = catalog->GetTableIndexes(table_info->name_);

        for (auto& values: raw_values) {
            Tuple tuple = Tuple(values, &table_info->schema_);
            RID rid;
            InsertTuple(tuple, rid, table_indexes);
        }
    } else {
        // values are from child node
        auto child_plan = plan_->GetChildPlan();
        child_executor_ = ExecutorFactory::CreateExecutor(exec_ctx_, child_plan);
        LOG_DEBUG("insert: init child");
        // init the child node
        child_executor_->Init(); 
    }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {

    if (plan_->IsRawInsert()) {
        // raw insert
        // then we don't need the 'next' process
        return false;
    }

    Tuple old_tuple;
    bool res = child_executor_->Next(&old_tuple, rid);

    if (!res) {
        return res;
    }

    if (rid->GetPageId() == INVALID_PAGE_ID) {
        // the tuple doesn't satisfy the predicate 
        return true;
    }


    // LOG_DEBUG("insert: get a tuple from child");
    Catalog* catalog = exec_ctx_->GetCatalog();
    TableInfo* table_info = catalog->GetTable(plan_->TableOid());
    auto table_indexes = catalog->GetTableIndexes(table_info->name_);
    InsertTuple(old_tuple, *rid, table_indexes);

    rid->Set(INVALID_PAGE_ID, 0);
    return true;
}

void InsertExecutor::InsertTuple(Tuple& tuple, RID& rid, std::vector<IndexInfo*>& table_indexes) {
    // insert the tuple into the table
    table_heap_->InsertTuple(tuple, &rid, exec_ctx_->GetTransaction());
    // insert the tuple into the all indexes
    for (auto table_index: table_indexes) {
        auto index = table_index->index_.get();
        index->InsertEntry(tuple, rid, exec_ctx_->GetTransaction());
    }

}
}  // namespace bustub
