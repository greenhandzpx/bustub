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
#include "concurrency/transaction.h"
#include "execution/executor_factory.h"
#include "execution/executors/abstract_executor.h"
#include "execution/executors/insert_executor.h"
#include "storage/index/extendible_hash_table_index.h"
#include "storage/table/tuple.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void InsertExecutor::Init() {
  Catalog *catalog = exec_ctx_->GetCatalog();
  TableInfo *table_info = catalog->GetTable(plan_->TableOid());
  // fetch the raw pointer
  table_heap_ = table_info->table_.get();

  if (plan_->IsRawInsert()) {
    // raw insert
    // then we don't need the 'next' process
    auto raw_values = plan_->RawValues();
    auto table_indexes = catalog->GetTableIndexes(table_info->name_);

    for (auto &values : raw_values) {
      Tuple tuple = Tuple(values, &table_info->schema_);
      RID rid;
      InsertTuple(&tuple, &rid, &table_indexes);
    }
  } else {
    // values are from child node
    auto child_plan = plan_->GetChildPlan();
    child_executor_ = ExecutorFactory::CreateExecutor(exec_ctx_, child_plan);
    // LOG_DEBUG("insert: init child");
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

  // if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
  //   // repeatable_read must hold the shared lock now, so we should upgrade the lock
  //   try {
  //     exec_ctx_->GetLockManager()->LockUpgrade(exec_ctx_->GetTransaction(), *rid);
  //   } catch (Exception &e) {
  //     e.what();
  //     return false;
  //   }
  // } else {
  //   // the other two levels don't get the shared lock, so we can just acquire the exclusive lock
  //   try {
  //     exec_ctx_->GetLockManager()->LockExclusive(exec_ctx_->GetTransaction(), *rid);
  //   } catch (Exception &e) {
  //     e.what();
  //     return false;
  //   }

  // }

  // LOG_DEBUG("insert: get a tuple from child");
  Catalog *catalog = exec_ctx_->GetCatalog();
  TableInfo *table_info = catalog->GetTable(plan_->TableOid());
  auto table_indexes = catalog->GetTableIndexes(table_info->name_);
  InsertTuple(&old_tuple, rid, &table_indexes);

  rid->Set(INVALID_PAGE_ID, 0);
  return true;
}

void InsertExecutor::InsertTuple(Tuple *tuple, RID *rid, std::vector<IndexInfo *> *table_indexes) {
  Transaction *txn = exec_ctx_->GetTransaction();

  // insert the tuple into the table
  table_heap_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());

  // acquire the lock(because when we roll back, we will release the lock)
  exec_ctx_->GetLockManager()->LockExclusive(exec_ctx_->GetTransaction(), *rid);
  // LOG_DEBUG("rid: %s", rid->ToString().c_str());
  // // save the write tuples into the txn
  // txn->AppendTableWriteRecord(TableWriteRecord(*rid, WType::INSERT, *tuple, table_heap_));

  Catalog *catalog = exec_ctx_->GetCatalog();
  auto table_info = catalog->GetTable(plan_->TableOid());
  // insert the tuple into the all indexes
  for (auto table_index : *table_indexes) {
    auto index = table_index->index_.get();
    Tuple key = tuple->KeyFromTuple(table_info->schema_, *table_index->index_->GetKeySchema(),
                                              table_index->index_->GetKeyAttrs());
    // save the write tuples into each index
    // TODO(greenhandzpx): 
    // not sure whether the original tuple or the key tuple should be passed to this function
    txn->AppendTableWriteRecord(IndexWriteRecord(*rid, plan_->TableOid(), WType::INSERT, *tuple, 
                                table_index->index_oid_, exec_ctx_->GetCatalog()));
    index->InsertEntry(key, *rid, exec_ctx_->GetTransaction());
  }
}
}  // namespace bustub
