//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  Catalog *catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->TableOid());
}
void DeleteExecutor::Init() {
  // LOG_DEBUG("update: init child");
  // init the child node
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple old_tuple;

  bool res = child_executor_->Next(&old_tuple, rid);

  if (!res) {
    return false;
  }

  if (rid->GetPageId() == INVALID_PAGE_ID) {
    // the tuple doesn't satisfy the predicate
    // LOG_DEBUG("delete: no child");
    return true;
  }

  if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    // repeatable_read must hold the shared lock now, so we should upgrade the lock
    try {
      exec_ctx_->GetLockManager()->LockUpgrade(exec_ctx_->GetTransaction(), *rid);
    } catch (Exception &e) {
      e.what();
      return false;
    }
  } else {
    // the other two levels don't get the shared lock, so we can just acquire the exclusive lock
    try {
      exec_ctx_->GetLockManager()->LockExclusive(exec_ctx_->GetTransaction(), *rid);
    } catch (Exception &e) {
      e.what();
      return false;
    }

  }

  // LOG_DEBUG("tuple:%s", old_tuple.ToString(&table_info_->schema_).c_str());
  // LOG_DEBUG("rid:%s", rid->ToString().c_str());

  // LOG_DEBUG("insert: get a tuple from child");
  Catalog *catalog = exec_ctx_->GetCatalog();
  auto table_indexes = catalog->GetTableIndexes(table_info_->name_);

  auto table_heap = table_info_->table_.get();

  Transaction *txn = exec_ctx_->GetTransaction();
  // // save the write tuples into the txn
  // txn->AppendTableWriteRecord(TableWriteRecord(*rid, WType::DELETE, old_tuple, table_heap));

  // delete the tuple from the table
  assert(table_heap->MarkDelete(*rid, exec_ctx_->GetTransaction()));

  // delete from all the indexes
  for (auto table_index : table_indexes) {
    // LOG_DEBUG("delete from index1");
    // LOG_DEBUG("index size:%ld", table_index->key_size_);
    auto index = table_index->index_.get();

    assert(old_tuple.IsAllocated());
    auto key = old_tuple.KeyFromTuple(table_info_->schema_, *table_index->index_->GetKeySchema(),
                                              table_index->index_->GetKeyAttrs());
    index->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
    // LOG_DEBUG("delete from index2");
    // save the write tuples into each index
    // TODO(greenhandzpx): 
    // not sure whether the original tuple or the key tuple should be passed to this function
    auto index_write_record = IndexWriteRecord(*rid, plan_->TableOid(), WType::DELETE, old_tuple,
                                table_index->index_oid_, exec_ctx_->GetCatalog());
    txn->AppendTableWriteRecord(index_write_record);
  }

  rid->Set(INVALID_PAGE_ID, 0);
  return true;
}

}  // namespace bustub
