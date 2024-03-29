//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "common/config.h"
#include "common/exception.h"
#include "concurrency/transaction.h"
#include "execution/executor_factory.h"
#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  Catalog *catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->TableOid());
}

void UpdateExecutor::Init() {
  // values are from child node
  // auto child_plan = plan_->GetChildPlan();
  // child_executor_ = ExecutorFactory::CreateExecutor(exec_ctx_, child_plan);
  // LOG_DEBUG("update: init child");
  // init the child node
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple old_tuple;
  bool res = child_executor_->Next(&old_tuple, rid);

  if (!res) {
    return res;
  }

  if (rid->GetPageId() == INVALID_PAGE_ID) {
    // the tuple doesn't satisfy the predicate
    return true;
  }

  if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    // repeatable_read must hold the shared lock now, so we should upgrade the lock
    try {
      LOG_DEBUG("update: start to upgrade lock");
      exec_ctx_->GetLockManager()->LockUpgrade(exec_ctx_->GetTransaction(), *rid);
      LOG_DEBUG("update: upgrade lock");
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

  // LOG_DEBUG("insert: get a tuple from child");
  Catalog *catalog = exec_ctx_->GetCatalog();
  auto table_indexes = catalog->GetTableIndexes(table_info_->name_);

  Tuple updated_tuple = GenerateUpdatedTuple(old_tuple);


  // update the tuple
  auto table_heap = table_info_->table_.get();
  table_heap->UpdateTuple(updated_tuple, *rid, exec_ctx_->GetTransaction());

  Transaction *txn = exec_ctx_->GetTransaction();
  // // save the write tuples into the txn
  // txn->AppendTableWriteRecord(TableWriteRecord(*rid, WType::UPDATE, old_tuple, table_heap));

  // update the index
  for (auto table_index : table_indexes) {
    auto index = table_index->index_.get();
    Tuple old_key = old_tuple.KeyFromTuple(table_info_->schema_, *table_index->index_->GetKeySchema(),
                                              table_index->index_->GetKeyAttrs());
    Tuple new_key = updated_tuple.KeyFromTuple(table_info_->schema_, *table_index->index_->GetKeySchema(),
                                              table_index->index_->GetKeyAttrs());

    // save the write tuples into each index
    // TODO(greenhandzpx): 
    // not sure whether the original tuple or the key tuple should be passed to this function
    auto index_write_record = IndexWriteRecord(*rid, plan_->TableOid(), WType::UPDATE, updated_tuple,
                                table_index->index_oid_, exec_ctx_->GetCatalog());
    index_write_record.old_tuple_ = old_tuple;
    txn->AppendTableWriteRecord(index_write_record);

    // first delete
    index->DeleteEntry(old_key, *rid, exec_ctx_->GetTransaction());
    // then insert
    index->InsertEntry(new_key, *rid, exec_ctx_->GetTransaction());
  }

  rid->Set(INVALID_PAGE_ID, 0);
  return true;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
