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
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)) {

  Catalog* catalog = exec_ctx_->GetCatalog();
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

  // LOG_DEBUG("tuple:%s", old_tuple.ToString(&table_info_->schema_).c_str());
  // LOG_DEBUG("rid:%s", rid->ToString().c_str());

  // LOG_DEBUG("insert: get a tuple from child");
  Catalog* catalog = exec_ctx_->GetCatalog();
  auto table_indexes = catalog->GetTableIndexes(table_info_->name_);


  // delete the tuple from the table
  auto table_heap = table_info_->table_.get();
  assert(table_heap->MarkDelete(*rid, exec_ctx_->GetTransaction()));

  // delete from all the indexes
  for (auto table_index: table_indexes) {
    // LOG_DEBUG("delete from index1");
    // LOG_DEBUG("index size:%ld", table_index->key_size_);
    auto index = table_index->index_.get();
    assert(old_tuple.IsAllocated());
    index->DeleteEntry(old_tuple.KeyFromTuple(table_info_->schema_, *table_index->index_->GetKeySchema(), table_index->index_->GetKeyAttrs()), *rid, exec_ctx_->GetTransaction());
    // LOG_DEBUG("delete from index2");
  }


  rid->Set(INVALID_PAGE_ID, 0);
  return true;
     
}

  
}  // namespace bustub
