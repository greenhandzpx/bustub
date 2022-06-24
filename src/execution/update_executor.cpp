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
#include "execution/executor_factory.h"
#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)) {

  Catalog* catalog = exec_ctx_->GetCatalog();
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


  // LOG_DEBUG("insert: get a tuple from child");
  Catalog* catalog = exec_ctx_->GetCatalog();
  auto table_indexes = catalog->GetTableIndexes(table_info_->name_);

  Tuple updated_tuple = GenerateUpdatedTuple(old_tuple);
  // update the tuple
  auto table_heap = table_info_->table_.get();
  table_heap->UpdateTuple(updated_tuple, *rid, exec_ctx_->GetTransaction());
  // update the index
  for (auto table_index: table_indexes) {
    auto index = table_index->index_.get();
    // first delete
    index->DeleteEntry(old_tuple, *rid, exec_ctx_->GetTransaction());
    // then insert
    index->InsertEntry(updated_tuple, *rid, exec_ctx_->GetTransaction());
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
