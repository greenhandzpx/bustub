//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"
#include "common/config.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DistinctExecutor::Init() {
  distinct_hash_table_.clear();
  child_executor_->Init();
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple tmp_tuple;
  RID tmp_rid;
  if (!child_executor_->Next(&tmp_tuple, &tmp_rid)) {
    return false;
  }

  if (tmp_rid.GetPageId() == INVALID_PAGE_ID) {
    rid->Set(INVALID_PAGE_ID, 0);
    return true;
  }

  //   LOG_DEBUG("key: %s", tmp_tuple.ToString(child_executor_->GetOutputSchema()).c_str());

  DistinctKey key;
  for (size_t col = 0; col < plan_->OutputSchema()->GetColumnCount(); ++col) {
    key.distinct_keys_.push_back(tmp_tuple.GetValue(child_executor_->GetOutputSchema(), col));
  }
  //   for (auto& col: plan_->OutputSchema()->GetColumns()) {
  //     auto col_expr = col.GetExpr();

  //     key.distinct_keys_.push_back(col_expr->Evaluate(&tmp_tuple, child_executor_->GetOutputSchema()));
  //   }
  if (distinct_hash_table_.find(key) == distinct_hash_table_.end()) {
    // There doesn't exist this key in the hash table
    distinct_hash_table_.emplace(key);
    *tuple = Tuple(key.distinct_keys_, plan_->OutputSchema());
    LOG_DEBUG("key: %s", tuple->ToString(plan_->OutputSchema()).c_str());
    *rid = tmp_rid;
  } else {
    rid->Set(INVALID_PAGE_ID, 0);
  }
  return true;
}

}  // namespace bustub
