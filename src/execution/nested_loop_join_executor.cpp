//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "catalog/column.h"
#include "common/config.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple right_tuple;
  RID right_rid;
  if (!right_executor_->Next(&right_tuple, &right_rid)) {
    // right tuples have been traversed completely
    // then we should shift to next left tuple
    if (!left_executor_->Next(&left_tuple_, &left_rid_)) {
      // left tuples have been traversed completely
      left_rid_.Set(INVALID_PAGE_ID, 0);
      return false;
    }
    right_executor_->Init();
  }

  if (right_rid.GetPageId() == INVALID_PAGE_ID) {
    // the right child doesn't produce a tuple
    rid->Set(INVALID_PAGE_ID, 0);
    return true;
  }

  if (left_rid_.GetPageId() == INVALID_PAGE_ID) {
    // the left child doesn't produce a tuple
    rid->Set(INVALID_PAGE_ID, 0);
    return true;
  }

  auto predicate = plan_->Predicate();
  if (predicate == nullptr) {
    if (plan_->OutputSchema() != nullptr) {
      std::vector<Value> values;
      for (size_t col = 0; col < plan_->OutputSchema()->GetColumnCount(); ++col) {
          values.push_back(right_tuple.GetValue(plan_->OutputSchema(), col));
      }
      *tuple = Tuple(values, plan_->OutputSchema());
      return true;
    }
    *tuple = right_tuple;
    return true;
  }

  if (predicate->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), 
      &right_tuple, right_executor_->GetOutputSchema()).GetAs<bool>()) {
    
    // LOG_DEBUG("nested_join: find a matched tuple");

    if (plan_->OutputSchema() != nullptr) {
      std::unordered_map<std::string, uint32_t> left_columns;
      std::unordered_map<std::string, uint32_t> right_columns;
      auto left_schema = left_executor_->GetOutputSchema();
      auto right_schema = right_executor_->GetOutputSchema();

      std::vector<Value> values;

      for (size_t col = 0; col < left_schema->GetColumnCount(); ++col) {
        left_columns.emplace(left_schema->GetColumn(col).GetName(), col);
      }

      for (size_t col = 0; col < right_schema->GetColumnCount(); ++col) {
        right_columns.emplace(right_schema->GetColumn(col).GetName(), col);
      }

      for (const auto& col: plan_->OutputSchema()->GetColumns()) {
        std::string col_name = col.GetName();
        if (left_columns.find(col_name) != left_columns.end()) {
          values.push_back(left_tuple_.GetValue(left_schema, left_columns[col_name]));
          continue;
        } 
        if (right_columns.find(col_name) != right_columns.end()) {
          values.push_back(right_tuple.GetValue(right_schema, right_columns[col_name]));
          continue;
        } 
        LOG_DEBUG("no matched column!");
      }

      *tuple = Tuple(values, plan_->OutputSchema());
      *rid = left_rid_;
      return true;
      
    }
    *rid = left_rid_;
    *tuple = right_tuple;
    return true;
  }

  // not satisfy the predicate
  rid->Set(INVALID_PAGE_ID, 0);
  return true;

}

}  // namespace bustub