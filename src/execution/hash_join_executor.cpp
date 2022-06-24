//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "catalog/column.h"
#include "common/config.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "type/type.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
  join_hash_table_.clear();
  // construct the outter hash table 
  // first we should get all the outter table's tuples
  left_executor_->Init();
  right_executor_->Init();
  const AbstractExpression* expr = plan_->LeftJoinKeyExpression();
  Tuple tuple;
  RID rid;
  HashJoinKey key;

  while (left_executor_->Next(&tuple, &rid)) {
    if (rid.GetPageId() == INVALID_PAGE_ID) {
      continue;
    }
    key.join_keys_ = {expr->Evaluate(&tuple, left_executor_->GetOutputSchema())};
    if (join_hash_table_.find(key) == join_hash_table_.end()) {
      join_hash_table_.insert({key, {tuple}});
    } else {
      join_hash_table_[key].push_back(tuple);
    }
  }

}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  // get one right tuple and its join key
  Tuple right_tuple;
  RID right_rid;
  if (!right_executor_->Next(&right_tuple, &right_rid)) {
    return false;
  }
  if (right_rid.GetPageId() == INVALID_PAGE_ID) {
    rid->Set(INVALID_PAGE_ID, 0);
    return true;
  }

  auto expr = plan_->RightJoinKeyExpression();
  HashJoinKey key;
  key.join_keys_ = {expr->Evaluate(&right_tuple, right_executor_->GetOutputSchema())};
  if (join_hash_table_.find(key) == join_hash_table_.end()) {
    // no same hash key in the hash table
    rid->Set(INVALID_PAGE_ID, 0);
    return true;
  }
  // check whether this right tuple's join key matches the tuples in the hash bucket
  if (!CheckBucket(tuple, right_tuple, join_hash_table_[key])) {
    // no matched tuple in the same hash bucket
    rid->Set(INVALID_PAGE_ID, 0);
    return true;
  }
  *rid = right_rid;
  return true;
}

bool HashJoinExecutor::CheckBucket(Tuple* output_tuple, const Tuple& right_tuple, const std::vector<Tuple>& buckets) {
  auto right_expr = plan_->RightJoinKeyExpression();
  auto left_expr = plan_->LeftJoinKeyExpression();
  auto right_key = right_expr->Evaluate(&right_tuple, right_executor_->GetOutputSchema());

  for (auto& left_tuple: buckets) {
    auto left_key = left_expr->Evaluate(&left_tuple, left_executor_->GetOutputSchema());
    if (left_key.CompareEquals(right_key) == CmpBool::CmpTrue) {
      GetOutputTuple(left_tuple, output_tuple, right_tuple);
      return true;
    }     
  }
  return false;
}

void HashJoinExecutor::GetOutputTuple(const Tuple& left_tuple, Tuple* output_tuple, const Tuple& right_tuple) {

  auto left_schema = left_executor_->GetOutputSchema();
  auto right_schema = right_executor_->GetOutputSchema();

  std::vector<Value> values;

  for (auto& col: plan_->OutputSchema()->GetColumns()) {
    auto col_expr = dynamic_cast<const ColumnValueExpression*>(col.GetExpr());
    if (col_expr->GetTupleIdx() == 0) {
      // left tuple
      values.push_back(col_expr->Evaluate(&left_tuple, left_schema));
    } else if (col_expr->GetTupleIdx() == 1) {
      // right tuple
      values.push_back(col_expr->Evaluate(&right_tuple, right_schema));
    } else {
      LOG_DEBUG("no matched column!");
    }
  }

  *output_tuple = Tuple(values, plan_->OutputSchema());
}
}  // namespace bustub
