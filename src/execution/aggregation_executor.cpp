//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "common/config.h"
#include "common/logger.h"
#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_({plan->GetAggregates(), plan->GetAggregateTypes()}),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  finish_traverse_ = false;
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  return false;
}

bool AggregationExecutor::Next(std::vector<Tuple>* result_set, RID *rid) {
  if (finish_traverse_) {
    // LOG_DEBUG("finish traverse");
    return false;
  }


  Tuple tmp_tuple;
  RID tmp_rid;
  if (!child_->Next(&tmp_tuple, &tmp_rid)) {
    // We have got all the tuples from child
    AggregateAllTuples(result_set);
    // LOG_DEBUG("set flag = true");
    finish_traverse_ = true;
    // here we just randomly give a page id
    rid->Set(0, 0);
    return true;
  }
  
  if (tmp_rid.GetPageId() == INVALID_PAGE_ID) {
    rid->Set(INVALID_PAGE_ID, 0);
    return true;
  }

  auto key = MakeAggregateKey(&tmp_tuple);
  auto value = MakeAggregateValue(&tmp_tuple);
  aht_.InsertCombine(key, value);
  // here we just randomly give a page id
  rid->Set(0, 0);
  return true;

}

void AggregationExecutor::AggregateAllTuples(std::vector<Tuple>* result_set) {
  if (result_set == nullptr) {
    return;
  }

//   LOG_DEBUG("aggregate: all");

  auto having = plan_->GetHaving();
  aht_iterator_ = aht_.Begin();

  while (aht_iterator_ != aht_.End()) {
    auto group_bys = aht_iterator_.Key().group_bys_;
    auto aggregate_vals = aht_iterator_.Val().aggregates_;
    if (having != nullptr && !having->EvaluateAggregate(group_bys, aggregate_vals).GetAs<bool>()) {
      // the tuple doesn't satisfy the having condition
      ++aht_iterator_;
      continue;
    }
    std::vector<Value> values;
    for (auto& col: plan_->OutputSchema()->GetColumns()) {
      values.push_back(col.GetExpr()->EvaluateAggregate(group_bys, aggregate_vals));
    }
    result_set->push_back(Tuple(values, plan_->OutputSchema()));

    ++aht_iterator_;
  }
}


const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
