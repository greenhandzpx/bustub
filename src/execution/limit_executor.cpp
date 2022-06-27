//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"
#include "common/config.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  child_executor_->Init();
  cnt_ = 0;
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  if (cnt_ == plan_->GetLimit()) {
    return false;
  }
  Tuple tmp_tuple;
  RID tmp_rid;
  if (!child_executor_->Next(&tmp_tuple, &tmp_rid)) {
    return false;
  }

  if (tmp_rid.GetPageId() == INVALID_PAGE_ID) {
    rid->Set(INVALID_PAGE_ID, 0);
    return true;
  }

  ++cnt_;
  *tuple = tmp_tuple;
  *rid = tmp_rid;
  return true;
}
}  // namespace bustub
