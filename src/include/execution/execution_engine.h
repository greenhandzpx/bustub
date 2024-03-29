//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// execution_engine.h
//
// Identification: src/include/execution/execution_engine.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "common/config.h"
#include "concurrency/transaction_manager.h"
#include "execution/executor_context.h"
#include "execution/executor_factory.h"
#include "execution/executors/aggregation_executor.h"
#include "execution/executors/hash_join_executor.h"
#include "execution/plans/abstract_plan.h"
#include "storage/table/tuple.h"
namespace bustub {

/**
 * The ExecutionEngine class executes query plans.
 */
class ExecutionEngine {
 public:
  /**
   * Construct a new ExecutionEngine instance.
   * @param bpm The buffer pool manager used by the execution engine
   * @param txn_mgr The transaction manager used by the execution engine
   * @param catalog The catalog used by the execution engine
   */
  ExecutionEngine(BufferPoolManager *bpm, TransactionManager *txn_mgr, Catalog *catalog)
      : bpm_{bpm}, txn_mgr_{txn_mgr}, catalog_{catalog} {}

  DISALLOW_COPY_AND_MOVE(ExecutionEngine);

  /**
   * Execute a query plan.
   * @param plan The query plan to execute
   * @param result_set The set of tuples produced by executing the plan
   * @param txn The transaction context in which the query executes
   * @param exec_ctx The executor context in which the query executes
   * @return `true` if execution of the query plan succeeds, `false` otherwise
   */
  bool Execute(const AbstractPlanNode *plan, std::vector<Tuple> *result_set, Transaction *txn,
               ExecutorContext *exec_ctx) {
    // Construct and executor for the plan
    auto executor = ExecutorFactory::CreateExecutor(exec_ctx, plan);

    // Prepare the root executor
    executor->Init();

    // Execute the query plan
    try {
      if (plan->GetType() == PlanType::Aggregation) {
        // The result of aggregate plan can only be computed through the final iteration.
        RID rid;
        auto aggregate_executor = dynamic_cast<AggregationExecutor *>(executor.get());

        while (aggregate_executor->Next(result_set, &rid)) {
        }

      } else if (plan->GetType() == PlanType::HashJoin) {
        // every invocation of next in hash join will get a vector of tuples
        RID rid;
        auto hash_join_executor = dynamic_cast<HashJoinExecutor *>(executor.get());
        while (hash_join_executor->Next(result_set, &rid)) {
        }

      } else {
        // other plans can get tuples one by one
        Tuple tuple;
        RID rid;
        while (executor->Next(&tuple, &rid)) {
          if (result_set != nullptr) {
            if (rid.GetPageId() == INVALID_PAGE_ID) {
              // LOG_DEBUG("invalid tuple");
              continue;
            }
            result_set->push_back(tuple);
          }
        }
      }
    } catch (Exception &e) {
      // TODO(student): handle exceptions
      e.what();
      return false;
    }

    return true;
  }

 private:
  /** The buffer pool manager used during query execution */
  [[maybe_unused]] BufferPoolManager *bpm_;
  /** The transaction manager used during query execution */
  [[maybe_unused]] TransactionManager *txn_mgr_;
  /** The catalog used during query execution */
  [[maybe_unused]] Catalog *catalog_;
};

}  // namespace bustub
