//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

// namespace bustub {

// struct HashJoinKey {
//   std::vector<Value> join_keys_;

//   bool operator==(const HashJoinKey &other) const {
//     for (uint32_t i = 0; i < other.join_keys_.size(); ++i) {
//       if (join_keys_[i].CompareEquals(other.join_keys_[i]) != CmpBool::CmpTrue) {
//         return false;
//       }
//     }
//     return true;
//   }
// };

// }  // namespace bustub

// namespace std {

//   /** Implements std::hash on HashJoinKey */
//   template <>
//   struct hash<bustub::HashJoinKey> {
//     std::size_t operator()(const bustub::HashJoinKey &hash_key) const {
//       size_t curr_hash = 0;
//       for (const auto &key : hash_key.join_keys_) {
//         if (!key.IsNull()) {
//           curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
//         }
//       }
//       return curr_hash;
//     }
//   };

// }  // namespace std

namespace bustub {

struct HashJoinKey {
  std::vector<Value> join_keys_;

  bool operator==(const HashJoinKey &other) const {
    for (uint32_t i = 0; i < other.join_keys_.size(); ++i) {
      if (join_keys_[i].CompareEquals(other.join_keys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced by the join
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  bool Next(std::vector<Tuple> *tuples, RID *rid);

  /** @return The output schema for the join */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

 private:
  /**
   * Check each tuple in the given bucket.
   */
  bool CheckBucket(Tuple *output_tuple, const Tuple &right_tuple, const std::vector<Tuple> &buckets);

  /**
   * Get the output tuple combined by left tuple and right tuple
   */
  void GetOutputTuple(const Tuple &left_tuple, Tuple *output_tuple, const Tuple &right_tuple);

  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  const std::unique_ptr<AbstractExecutor> left_executor_;
  const std::unique_ptr<AbstractExecutor> right_executor_;

  struct HashFunction {
    std::size_t operator()(const bustub::HashJoinKey &hash_key) const {
      size_t curr_hash = 0;
      for (const auto &key : hash_key.join_keys_) {
        if (!key.IsNull()) {
          curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
        }
      }
      return curr_hash;
    }
  };

  /** hash table constructed by outter table    */
  std::unordered_map<HashJoinKey, std::vector<Tuple>, HashFunction> join_hash_table_{};
};

}  // namespace bustub
