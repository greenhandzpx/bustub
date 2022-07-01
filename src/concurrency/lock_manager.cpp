//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <sys/wait.h>

#include <mutex>
#include <utility>
#include <vector>
#include "common/config.h"
#include "common/logger.h"
#include "concurrency/transaction.h"

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {

  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }

  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    // "read_uncommitted" shouldn't try to acquire the shared lock
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
    return false;
  }

  LockRequest lock_request = LockRequest(txn->GetTransactionId(), LockMode::SHARED);
  lock_request.transaction_ = txn;

  latch_.lock();

  assert(txn->GetState() == TransactionState::GROWING);

  auto& request_queue = lock_table_[rid];

  // check whether there are any younger reqs in the queue
  // if so, then abort them
  for (auto iter = request_queue.request_queue_.begin(); iter != request_queue.request_queue_.end();) {
    if (iter->lock_mode_ == LockMode::EXCLUSIVE &&
        iter->txn_id_ > txn->GetTransactionId()) {
      // there is a younger request in the queue
      // then this request should abort
      AbortTxn(iter, &request_queue);
      // delete the request from queue
      request_queue.request_queue_.erase(iter++);
    } else {
      ++iter;
    }
  }

  // latch_.unlock();
  request_queue.cv_.notify_all();
  // latch_.lock();

  // push the request to the back of the queue
  request_queue.request_queue_.emplace_back(lock_request);

  auto& request = request_queue.request_queue_.back();

  // only when the front req is exclusive && no readers will it be waiting
  while (txn->GetState() != TransactionState::ABORTED && request_queue.request_queue_.front().lock_mode_ == LockMode::EXCLUSIVE && 
         request_queue.reader_count_ == 0) {

    
    latch_.unlock();
    std::unique_lock<std::mutex> ul(request_queue.latch_);
    request_queue.cv_.wait(ul);

    if (txn->GetState() == TransactionState::ABORTED) {
      // This means that the request wakes up because of aborting
      LOG_DEBUG("shared: abort, rid:%s", rid.ToString().c_str());
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
      return false;
    }

    latch_.lock();
  }

  request.granted_ = true;
  ++request_queue.reader_count_;
  LOG_DEBUG("txn:%d shared lock: reader_cnt:%d", txn->GetTransactionId(), request_queue.reader_count_);

  latch_.unlock();
  txn->GetSharedLockSet()->emplace(rid);

  return true;

}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {

  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }

  LockRequest lock_request = LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  lock_request.transaction_ = txn;
  // LOG_DEBUG("iter.txn: %p", txn);

  latch_.lock();

  auto& request_queue = lock_table_[rid];

  // check whether there are any younger reqs in the queue
  // if so, then abort them
  for (auto iter = request_queue.request_queue_.begin(); iter != request_queue.request_queue_.end();) {
    if (iter->txn_id_ > txn->GetTransactionId()) {
      // there is a younger request in the queue
      // then this request should abort
      AbortTxn(iter, &request_queue);
      // delete the request from queue
      request_queue.request_queue_.erase(iter++);

    } else {
      ++iter;
    }
  }
  // latch_.unlock();
  request_queue.cv_.notify_all();
  // latch_.lock();

  // push the request to the back of the queue
  request_queue.request_queue_.emplace_back(lock_request);

  // only when the request is at the front && reader_count == 0 will it be granted
  while (txn->GetState() != TransactionState::ABORTED && (request_queue.request_queue_.front().txn_id_ != txn->GetTransactionId() ||
          request_queue.reader_count_ > 0)) {
    latch_.unlock();
    std::unique_lock<std::mutex> ul(request_queue.latch_);
    request_queue.cv_.wait(ul);

    if (txn->GetState() == TransactionState::ABORTED) {
      // This means that the request wakes up because of aborting
      LOG_DEBUG("exclusive: abort, rid:%s, txn_id:%d", rid.ToString().c_str(), txn->GetTransactionId());
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
      return false;
    }

    latch_.lock();
  }


  request_queue.request_queue_.front().granted_ = true;

  latch_.unlock();
  txn->GetExclusiveLockSet()->emplace(rid);

  return true;


}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {

  if (!txn->IsSharedLocked(rid)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    return false;
  }

  latch_.lock();
  if (lock_table_[rid].upgrading_ != INVALID_TXN_ID) {
    latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    return false;
  }

  lock_table_[rid].upgrading_ = txn->GetTransactionId();
  latch_.unlock();

  // first release the shared lock
  if (!Unlock(txn, rid)) {
    return false;
  }

  LOG_DEBUG("upgrade: start to acquire the exclusive lock");

  // then acquire the exclusive lock
  if (!LockExclusive(txn, rid)) {
    return false;
  }

  LOG_DEBUG("upgrade: acquire the exclusive lock");
  
  latch_.lock();
  lock_table_[rid].upgrading_ = INVALID_TXN_ID;
  latch_.unlock();

  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {

  latch_.lock();
  auto& request_queue = lock_table_[rid];

  if (txn->GetState() == TransactionState::GROWING && 
      request_queue.upgrading_ != txn->GetTransactionId() &&
      txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    // should transfer to SHRINKING(except the upgrading one)
    txn->SetState(TransactionState::SHRINKING);
  }

  // erase the request from the queue
  for (auto iter = request_queue.request_queue_.begin(); 
        iter != request_queue.request_queue_.end(); ++iter) {
    if (iter->txn_id_ == txn->GetTransactionId()) {
      if (iter->lock_mode_ == LockMode::SHARED) {
        --request_queue.reader_count_;
        LOG_DEBUG("txn:%d after unlocking, reader cnt:%d", txn->GetTransactionId(), request_queue.reader_count_);
      }
      request_queue.request_queue_.erase(iter);
      break;
    }
  }
  
  latch_.unlock();

  if (txn->IsExclusiveLocked(rid)) {
    // LOG_DEBUG("exclusive lock, rid:%s", rid.ToString().c_str());
    request_queue.cv_.notify_all();
    txn->GetExclusiveLockSet()->erase(rid);

  } else if (txn->IsSharedLocked(rid)) {
    // LOG_DEBUG("shared lock, rid:%s", rid.ToString().c_str());
    // lock_table_[rid].rwlatch_.RUnlock();
    request_queue.cv_.notify_all();
    txn->GetSharedLockSet()->erase(rid);

  } else {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UNLOCK_ON_SHRINKING);
    return false;
  }

  // // get the request queue of this rid
  // if (lock_table_.find(rid) == lock_table_.end()) {
  //   latch_.unlock();
  //   return true;
  // }

  // // the one holding the lock must be the first one in the list
  // lock_table_[rid].request_queue_.pop_front();
  // latch_.unlock();
  // // notify all the requests but only the front request will be granted
  // lock_table_[rid].cv_.notify_all();


  // LOG_DEBUG("unlock");  
  return true;
}


bool LockManager::AbortTxn(std::list<LockRequest>::iterator iter, LockRequestQueue *request_queue) {
  if (iter->lock_mode_ == LockMode::SHARED && iter->granted_) {
    --(request_queue->reader_count_);
  }

  iter->transaction_->SetState(TransactionState::ABORTED);
  return true;
}


}  // namespace bustub
