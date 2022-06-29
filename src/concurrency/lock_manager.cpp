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

  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && 
      txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  LockRequest lock_request = LockRequest(txn->GetTransactionId(), LockMode::SHARED);
  lock_request.transaction_ = txn;

  latch_.lock();

  auto& request_queue = lock_table_[rid];

  // check whether there are any younger reqs in the queue
  // if so, then abort them

  for (auto & iter : request_queue.request_queue_) {
    if (iter.lock_mode_ == LockMode::EXCLUSIVE && 
        iter.txn_id_ > txn->GetTransactionId()) {
      // there is a younger request in the queue
      // then this request should abort
      iter.transaction_->SetState(TransactionState::ABORTED);
    }
  }

  latch_.unlock();
  request_queue.cv_.notify_all();

  latch_.lock();
  // for (auto & iter : request_queue.request_queue_) {
  //   if (iter.lock_mode_ == LockMode::EXCLUSIVE && iter.txn_id_ > txn->GetTransactionId()) {
  //     // there is a younger request in the queue
  //     // then this request should abort
  //     iter.transaction_->SetState(TransactionState::ABORTED);
  //     request_queue.cv_.notify_all();
  //     // iter.granted_ = false;
  //     // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  //   }
  // }

  // push the request to the back of the queue
  request_queue.request_queue_.emplace_back(lock_request);

  auto& request = request_queue.request_queue_.back();

  // only when the front req is exclusive && no readers will it be waiting
  while (txn->GetState() != TransactionState::ABORTED && request_queue.request_queue_.front().lock_mode_ == LockMode::EXCLUSIVE && 
         request_queue.reader_count_ == 0) {
    latch_.unlock();
    std::unique_lock<std::mutex> ul(request_queue.latch_);
    request_queue.cv_.wait(ul);

    // if (txn->GetState() == TransactionState::ABORTED) {
    //   throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    //   return false;
    // }

    latch_.lock();
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    // This means that the request wakes up because of aborting
    // erase the request from the queue
    for (auto iter = request_queue.request_queue_.begin(); 
          iter != request_queue.request_queue_.end(); ++iter) {
      if (iter->txn_id_ == txn->GetTransactionId()) {
        if (iter->lock_mode_ == LockMode::SHARED) {
          --request_queue.reader_count_;
        }
        request_queue.request_queue_.erase(iter);
        break;
      }
    }
    LOG_DEBUG("shared: abort, rid:%s", rid.ToString().c_str());
    // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    latch_.unlock();
    return false;
  }

  request.granted_ = true;
  ++request_queue.reader_count_;

  latch_.unlock();
  txn->GetSharedLockSet()->emplace(rid);

  return true;




  // auto& request = lock_table_[rid].request_queue_.back();

  // auto request_queue = lock_table_[rid].request_queue_;
  // for (auto & iter : request_queue) {
  //   if (iter.lock_mode_ == LockMode::EXCLUSIVE && iter.granted_ && iter.txn_id_ > txn->GetTransactionId()) {
  //     // the one waiting for the lock is older than this request
  //     // then this request should abort
  //     iter.transaction_->SetState(TransactionState::ABORTED);
  //     // iter.granted_ = false;
  //     throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  //     break;
  //   }
  // }

  // latch_.unlock();

  // lock_table_[rid].rwlatch_.RLock();
  // // when acquiring a shared lock, 
  // // check whether there is an older request waiting for a exclusive lock
  // for (auto & iter : request_queue) {
  //   if (iter.lock_mode_ == LockMode::EXCLUSIVE && iter.txn_id_ < txn->GetTransactionId()) {
  //     // the one waiting for the lock is older than this request
  //     // then this request should abort
  //     txn->SetState(TransactionState::ABORTED);
  //     lock_table_[rid].rwlatch_.RUnlock();
  //     throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  //     return false;
  //   }
  // }

  // request.granted_ = true;  
  // // if (lock_table_.find(rid) == lock_table_.end()) {
  // //   // There is no other request of the lock or no other request of write lock
  // //   // then we just grant the lock and insert the req into the queue
  // //   lock_request.granted_ = true;
  // //   lock_table_[rid].request_queue_.emplace_back(lock_request);
  // //   latch_.unlock();
  // //   lock_table_[rid].rwlatch_.RLock();

  // // } else {

  // //   lock_table_[rid].request_queue_.emplace_back(lock_request);
  // //   latch_.unlock();


  // //   // std::unique_lock<std::mutex> ul(latch_);
  // //   // while (lock_table_[rid].request_queue_.front().txn_id_ != txn->GetTransactionId()) {
  // //   //   // wait until the request becomes the first request in the queue
  // //   //   lock_table_[rid].cv_.wait(ul);
  // //   // }

  // // }

  // // switch (txn->GetIsolationLevel()) {
  // //   case IsolationLevel::REPEATABLE_READ:
  // //   case IsolationLevel::READ_COMMITTED:
  // //   case IsolationLevel::READ_UNCOMMITTED:
  // // }

  // txn->GetSharedLockSet()->emplace(rid);

  // return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && 
      txn->GetState() == TransactionState::SHRINKING) {
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
  for (auto & iter : request_queue.request_queue_) {
    if (iter.txn_id_ > txn->GetTransactionId()) {
      // there is a younger request in the queue
      // then this request should abort
      iter.transaction_->SetState(TransactionState::ABORTED);
      // iter.granted_ = false;
      // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }
  latch_.unlock();
  request_queue.cv_.notify_all();

  latch_.lock();
  // push the request to the back of the queue
  request_queue.request_queue_.emplace_back(lock_request);

  // only when the request is at the front && reader_count == 0 will it be granted
  while (txn->GetState() != TransactionState::ABORTED && (request_queue.request_queue_.front().txn_id_ != txn->GetTransactionId() ||
          request_queue.reader_count_ > 0)) {
    latch_.unlock();
    std::unique_lock<std::mutex> ul(request_queue.latch_);
    request_queue.cv_.wait(ul);
    // if (txn->GetState() == TransactionState::ABORTED) {
    //   throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    //   return false;
    // }
    latch_.lock();
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    // This means that the request wakes up because of aborting

    // erase the request from the queue
    for (auto iter = request_queue.request_queue_.begin(); 
          iter != request_queue.request_queue_.end(); ++iter) {
      LOG_DEBUG("iter: %d", iter->txn_id_);
      if (iter->txn_id_ == txn->GetTransactionId()) {
        if (iter->lock_mode_ == LockMode::SHARED) {
          --request_queue.reader_count_;
        }
        request_queue.request_queue_.erase(iter);
        break;
      }
    }
    LOG_DEBUG("exclusive: abort, rid:%s", rid.ToString().c_str());
    // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    latch_.unlock();
    return false;
  }

  request_queue.request_queue_.front().granted_ = true;

  latch_.unlock();
  txn->GetExclusiveLockSet()->emplace(rid);

  return true;




  // latch_.lock();

  // lock_table_[rid].request_queue_.emplace_back(lock_request);
  

  // auto& request = lock_table_[rid].request_queue_.back();

  // auto request_queue = lock_table_[rid].request_queue_;
  // for (auto & iter : request_queue) {
  //   if (iter.granted_ && iter.txn_id_ > txn->GetTransactionId()) {
  //     // the one holding the lock is younger than this request
  //     // then that one should abort
  //     LOG_DEBUG("exclusive: find a younger one, id %d", iter.txn_id_);
  //     LOG_DEBUG("iter.txn: %p", iter.transaction_);
  //     iter.transaction_->SetState(TransactionState::ABORTED);
  //     // iter.granted_ = false;
  //     // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  //   }
  // }

  // latch_.unlock();

  // lock_table_[rid].rwlatch_.WLock();  
  // latch_.lock();
  // // when acquiring an exclusive lock, 
  // // check whether there are older requests waiting for a shared/exclusive lock
  // request_queue = lock_table_[rid].request_queue_;
  // for (auto & iter : request_queue) {
  //   if (iter.txn_id_ < txn->GetTransactionId()) {
  //     // the one waiting for the lock is older than this request
  //     // then this request should abort
  //     txn->SetState(TransactionState::ABORTED);
  //     latch_.unlock();
  //     lock_table_[rid].rwlatch_.WUnlock();
  //     throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  //     return false;
  //   }
  // }
  // request.granted_ = true;
  // latch_.unlock();

  // // if (lock_table_.find(rid) == lock_table_.end() || 
  // //     lock_table_[rid].request_queue_.empty() || 
  // //     lock_table_[rid].request_queue_.front().lock_mode_ == LockMode::SHAR) {

  // //   // There is no other request of the lock or no other request of write lock
  // //   // then we just grant the lock and insert the req into the queue
  // //   lock_request.granted_ = true;
  // //   lock_table_[rid].request_queue_.emplace_back(lock_request);

  // // } else {

  // //   lock_table_[rid].request_queue_.emplace_back(lock_request);
  // //   latch_.unlock();
  // //   std::unique_lock<std::mutex> ul(latch_);
  // //   while (lock_table_[rid].request_queue_.front().txn_id_ != txn->GetTransactionId()) {
  // //     // wait until the request becomes the first request in the queue
  // //     lock_table_[rid].cv_.wait(ul);
  // //   }

  // // }

  // txn->GetExclusiveLockSet()->emplace(rid);

  // return true;
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

  // then acquire the exclusive lock
  if (!LockExclusive(txn, rid)) {
    return false;
  }
  
  latch_.lock();
  lock_table_[rid].upgrading_ = INVALID_TXN_ID;
  latch_.unlock();

  return true;
  // lock_table_[rid].upgrading_ = txn->GetTransactionId();
  // lock_table_[rid].rwlatch_.RUnlock();
  // txn->GetSharedLockSet()->erase(rid);

  // auto request_queue = lock_table_[rid].request_queue_;
  // for (auto & iter : request_queue) {
  //   if (iter.granted_ && iter.txn_id_ > txn->GetTransactionId()) {
  //     // the one holding the lock is younger than this request
  //     // then that one should abort
  //     iter.transaction_->SetState(TransactionState::ABORTED);
  //     iter.granted_ = false;
  //     throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  //   }
  // }


  // lock_table_[rid].rwlatch_.WLock();

  // // auto request_queue = lock_table_[rid].request_queue_;
  // // for (auto & iter : request_queue) {
  // //   if (iter.txn_id_ < txn->GetTransactionId()) {
  // //     // the one waiting for the lock is older than this request
  // //     // then this request should abort
  // //     txn->SetState(TransactionState::ABORTED);
  // //     lock_table_[rid].rwlatch_.WUnlock();
  // //     throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  // //     return false;
  // //   }
  // // }
  // txn->GetExclusiveLockSet()->emplace(rid);

  // return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {

  latch_.lock();
  auto& request_queue = lock_table_[rid];

  if (txn->GetState() == TransactionState::GROWING && 
      request_queue.upgrading_ != txn->GetTransactionId()) {
    // should transfer to SHRINKING(except the upgrading one)
    txn->SetState(TransactionState::SHRINKING);
  }

  // erase the request from the queue
  for (auto iter = request_queue.request_queue_.begin(); 
        iter != request_queue.request_queue_.end(); ++iter) {
    if (iter->txn_id_ == txn->GetTransactionId()) {
      if (iter->lock_mode_ == LockMode::SHARED) {
        --request_queue.reader_count_;
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

}  // namespace bustub
