//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sys/types.h>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"
#include "storage/page/hash_table_directory_page.h"
#include "storage/page/hash_table_page_defs.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!

}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t hash = Hash(key);
  return hash & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t dir_index = KeyToDirectoryIndex(key, dir_page); 
  return dir_page->GetBucketPageId(dir_index);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t GetBucketCapacity(uint32_t GlobalDepth, uint32_t LocalDepth) {
  if (GlobalDepth < LocalDepth) {
    return 0;
  }
  return 1 << (GlobalDepth - LocalDepth);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  auto dir_page = reinterpret_cast<HashTableDirectoryPage*>(
    buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
  return dir_page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  auto bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE*>(
    buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());
  return bucket_page;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  // get the directory page
  auto dir_page = FetchDirectoryPage(); 
  // get the bucket page
  auto bucket_page = FetchBucketPage(KeyToPageId(key, dir_page));
  return bucket_page->GetValue(key, comparator_, result);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // get the directory page
  auto dir_page = FetchDirectoryPage(); 
  // get the bucket page
  auto bucket_page = FetchBucketPage(KeyToPageId(key, dir_page));
  if (!bucket_page->Insert(key, value, comparator_)) {
    // The bucket is full or the kv exists.
    if (!bucket_page->IsFull()) {
      // if the bucket isn't full which means duplicate kv
      return false;
    }
    // The bucket is full, 
    // then we should try to split a bucket and insert again.
    return SplitInsert(transaction, key, value);
  }
  LOG_DEBUG("insert a kv without spliting");
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // get the directory page
  auto dir_page = FetchDirectoryPage(); 
  // get the bucket page
  auto bucket_page = FetchBucketPage(KeyToPageId(key, dir_page));
  // get the bucket index
  uint32_t dir_index = KeyToDirectoryIndex(key, dir_page);
  // get the unique index that identifies the bucket page
  uint32_t local_index = dir_index & dir_page->GetLocalDepthMask(dir_index);

  // allocate a new page for a new bucket
  page_id_t new_page_id;
  buffer_pool_manager_->NewPage(&new_page_id);
  auto new_bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE*>(
    buffer_pool_manager_->FetchPage(new_page_id)->GetData());

  if (dir_page->GetGlobalDepth() <= dir_page->GetLocalDepth(dir_index)) {
    // i == i_j
    for (size_t i = 0; i < dir_page->Size(); ++i) {
      // e.g. i = 01, GlobalDepth = 2, then new_index = 101(old_index = 001)
      uint32_t new_index = i | (1 << dir_page->GetGlobalDepth());
      // let the new index point to the same bucket
      page_id_t page_id = dir_page->GetBucketPageId(i);
      dir_page->SetBucketPageId(new_index, page_id);
    }
    // increase the global depth(i)
    dir_page->IncrGlobalDepth();
  }

  for (size_t i = 0; i < dir_page->Size(); ++i) {
    // update all the pointers that refer to the old full page
    if ((i & dir_page->GetLocalDepthMask(i)) == local_index) {
      // this index points to the same old bucket
      if (dir_page->GetLocalHighBit(i)) {
        // This index should point to the newly allocated page.
        dir_page->SetBucketPageId(i, new_page_id);
      }
      dir_page->IncrLocalDepth(i);
    }
  }

  // rehash all the kvs in the old page and decide whether they should
  // be put into the old or new bucket.
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    KeyType key = bucket_page->KeyAt(i);
    if (comparator_(key, {}) == 0) {
      // tombstone or just nothing
      continue;
    } 
    uint32_t key_index = KeyToDirectoryIndex(key, dir_page);
    if ((key_index & dir_page->GetLocalDepthMask(key_index)) != local_index) {
      // this key's index is different from the old index
      // should be put into the new page
      auto value = bucket_page->ValueAt(i);
      bucket_page->Remove(key, value, comparator_);
      new_bucket_page->Insert(key, value, comparator_);
    }
  }

  // After we've done all the split things, we should try to insert this kv .
  // get the bucket page where this kv should be put
  bucket_page = FetchBucketPage(KeyToPageId(key, dir_page));
  // try to insert the kv again
  if (bucket_page->Insert(key, value, comparator_)) {
    LOG_DEBUG("insert a kv after spliting");
    return true;
  }
  return false;
  // return static_cast<bool>(bucket_page->Insert(key, value, comparator_));
}


/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // get the directory page
  auto dir_page = FetchDirectoryPage(); 
  // get the bucket page
  auto bucket_page = FetchBucketPage(KeyToPageId(key, dir_page));
  if (bucket_page->Remove(key, value, comparator_)) {
    if (bucket_page->IsEmpty()) {
      Merge(transaction, key, value);
    }
    return true;
  }
  return false;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // TODO(greenhandzpx): 
  // get the directory page
  auto dir_page = FetchDirectoryPage(); 
  // get the bucket page
  auto bucket_page = FetchBucketPage(KeyToPageId(key, dir_page));
  // case (1)
  if (!bucket_page->IsEmpty()) {
    return;
  }
  // get the bucket index
  uint32_t dir_index = KeyToDirectoryIndex(key, dir_page);
  // case (2)
  if (dir_page->GetLocalDepth(dir_index) == 0) {
    return;
  }
  // case (3)
  uint32_t split_index = dir_page->GetSplitImageIndex(dir_index);
  if (dir_page->GetLocalDepth(dir_index) != dir_page->GetLocalDepth(split_index)) {
    return;
  }
  // delete the empty page
  page_id_t page_id = dir_page->GetBucketPageId(dir_index);
  buffer_pool_manager_->DeletePage(page_id);
  // let the index point to its split-image index's page
  page_id = dir_page->GetBucketPageId(split_index);
  dir_page->SetBucketPageId(dir_index, page_id);
  // decrease the depth of both index and split index
  dir_page->DecrLocalDepth(dir_index);
  dir_page->DecrLocalDepth(split_index);
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
