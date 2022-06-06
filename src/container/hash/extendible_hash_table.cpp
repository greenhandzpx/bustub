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
#include <cstdint>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "common/rwlatch.h"
#include "common/util/hash_util.h"
#include "container/hash/extendible_hash_table.h"
#include "storage/page/hash_table_directory_page.h"
#include "storage/page/hash_table_page_defs.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  // allocate a directory page
  buffer_pool_manager_->NewPage(&directory_page_id_);
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true));

  auto dir_page = FetchDirectoryPage();
  dir_page->SetPageId(directory_page_id_);
  // allocate a bucket page for index 0
  page_id_t page_id;
  buffer_pool_manager_->NewPage(&page_id);
  dir_page->SetBucketPageId(0, page_id);

  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true));
  assert(buffer_pool_manager_->UnpinPage(page_id, true));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::~ExtendibleHashTable() = default;
// {
// LOG_DEBUG("destruct ht");
// // delete all bucket pages
// auto dir_page = FetchDirectoryPage();
// for (size_t i = 0; i < dir_page->Size(); ++i) {
//   buffer_pool_manager_->DeletePage(dir_page->GetBucketPageId(i));
// }
// // delete the directory page
// buffer_pool_manager_->DeletePage(directory_page_id_);
// }

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
  auto dir_page =
      reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
  return dir_page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  auto bucket_page =
      reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());
  return bucket_page;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  // get the directory page
  auto dir_page = FetchDirectoryPage();
  // get the bucket page
  page_id_t page_id = KeyToPageId(key, dir_page);
  auto bucket_page = FetchBucketPage(page_id);
  auto page = reinterpret_cast<Page *>(bucket_page);
  page->RLatch();

  bool res = bucket_page->GetValue(key, comparator_, result);

  if (!res) {
    LOG_DEBUG("get value fail");
    bucket_page->PrintBucket();
    // dir_page->PrintDirectory();
  }
  page->RUnlatch();

  assert(buffer_pool_manager_->UnpinPage(page_id, false));
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));

  table_latch_.RUnlock();
  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();

  // get the directory page
  auto dir_page = FetchDirectoryPage();
  page_id_t page_id = KeyToPageId(key, dir_page);
  // get the bucket page
  auto bucket_page = FetchBucketPage(page_id);

  auto page = reinterpret_cast<Page *>(bucket_page);

  page->WLatch();
  if (bucket_page->IsFull()) {
    // The bucket is full,
    // then we should try to split a bucket and insert again.
    // we should release the rlock(it may be unsafe?(not sure))
    page->WUnlatch();

    assert(buffer_pool_manager_->UnpinPage(page_id, false));
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
    table_latch_.RUnlock();
    return SplitInsert(transaction, key, value);
  }

  if (!bucket_page->Insert(key, value, comparator_)) {
    // if the bucket isn't full which means duplicate kv
    // LOG_DEBUG("duplicate kv");
    page->WUnlatch();

    assert(buffer_pool_manager_->UnpinPage(page_id, false));
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
    LOG_DEBUG("insert fail, duplicate kv");
    table_latch_.RUnlock();
    return false;
  }

  // LOG_DEBUG("insert a kv without spliting");
  page->WUnlatch();
  assert(buffer_pool_manager_->UnpinPage(page_id, true));
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
  table_latch_.RUnlock();
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  // get the directory page
  auto dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  // get the bucket page
  auto bucket_page = FetchBucketPage(bucket_page_id);
  // get the bucket index
  uint32_t dir_index = KeyToDirectoryIndex(key, dir_page);
  // LOG_DEBUG("split insert, dir_index: %u", dir_index);

  // // get the unique index that identifies the bucket page
  // uint32_t local_index = dir_index & dir_page->GetLocalDepthMask(dir_index);
  // // get the local depth
  // uint32_t local_depth = dir_page->GetLocalDepth(dir_index);

  // maybe there is another thread that modified the bucket page,
  // so we should check again
  if (!bucket_page->IsFull()) {
    // other thread has split the page, so just insert again
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true));
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));

    table_latch_.WUnlock();

    return Insert(transaction, key, value);
  }

  if (dir_page->GetLocalDepth(dir_index) == 32) {
    // cannot increase local depth anymore
    LOG_DEBUG("insert fail, max ld");
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
    table_latch_.WUnlock();
    return false;
  }

  // allocate a new page for a new bucket
  page_id_t new_page_id;
  buffer_pool_manager_->NewPage(&new_page_id);
  assert(buffer_pool_manager_->UnpinPage(new_page_id, true));
  auto new_bucket_page = FetchBucketPage(new_page_id);

  if (dir_page->GetGlobalDepth() == dir_page->GetLocalDepth(dir_index)) {
    // i == i_j
    for (size_t i = 0; i < dir_page->Size(); ++i) {
      // e.g. i = 01, GlobalDepth = 2, then new_index = 101(old_index = 001)
      uint32_t new_index = i | (1 << dir_page->GetGlobalDepth());
      // let the new index point to the same bucket
      page_id_t page_id = dir_page->GetBucketPageId(i);
      dir_page->SetBucketPageId(new_index, page_id);
      // let the new index's local depth equal to i's
      dir_page->SetLocalDepth(new_index, dir_page->GetLocalDepth(i));
    }
    // increase the global depth(i)
    dir_page->IncrGlobalDepth();
  }

  // update all the pointers that refer to the old full page
  for (size_t i = 0; i < dir_page->Size(); ++i) {
    if (dir_page->GetBucketPageId(i) == bucket_page_id) {
      // if (dir_page->GetLocalDepth(i) == local_depth && (i & dir_page->GetLocalDepthMask(i)) == local_index) {
      // this index points to the same old bucket
      dir_page->IncrLocalDepth(i);
      if (i & dir_page->GetLocalHighBit(i)) {
        // This index should point to the newly allocated page.
        dir_page->SetBucketPageId(i, new_page_id);
      }
    }
  }

  // rehash all the kvs in the old page and decide whether they should
  // be put into the old or new bucket.
  RehashKvs(bucket_page, dir_page, new_bucket_page);
  // for (size_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
  //   KeyType key = bucket_page->KeyAt(i);
  //   // if (comparator_(key, {}) == 0) {
  //   //   // tombstone or just nothing
  //   //   continue;
  //   // }
  //   uint32_t key_index = KeyToDirectoryIndex(key, dir_page);
  //   if ((key_index & dir_page->GetLocalDepthMask(key_index)) != local_index) {
  //     // this key's index is different from the old index
  //     // should be put into the new page
  //     auto value = bucket_page->ValueAt(i);
  //     assert(new_bucket_page->Insert(key, value, comparator_));
  //     assert(bucket_page->Remove(key, value, comparator_));
  //   }
  // }

  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(new_page_id, true));

  // After we've done all the split things, we should try to insert this kv .
  // buffer_pool_manager_->UnpinPage(page_id, false);
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true));

  // LOG_DEBUG("recursively insert");
  table_latch_.WUnlock();

  return Insert(transaction, key, value);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::RehashKvs(HASH_TABLE_BUCKET_TYPE *old_page, HashTableDirectoryPage *dir_page,
                                HASH_TABLE_BUCKET_TYPE *new_page) {
  // rehash all the kvs in the old page and decide whether they should
  // be put into the old or new bucket.
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    KeyType key = old_page->KeyAt(i);
    uint32_t key_index = KeyToDirectoryIndex(key, dir_page);
    if ((key_index & dir_page->GetLocalHighBit(key_index)) != 0) {
      // this key's index's local high bit is 1
      // should be put into the new page
      auto value = old_page->ValueAt(i);
      assert(new_page->Insert(key, value, comparator_));
      assert(old_page->Remove(key, value, comparator_));
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();

  // get the directory page
  auto dir_page = FetchDirectoryPage();
  page_id_t page_id = KeyToPageId(key, dir_page);
  // get the bucket page
  auto bucket_page = FetchBucketPage(page_id);
  auto page = reinterpret_cast<Page *>(bucket_page);
  page->WLatch();

  if (bucket_page->Remove(key, value, comparator_)) {
    if (bucket_page->IsEmpty()) {
      page->WUnlatch();
      assert(buffer_pool_manager_->UnpinPage(page_id, true));
      assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
      table_latch_.RUnlock();
      Merge(transaction, key, value);
      return true;
    }
    page->WUnlatch();
    assert(buffer_pool_manager_->UnpinPage(page_id, true));
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
    table_latch_.RUnlock();
    return true;
  }
  // LOG_DEBUG("remove fail, hash: %x bucket page %u, dir_index %u", Hash(key), KeyToPageId(key, dir_page),
  //           KeyToDirectoryIndex(key, dir_page));

  // buffer_pool_manager_->UnpinPage(page_id, true);
  page->WUnlatch();

  assert(buffer_pool_manager_->UnpinPage(page_id, true));
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));

  table_latch_.RUnlock();
  // LOG_DEBUG("remove fail");

  return false;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();

  // get the directory page
  auto dir_page = FetchDirectoryPage();
  page_id_t page_id = KeyToPageId(key, dir_page);
  // get the bucket page
  auto bucket_page = FetchBucketPage(page_id);
  // LOG_DEBUG("start merging page %u", KeyToPageId(key, dir_page));

  // get the bucket index
  uint32_t dir_index = KeyToDirectoryIndex(key, dir_page);
  // case (1)(2)
  if (!bucket_page->IsEmpty() || dir_page->GetLocalDepth(dir_index) == 0) {
    // LOG_DEBUG("page not empty");
    assert(buffer_pool_manager_->UnpinPage(page_id, false));
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
    table_latch_.WUnlock();
    return;
  }

  // // case (2)
  // if (dir_page->GetLocalDepth(dir_index) == 0) {
  //   // LOG_DEBUG("local depth zero");
  //   assert(buffer_pool_manager_->UnpinPage(page_id, false));
  //   assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
  //   table_latch_.WUnlock();
  //   return;
  // }

  uint32_t split_index = dir_page->GetSplitImageIndex(dir_index);
  // LOG_DEBUG("index: %u, split index: %u, local depth: %u",
  // dir_index, split_index, dir_page->GetLocalDepth(split_index));

  // case (3)
  // uint32_t split_index;
  bool no_equal_ld_split_page = true;
  for (size_t i = 0; i < dir_page->Size(); ++i) {
    if (dir_page->GetBucketPageId(i) != page_id) {
      continue;
    }
    // find all the dir indexes that point to this empty bucket
    // LOG_DEBUG("find a index with same page, index %lu, page_id %u", i, page_id);
    split_index = dir_page->GetSplitImageIndex(i);
    if (dir_page->GetLocalDepth(i) == dir_page->GetLocalDepth(split_index)) {
      no_equal_ld_split_page = false;
      break;
    }
  }

  if (no_equal_ld_split_page) {
    // all the split index pages' local depth are unequal to their original index's
    // LOG_DEBUG("no match split-image index, page_id %u, local depth %u",
    //           page_id, dir_page->GetLocalDepth(dir_index));
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, true);
    table_latch_.WUnlock();
    return;
  }
  // uint32_t split_index = dir_page->GetSplitImageIndex(dir_index);
  // if (dir_page->GetLocalDepth(dir_index) != dir_page->GetLocalDepth(split_index)) {
  //   // LOG_DEBUG("local depth not the same as split image");
  //   buffer_pool_manager_->UnpinPage(page_id, false);
  //   buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  //   table_latch_.WUnlock();
  //   return;
  // }

  uint32_t local_depth = dir_page->GetLocalDepth(dir_index);
  page_id_t split_page_id = dir_page->GetBucketPageId(split_index);

  // let all the indexes that point to the empty page point to their split-image index's page
  for (size_t i = 0; i < dir_page->Size(); ++i) {
    if (dir_page->GetBucketPageId(i) != page_id) {
      continue;
    }
    dir_page->SetBucketPageId(i, split_page_id);
    // decrease the depth of both index and split index
    split_index = dir_page->GetSplitImageIndex(i);
    if (dir_page->GetLocalDepth(split_index) == local_depth) {
      dir_page->DecrLocalDepth(split_index);
    }
    dir_page->DecrLocalDepth(i);
  }

  // delete the empty page
  assert(buffer_pool_manager_->UnpinPage(page_id, false));
  assert(buffer_pool_manager_->DeletePage(page_id));

  // shrink the dir_page
  if (dir_page->CanShrink()) {
    dir_page->DecrGlobalDepth();
  }

  auto split_page = FetchBucketPage(split_page_id);
  // after merging, check whether the split-image is also empty
  if (split_page->IsEmpty()) {
    // if split-image is also empty, we should merge recursively
    buffer_pool_manager_->UnpinPage(split_page_id, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, true);
    table_latch_.WUnlock();
    Merge(transaction, key, value);

  } else {
    buffer_pool_manager_->UnpinPage(split_page_id, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, true);
    table_latch_.WUnlock();
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::PrintDirectoryAndBuckets() {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->PrintDirectory();
  for (size_t i = 0; i < dir_page->Size(); ++i) {
    page_id_t page_id = dir_page->GetBucketPageId(i);
    auto page = FetchBucketPage(page_id);
    page->PrintBucket();
    assert(buffer_pool_manager_->UnpinPage(page_id, false));
  }
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
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
