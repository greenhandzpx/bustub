//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include <cstdint>
#include "common/config.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/hash_table_page_defs.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  bool flag = false;
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (!IsOccupied(i)) {
      break;
    }
    if (!IsReadable(i)) {
      continue;
    }
    if (cmp(key, KeyAt(i)) == 0) {
      result->push_back(ValueAt(i));
      flag = true;
    }
  }
  return flag;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  size_t slot;
  bool has_slot = false;
  size_t extend_slot;
  bool has_extend_slot = false;
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (!IsOccupied(i)) {
      has_extend_slot = true;
      extend_slot = i;
      break;
    }
    if (!IsReadable(i)) {
      // tombstone or just nothing
      if (!has_slot) {
        slot = i;
        has_slot = true;
      }
      continue;
    }
    if (cmp(key, KeyAt(i)) == 0 && value == ValueAt(i)) {
      // The key already exists.
      // LOG_DEBUG("kv has existed");
      return false;
    }
  }

  if (has_slot) {
    // there is tombstone
    array_[slot] = std::make_pair(key, value);
    SetReadable(slot);
    return true;
  }

  if (has_extend_slot) {
    array_[extend_slot] = std::make_pair(key, value);
    SetOccupied(extend_slot);
    SetReadable(extend_slot);
    return true;
  }

  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (!IsOccupied(i)) {
      break;
    }
    if (!IsReadable(i)) {
      // tombstone or just nothing
      continue;
    }
    if (cmp(key, KeyAt(i)) == 0 && value == ValueAt(i)) {
      RemoveAt(i);
      return true;
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  if (!IsReadable(bucket_idx)) {
    // LOG_DEBUG("index:%d is not readable", bucket_idx);
    return {};
  }
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  if (!IsReadable(bucket_idx)) {
    // LOG_DEBUG("index:%d is not readable", bucket_idx);
    return {};
  }
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  uint8_t *dummy = reinterpret_cast<uint8_t *>(&readable_[bucket_idx / 8]);
  *dummy = *dummy & ~(1 << (bucket_idx % 8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  char dummy = occupied_[bucket_idx / 8];
  return (dummy & (1 << (bucket_idx % 8))) != 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  uint8_t *dummy = reinterpret_cast<uint8_t *>(&occupied_[bucket_idx / 8]);
  *dummy = *dummy | (1 << (bucket_idx % 8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  char dummy = readable_[bucket_idx / 8];
  return (dummy & (1 << (bucket_idx % 8))) != 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  uint8_t *dummy = reinterpret_cast<uint8_t *>(&readable_[bucket_idx / 8]);
  *dummy = *dummy | (1 << (bucket_idx % 8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  return static_cast<bool>(NumReadable() == BUCKET_ARRAY_SIZE);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  uint32_t cnt = 0;
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (!IsOccupied(i)) {
      break;
    }
    if (IsReadable(i)) {
      ++cnt;
    }
  }
  return cnt;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (IsReadable(i)) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
