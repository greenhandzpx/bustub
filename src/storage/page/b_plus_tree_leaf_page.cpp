//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
}

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (comparator(array_[i].first, key) >= 0) {
      return i;
    }
  }
  return -1;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  if (index <0 || index >= GetSize()) {
    std::cout << "[ERROR] index " << index << " out of range, size " << GetSize() << "\n";
    return KeyType{};
  }
  // std::cout << "[DEBUG] key " << array_[index].first << " at index " << index << std::endl;
  return array_[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
  if (index <0 || index >= GetSize()) {
    // TODO(greenhandzpx) not sure if the index is out of range
    return array_[0];
  }
  return array_[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  int old_size = GetSize();
  if (old_size == GetMaxSize()) {
    // the leaf page is already full, should not insert anymore
    return -1;
  }
  for (int i = 0; i < old_size; ++i) {
    if (comparator(key, array_[i].first) == 0) {
      // the key already exists
      return -1;
    }
  }
  if (comparator(key, array_[old_size-1].first) > 0) {
    array_[old_size].first = key;
    array_[old_size].second = value;

  } else {
    for (int i = 0; i < old_size; ++i) {
      if (comparator(key, array_[i].first) < 0) {
        for (int k = old_size; k > i; --k) {
          array_[k] = array_[k-1];
        }
        array_[i].first = key;
        array_[i].second = value;
        break;
      }
    }
  }
  // std::cout << "[DEBUG] insert a key " << key << " value " << value
  //   << " leaf page id " << GetPageId() << std::endl;
  SetSize(old_size + 1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  int old_size = GetSize();

  MappingType *items = new MappingType[old_size - old_size/2];
  for (int i = old_size/2; i < old_size; ++i) {
    items[i-old_size/2] = array_[i];
  }
  recipient->CopyNFrom(items, old_size-old_size/2);
  delete [] items;
  SetSize(old_size/2);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  for (int i = 0; i < size; ++i) {
    array_[i] = items[i];
  }
  SetSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (comparator(array_[i].first, key) == 0) {
      *value = array_[i].second;
      return true;
    }
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion (-1 if not exists)
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {

  for (int i = 0; i < GetSize(); ++i) {
    if (comparator(array_[i].first, key) == 0) {
      for (int k = i; k < GetSize()-1; ++k) {
        array_[k] = array_[k+1];
      }
      SetSize(GetSize() - 1);
      break;
    }
  }
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {

  // if (GetNextPageId() == recipient->GetPageId()) {
  //   // the recipent is at the right side of me
  //   for (int i = 0; i < GetSize(); ++i) {
  //     // TODO(greenhandzpx) this way is inefficient, should be optimized
  //     recipient->CopyFirstFrom(array_[i]);
  //   }
  //   // Only when we are at the leftmost side will we move all to right sibling,
  //   // so we shouldn't worry about the sibling pointer.
  // } else {
  //   // the recipent is at the left side of me
  //   for (int i = 0; i < GetSize(); ++i) {
  //     recipient->CopyLastFrom(array_[i]);
  //   }
  //   // modify the sibling pointer
  //   recipient->SetNextPageId(GetNextPageId());
  // }
  for (int i = 0; i < GetSize(); ++i) {
    recipient->CopyLastFrom(array_[i]);
  }
  // modify the sibling pointer
  recipient->SetNextPageId(GetNextPageId());
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  recipient->CopyLastFrom(array_[0]);
  for (int i = 0; i < GetSize() - 1; ++i) {
    array_[i] = array_[i+1];
  }
  SetSize(GetSize() - 1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  // std::cout << "[DEBUG] copy last from key " << item.first << std::endl;
  array_[GetSize()] = item;
  SetSize(GetSize() + 1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  recipient->CopyFirstFrom(array_[GetSize() - 1]);
  SetSize(GetSize() - 1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  for (int i = GetSize(); i > 0; --i) {
    array_[i] = array_[i-1];
  } 
  array_[0] = item;
  SetSize(GetSize() + 1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
