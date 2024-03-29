//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(BufferPoolManager *buffer_pool_manager, B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page, int index);
  ~IndexIterator();

  bool IsEnd();

  const MappingType &operator*();

  IndexIterator &operator++();

  bool operator==(const IndexIterator &itr) const {
    return leaf_page_ == itr.leaf_page_ && index_ == itr.index_;
  }

  bool operator!=(const IndexIterator &itr) const {
    return !(*this == itr);
  }

 private:
  // add your own private member variables here
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page_;
  int index_;
  BufferPoolManager *buffer_pool_manager_;
};

}  // namespace bustub
