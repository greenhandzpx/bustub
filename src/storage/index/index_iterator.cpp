/**
 * index_iterator.cpp
 */
#include <cassert>

#include "common/config.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(): leaf_page_(nullptr), index_(-1), buffer_pool_manager_(nullptr) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer_pool_manager, 
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page, int index)
  : leaf_page_(leaf_page), 
    index_(index), 
    buffer_pool_manager_(buffer_pool_manager) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (leaf_page_ != nullptr) {
    buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::IsEnd() {
  return leaf_page_ == nullptr;
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
  assert(!IsEnd());
  return leaf_page_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if (IsEnd()) {
    return *this;
  }
  if (index_ == leaf_page_->GetSize() - 1) {
    // we have finished traversing this leaf page, switching to next one
    index_ = 0;
    page_id_t next_page_id = leaf_page_->GetNextPageId();
    buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), false);

    if (next_page_id == INVALID_PAGE_ID) {
      leaf_page_ = nullptr;
      index_ = -1;
    } else {
      leaf_page_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(
        buffer_pool_manager_->FetchPage(next_page_id)->GetData());
    }

  } else {
    ++index_;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
