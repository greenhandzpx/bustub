//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {

  std::cout << "[DEBUG] leaf max size " << leaf_max_size << " internal max size " << internal_max_size << std::endl;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }


INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnLcokAndUnpinLastPage(Transaction *transaction, OperationType type) {
  auto last_page = transaction->GetPageSet()->back();
  transaction->GetPageSet()->pop_back();
  
  if (type == OperationType::SearchKey) {
    last_page->RUnlatch();
    assert(buffer_pool_manager_->UnpinPage(last_page->GetPageId(), false));
  } else {
    last_page->WUnlatch();
    assert(buffer_pool_manager_->UnpinPage(last_page->GetPageId(), true));
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnLockAndUnpinPages(Transaction *transaction, OperationType type) {


  if (transaction == nullptr) {
    return;
  }

  // std::cout << "[DEBUG] start unlock all txn pages\n";
  auto page_set = transaction->GetPageSet();
  // std::cout << "page set size " << page_set->size() << std::endl;

  // unlock and unpin all pages above
  while (!page_set->empty()) {
    auto page = page_set->front();
    page_set->pop_front();

    auto b_plus_tree_page = reinterpret_cast<BPlusTreePage*>(page->GetData());
    if (b_plus_tree_page->IsRootPage()) {
      // note that when we update the root node, IsRootPage() won't be true because old root isn't root anymore
      // so in that case we should unlock the mutex outside
      root_page_mutex_.unlock();
    }

    // std::cout << "page id" << page->GetPageId() << " pin cnt " << page->GetPinCount() << std::endl;
    if (type == OperationType::SearchKey) {
      page->RUnlatch();
      assert(buffer_pool_manager_->UnpinPage(page->GetPageId(), false));
    } else {
      // TOOD(greenhandzpx) should be optimized
      page->WUnlatch();
      assert(buffer_pool_manager_->UnpinPage(page->GetPageId(), true));
    }
  }
  // delete some pages if any
  for (page_id_t page_id: *transaction->GetDeletedPageSet()) {
    assert(buffer_pool_manager_->DeletePage(page_id));
    LOG_DEBUG("delete page id %u", page_id);
  }
  transaction->GetDeletedPageSet()->clear();


  // std::cout << "[DEBUG] finish unlock all txn pages\n";

}

/*
 * Helper function to get the leaf page of the given key
 * Must hold the lock of the root page when calling this function
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetLeafPageOfKey(const KeyType &key, Page **page, bool leftMost, 
  OperationType type, Transaction *transaction) {
  
  if (transaction == nullptr) {
    root_page_mutex_.unlock();
  }

  // fetch the root page and then search the root page
  page_id_t next_page_id = root_page_id_;

  Page *last_page = nullptr;

  while (next_page_id != INVALID_PAGE_ID) {

    *page = buffer_pool_manager_->FetchPage(next_page_id);

    assert((*page) != nullptr);


    if (type == OperationType::SearchKey) {
      // if search, then we only need to get RLatch and relese parent's at once
      (*page)->RLatch();
      // LOG_DEBUG("get rlatch in page id %u", (*page)->GetPageId());
      if (last_page != nullptr) {
        // LOG_DEBUG("unrlatch in page id %u", last_page->GetPageId());
        last_page->RUnlatch();
        assert(buffer_pool_manager_->UnpinPage(last_page->GetPageId(), false));
        auto b_plus_tree_page = reinterpret_cast<BPlusTreePage*>(last_page->GetData());
        if (b_plus_tree_page->IsRootPage()) {
          // root page has a mutex
          root_page_mutex_.unlock();
        }
      }
      last_page = *page;
      // UnLockAndUnpinPages(transaction, OperationType::SearchKey);
      // transaction->AddIntoPageSet(*page);

    } else {
      (*page)->WLatch();
      auto b_plus_tree_page = reinterpret_cast<BPlusTreePage*>((*page)->GetData());
      if (type == OperationType::InsertKey) {
        if (b_plus_tree_page->IsLeafPage()) {
          if (b_plus_tree_page->GetSize() < b_plus_tree_page->GetMaxSize() - 1) {
            // it is safe for inserting
            UnLockAndUnpinPages(transaction, OperationType::InsertKey);
          }
        } else {
          if (b_plus_tree_page->GetSize() < b_plus_tree_page->GetMaxSize()) {
            // it is safe for inserting
            UnLockAndUnpinPages(transaction, OperationType::InsertKey);
          }
        }

      } else if (type == OperationType::DeleteKey) {

        if (b_plus_tree_page->GetSize() > b_plus_tree_page->GetMinSize()) {
          // it is safe for deleting
          UnLockAndUnpinPages(transaction, OperationType::InsertKey);
        }

      }
      transaction->AddIntoPageSet(*page);
    }


    auto b_plus_tree_page = reinterpret_cast<BPlusTreePage*>((*page)->GetData());
    if (b_plus_tree_page->IsLeafPage()) {
      // we finally get the leaf page
      auto leaf_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>((*page)->GetData());
      if (leftMost) {
        return true;
      }
      ValueType value;
      bool exists = leaf_page->Lookup(key, &value, comparator_);
      if (exists) {
        // std::cout << "[DEBUG] key " << key << " exists in leaf page " << next_page_id << std::endl;
        return true;
      }
      // std::cout << "[DEBUG] key " << key << " doesn't exist in leaf page " << next_page_id << std::endl;
      return false;
    }

    auto intern_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>((*page)->GetData());
    if (leftMost) {
      next_page_id = intern_page->ValueAt(0);
    } else {
      next_page_id = intern_page->Lookup(key, comparator_);
    }
  }
  return false;
}


/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {


  // ToString(reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData()), buffer_pool_manager_);

  Page *page = nullptr;
  if (!GetLeafPageOfKey(key, &page, false, OperationType::SearchKey, transaction)) {
    // the key doesn't exist
    // std::cout << "[DEBUG] key " << key << " doesn't exist.\n";
    auto b_plus_tree_page = reinterpret_cast<BPlusTreePage*>(page->GetData());
    if (b_plus_tree_page->IsRootPage()) {
      // root page has a mutex
      root_page_mutex_.unlock();
    }
    page->RUnlatch();
    assert(buffer_pool_manager_->UnpinPage(page->GetPageId(), false));
    // UnLockAndUnpinPages(transaction, OperationType::SearchKey);
    return false;
  }

  auto leaf_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page->GetData());
  ValueType value;
  leaf_page->Lookup(key, &value, comparator_);
  result->push_back(value);

  // we can safely release this node's lock
  auto b_plus_tree_page = reinterpret_cast<BPlusTreePage*>(page->GetData());
  if (b_plus_tree_page->IsRootPage()) {
    // root page has a mutex
    root_page_mutex_.unlock();
  }
  page->RUnlatch();
  assert(buffer_pool_manager_->UnpinPage(page->GetPageId(), false));
  // UnLockAndUnpinPages(transaction, OperationType::SearchKey);
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  root_page_mutex_.lock();
  if (root_page_id_ == INVALID_PAGE_ID) {
    StartNewTree(key, value);
    root_page_mutex_.unlock();
  } else {
    return InsertIntoLeaf(key, value, transaction);
  }
  return true;
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t page_id;
  auto page = buffer_pool_manager_->NewPage(&page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "buffer pool out of memory");
  }
  root_page_id_ = page_id;
  UpdateRootPageId();
  // 1) we first construct a root page(leaf page)
  auto root_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page->GetData());
  root_page->Init(page_id, INVALID_PAGE_ID, leaf_max_size_);
  // 2) just insert the kv into leaf page
  // std::cout << "[DEBUG] (start new tree) insert key " << key << " val " << value << std::endl;
  root_page->Insert(key, value, comparator_);
  assert(buffer_pool_manager_->UnpinPage(page_id, true));
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {

  Page *page = nullptr;
  if (GetLeafPageOfKey(key, &page, false, OperationType::InsertKey, transaction)) {
    // the key has already existed
    UnLockAndUnpinPages(transaction, OperationType::InsertKey);
    return false;
  }
  // keep pin count consistant
  buffer_pool_manager_->FetchPage(page->GetPageId());
  auto leaf_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page->GetData());
  int leaf_size = leaf_page->Insert(key, value, comparator_);
  assert(leaf_size  != -1);
  // std::cout << "[DEBUG] insert key " << key << " value " << value << std::endl;

  if (leaf_size == leaf_max_size_) {
    // the leaf is full
    // std::cout << "[DEBUG] split a node " << std::endl;
    auto new_page = Split(leaf_page);
    // fetch the middle key of the leaf page and put it into the parent page
    InsertIntoParent(leaf_page, new_page->KeyAt(0), new_page, transaction);
    // assert(buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true));

  } else {
    assert(buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true));
  }
  // release all locks
  UnLockAndUnpinPages(transaction, OperationType::InsertKey);

  return true;

}
/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t page_id;
  auto page = buffer_pool_manager_->NewPage(&page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "buffer pool out of memory");
  }
  if (node->IsLeafPage()) {
    // leaf page
    auto new_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page->GetData());
    new_page->Init(page_id, node->GetParentPageId(), leaf_max_size_);
    auto old_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(node);
    old_node->MoveHalfTo(new_page);
    // modify the sibling pointer
    new_page->SetNextPageId(old_node->GetNextPageId());
    old_node->SetNextPageId(page_id);

  } else {
    // internal page
    auto new_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(page->GetData());
    new_page->Init(page_id, node->GetParentPageId(), internal_max_size_);
    auto old_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(node);
    old_node->MoveHalfTo(new_page, buffer_pool_manager_);
  }

  return reinterpret_cast<N*>(page->GetData());
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) 
{
  // std::cout << "[DEBUG] insert a key " << key << " into parent page\n";
  page_id_t parent_page_id = old_node->GetParentPageId();

  if (parent_page_id == INVALID_PAGE_ID) {
    // the root page has split
    page_id_t new_page_id;
    auto page = buffer_pool_manager_->NewPage(&new_page_id);
    if (page == nullptr) {
      std::cout << "[ERROR] buffer pool out of memory\n";
      throw Exception(ExceptionType::OUT_OF_MEMORY, "buffer pool out of memory");
    }
    auto new_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(page->GetData());
    new_page->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);

    new_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

    root_page_id_ = new_page_id;
    UpdateRootPageId(1);
    old_node->SetParentPageId(new_page_id);
    new_node->SetParentPageId(new_page_id);

    // TODO(greenhandzpx) not sure
    // we can safely release all locks and unpin new node and new root
    // UnLockAndUnpinPages(transaction, OperationType::InsertKey);
    assert(buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true));
    assert(buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true));
    assert(buffer_pool_manager_->UnpinPage(new_page_id, true));

    root_page_mutex_.unlock();

  } else {
    // non-root page
    auto page = buffer_pool_manager_->FetchPage(parent_page_id);
    assert(page != nullptr);
    auto parent_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(page->GetData());
    int parent_size = parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

    new_node->SetParentPageId(parent_page_id);

    // we can safely release this node's lock and unpin old & new node
    // UnLcokAndUnpinLastPage(transaction, OperationType::InsertKey);
    assert(buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true));
    assert(buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true));

    if (parent_size == internal_max_size_ + 1) {
      // for internal page, only when the size = max + 1(because the first key is invalid), will it split
      auto new_parent_page = Split(parent_page);
      // fetch the middle key of the leaf page and put it into the parent page
      InsertIntoParent(parent_page, new_parent_page->KeyAt(0), new_parent_page, transaction);
      // assert(buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true));
      // assert(buffer_pool_manager_->UnpinPage(new_parent_page->GetPageId(), true));

    } else {
      // the parent is safe, then release all locks
      // UnLockAndUnpinPages(transaction, OperationType::InsertKey);
      assert(buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true));
    }

  }


}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  root_page_mutex_.lock();
  if (root_page_id_ == INVALID_PAGE_ID) {
    // empty tree
    root_page_mutex_.unlock();
    return;
  }

  // we need to traverse down to find the right leaf
  // root_page_mutex_.unlock();
  Page *page;
  if (!GetLeafPageOfKey(key, &page, false, OperationType::DeleteKey, transaction)) {
    // the key doesn't exist
    UnLockAndUnpinPages(transaction, OperationType::DeleteKey);
    return;
  }

  // here we just increase the pin count of the leaf page, in oreder to keep the pin count consistant
  assert(buffer_pool_manager_->FetchPage(page->GetPageId()));
  // std::cout << "page id " << page->GetPageId() << " pin cnt " << page->GetPinCount() << std::endl;

  auto leaf_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page->GetData());

  // delete the key from this leaf
  int leaf_size = leaf_page->RemoveAndDeleteRecord(key, comparator_);
  // std::cout << "[DEBUG] delete key " << key << std::endl;

  if (leaf_size < leaf_page->GetMinSize()) {
    // we need to merge or redistribute the leaf  
    LOG_DEBUG("leaf page %u needs to merge or redistribute, leaf_size %d, min_size %d", 
      leaf_page->GetPageId(), leaf_size, leaf_page->GetMinSize());
    if (CoalesceOrRedistribute(leaf_page, transaction)) {
      // the leaf is merged with its sibling page
      // then we should check whether their parent should merge
      // TODO(greenhandzpx)
    } 

  } else {
    assert(buffer_pool_manager_->UnpinPage(page->GetPageId(), true));
  }
  // std::cout << "page id" << page->GetPageId() << " pin cnt " << page->GetPinCount() << std::endl;
  // std::cout << "page id" << page->GetPageId() << " pin cnt " << page->GetPinCount() << std::endl;
  UnLockAndUnpinPages(transaction, OperationType::DeleteKey);


}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {

  page_id_t parent_page_id = node->GetParentPageId();
  if (parent_page_id == INVALID_PAGE_ID) {
    // this page is root page
    return AdjustRoot(node, transaction);
  }

  auto parent_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>
    (buffer_pool_manager_->FetchPage(parent_page_id)->GetData());
  assert(parent_page != nullptr);
  page_id_t left_page_id = INVALID_PAGE_ID;
  page_id_t right_page_id = INVALID_PAGE_ID;
  BPlusTreePage *left_page = nullptr;
  BPlusTreePage *right_page = nullptr;

  for (int i = 0; i < parent_page->GetSize(); ++i) {
    std::cout << "key " << parent_page->KeyAt(i) << " value " << parent_page->ValueAt(i) << std::endl;
  }
  // 1) check wether we can steal one kv from sibling
  int index = parent_page->ValueIndex(node->GetPageId());
  if (index > 0) {
    // check its left sibling page
    left_page_id = parent_page->ValueAt(index-1);
    left_page = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(left_page_id)->GetData());
    assert(left_page != nullptr);
    LOG_DEBUG("left size %d index %d", left_page->GetSize(), index-1);  
    if (left_page->GetSize() > left_page->GetMinSize()) {
      // the left sibling page can give a kv to the node
      auto left_n_page = reinterpret_cast<N*>(left_page);
      if (!node->IsLeafPage()) {
        auto intern_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>
          (node);
        assert(intern_node != nullptr);
        // we first let the array_[0].first equal to parent seperate key
        intern_node->SetKeyAt(0, parent_page->KeyAt(index));
        // std::cout << "hh\n";
      }
      // modify the parent's key to the one that the sibling will give(the last one)
      parent_page->SetKeyAt(index, left_n_page->KeyAt(left_n_page->GetSize()-1));
      Redistribute(left_n_page, node, 1);

      assert(buffer_pool_manager_->UnpinPage(parent_page_id, false));
      assert(buffer_pool_manager_->UnpinPage(left_page_id, true));
      assert(buffer_pool_manager_->UnpinPage(node->GetPageId(), true));
      // UnLockAndUnpinPages(transaction, OperationType::DeleteKey);
      // buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
      return false;
    }
  } 

  if (index < parent_page->GetSize()-1) {
    // check its right sibling page
    right_page_id = parent_page->ValueAt(index+1);
    right_page = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(right_page_id)->GetData());
    assert(right_page != nullptr);
    LOG_DEBUG("right size %d index %d", right_page->GetSize(), index+1);  
    if (right_page->GetSize() > right_page->GetMinSize()) {
      // the right sibling page can give a kv to the node
      auto right_n_page = reinterpret_cast<N*>(right_page);
      if (!node->IsLeafPage()) {
        auto right_intern_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>
          (right_n_page);
        // we first let the array_[0].first equal to parent seperate key
        right_intern_page->SetKeyAt(0, parent_page->KeyAt(index+1));
      }
      // modify the parent's key to the one after the sibling will give(the second one)
      parent_page->SetKeyAt(index+1, right_n_page->KeyAt(1));
      Redistribute(right_n_page, node, 0);

      assert(buffer_pool_manager_->UnpinPage(parent_page_id, false));
      assert(buffer_pool_manager_->UnpinPage(right_page_id, true));
      assert(buffer_pool_manager_->UnpinPage(node->GetPageId(), true));
      // UnLockAndUnpinPages(transaction, OperationType::DeleteKey);
      // buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
      return false;
    }
  }

  // 2) or we can only coalesce the page with its left sibling page(if any)
  if (left_page_id != INVALID_PAGE_ID) {
    // merge left sibling
    auto left_n_page = reinterpret_cast<N*>(left_page);
    return Coalesce(&left_n_page, &node, &parent_page, index, transaction);
    // buffer_pool_manager_->UnpinPage(left_page_id, true);
    // buffer_pool_manager_->UnpinPage(parent_page_id, true);
    // return res; 
  }

  // or merge right sibling
  auto right_n_page = reinterpret_cast<N*>(right_page);
  // let the right sibling merge to this node
  return Coalesce(&node, &right_n_page, &parent_page, index+1, transaction);
  // buffer_pool_manager_->UnpinPage(right_page_id, true);
  // buffer_pool_manager_->UnpinPage(parent_page_id, true);
  // return res;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) 
{
  if ((*node)->IsLeafPage()) {
    auto neighbor_leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(*neighbor_node);
    auto leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(*node);
    leaf_node->MoveAllTo(neighbor_leaf_node);

  } else {

    auto neighbor_intern_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(*neighbor_node);
    auto intern_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(*node);

    intern_node->MoveAllTo(neighbor_intern_node, (*parent)->KeyAt(index), buffer_pool_manager_);
  }

  // TODO(greenhandzpx) not sure whether add to the delete page set
  assert(transaction != nullptr);
  LOG_DEBUG("page id %u add into delete page set", (*node)->GetPageId());
  transaction->AddIntoDeletedPageSet((*node)->GetPageId());
  // UnLcokAndUnpinLastPage(transaction, OperationType::DeleteKey);
  // buffer_pool_manager_->DeletePage((*node)->GetPageId());
  assert(buffer_pool_manager_->UnpinPage((*node)->GetPageId(), true));
  assert(buffer_pool_manager_->UnpinPage((*neighbor_node)->GetPageId(), true));

  if (index == 0) {
    (*parent)->Remove(1);
  } else {
    (*parent)->Remove(index);
  }

  if ((*parent)->GetSize() < (*parent)->GetMinSize()) {
    // parent also needs to adjust 
    return CoalesceOrRedistribute(*parent, transaction);
  }
  // else, unLock all pages above
  assert(buffer_pool_manager_->UnpinPage((*parent)->GetPageId(), true));
  // UnLockAndUnpinPages(transaction, OperationType::DeleteKey);
  
  return true;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {

  if (node->IsLeafPage()) {

    auto neighbor_leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(neighbor_node);
    auto leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(node);
    if (index == 0) {
      // the sibling page is right
      neighbor_leaf_node->MoveFirstToEndOf(leaf_node);
    } else {
      // the sibling page is left
      neighbor_leaf_node->MoveLastToFrontOf(leaf_node);
    }

  } else {

    auto neighbor_intern_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(neighbor_node);
    auto intern_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(node);
    if (index == 0) {
      // the sibling page is right
      neighbor_intern_node->MoveFirstToEndOf(intern_node, neighbor_intern_node->KeyAt(0),
        buffer_pool_manager_);
      
    } else {
      // the sibling page is left
      neighbor_intern_node->MoveLastToFrontOf(intern_node, intern_node->KeyAt(0),
        buffer_pool_manager_);
    }
  }

}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node, Transaction *transaction) {
  if (old_root_node->GetSize() > 1) {
    // the root page still has two children
    assert(buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), true));
    // UnLockAndUnpinPages(transaction, OperationType::DeleteKey);
    return false;
  }

  if (old_root_node->GetSize() == 1 && !old_root_node->IsLeafPage()) {
    // the root has only one leaf child
    auto old_root_intern_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(old_root_node);
    auto leaf_child = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(
      buffer_pool_manager_->FetchPage(old_root_intern_node->ValueAt(0))->GetData());
    assert(leaf_child != nullptr);
    // let the child be the new root (note that we already get the root mutex)
    leaf_child->SetParentPageId(INVALID_PAGE_ID);
    root_page_id_ = leaf_child->GetPageId();
    UpdateRootPageId();
    assert(buffer_pool_manager_->UnpinPage(leaf_child->GetPageId(), true));
    assert(buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), true));
    // UnLockAndUnpinPages(transaction, OperationType::DeleteKey);
    LOG_DEBUG("page id %u add into delete page set", old_root_node->GetPageId());
    transaction->AddIntoDeletedPageSet(old_root_node->GetPageId());
    // buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    root_page_mutex_.unlock();
    return true;
  }

  if (old_root_node->GetSize() == 0) {
    // the last element of the whole tree has been deleted, which means the whole tree should be empty
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId();
    assert(buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), true));
    LOG_DEBUG("page id %u add into delete page set", old_root_node->GetPageId());
    transaction->AddIntoDeletedPageSet(old_root_node->GetPageId());
    // buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    root_page_mutex_.unlock();
    return true;
  }

  assert(buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), true));
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  auto page = FindLeafPage({}, true);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, reinterpret_cast<LeafPage*>(page->GetData()), 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  auto page = FindLeafPage(key, false);
  auto leaf_page = reinterpret_cast<LeafPage*>(page->GetData());
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page, leaf_page->KeyIndex(key, comparator_));
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() {
  return INDEXITERATOR_TYPE(buffer_pool_manager_, nullptr, -1);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  Page *page;
  root_page_mutex_.lock();
  if (leftMost) {
    assert(GetLeafPageOfKey(key, &page, true, OperationType::SearchKey, nullptr));
  } else {
    assert(GetLeafPageOfKey(key, &page, false, OperationType::SearchKey, nullptr));
  }
  auto b_plus_tree_page = reinterpret_cast<BPlusTreePage*>(page->GetData());
  if (b_plus_tree_page->IsRootPage()) {
    // root page has a mutex
    root_page_mutex_.unlock();
  }
  page->RUnlatch();
  return page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  assert(buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true));
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
