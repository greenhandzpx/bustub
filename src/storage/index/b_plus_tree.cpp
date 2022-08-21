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
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return true; }


/*
 * Helper function to get the leaf page of the given key
 */
INDEX_TEMPLATE_ARGUMENTS
BPlusTreePage *BPLUSTREE_TYPE::GetLeafPageOfKey(const KeyType &key, page_id_t *page_id) {
  // fetch the root page and then search the root page
  page_id_t next_page_id = root_page_id_;

  while (next_page_id != INVALID_PAGE_ID) {
    auto page = buffer_pool_manager_->FetchPage(next_page_id);
    // TODO(greenhandzpx) not sure
    auto b_plus_tree_page = reinterpret_cast<BPlusTreePage*>(page->GetData());
    if (b_plus_tree_page->IsLeafPage()) {
      // we finally get the leaf page
      auto leaf_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page->GetData());

      ValueType value;
      bool exists = leaf_page->Lookup(key, &value, comparator_);
      if (exists) {
        *page_id = next_page_id;
        return b_plus_tree_page;
      }
      std::cout << "[DEBUG] key " << key << " doesn't exist in leaf page " << next_page_id << std::endl;
      buffer_pool_manager_->UnpinPage(next_page_id, false);
      return nullptr;
    }
    auto intern_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(page->GetData());
    buffer_pool_manager_->UnpinPage(next_page_id, false);
    next_page_id = intern_page->Lookup(key, comparator_);
  }
  return nullptr;
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

  page_id_t leaf_page_id;
  auto page = GetLeafPageOfKey(key, &leaf_page_id);
  if (page == nullptr) {
    // the key doesn't exist
    std::cout << "[DEBUG] key " << key << " doesn't exist.\n";
    return false;
  }
  auto leaf_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page);
  ValueType value;
  leaf_page->Lookup(key, &value, comparator_);
  result->push_back(value);
  buffer_pool_manager_->UnpinPage(leaf_page_id, false);
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
  if (root_page_id_ == INVALID_PAGE_ID) {
    StartNewTree(key, value);
  } else {
    return InsertIntoLeaf(key, value);
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
  auto root_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page);
  root_page->Init(page_id, INVALID_PAGE_ID, internal_max_size_);
  // 2) just insert the kv into leaf page
  std::cout << "[DEBUG] insert key " << key << " val " << value << std::endl;
  root_page->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(page_id, true);
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
  page_id_t next_page_id = root_page_id_;

  while (next_page_id != INVALID_PAGE_ID) {
    auto page = buffer_pool_manager_->FetchPage(next_page_id);
    // TODO(greenhandzpx) not sure
    auto b_plus_tree_page = reinterpret_cast<BPlusTreePage*>(page->GetData());
    if (b_plus_tree_page->IsLeafPage()) {
      // we finally get the leaf page
      auto leaf_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page->GetData());

      int leaf_size = leaf_page->Insert(key, value, comparator_);
      if (leaf_size == -1) {
        // the key has already existed
        buffer_pool_manager_->UnpinPage(next_page_id, false);
        return false;
      }

      std::cout << "[DEBUG] insert key " << key << " value " << value << std::endl;
      if (leaf_size == leaf_max_size_) {
        // the leaf is full
        std::cout << "[DEBUG] split a node " << std::endl;
        auto new_page = Split(leaf_page);
        // fetch the middle key of the leaf page and put it into the parent page
        InsertIntoParent(leaf_page, new_page->KeyAt(0), new_page);
      }
      return true;
    }
    auto intern_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(page->GetData());
    buffer_pool_manager_->UnpinPage(next_page_id, false);
    next_page_id = intern_page->Lookup(key, comparator_);
  }
  return false;
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
  std::cout << "[DEBUG] insert a key " << key << " into parent page\n";
  page_id_t parent_page_id = old_node->GetParentPageId();

  if (parent_page_id == INVALID_PAGE_ID) {
    // the root page has split
    page_id_t new_page_id;
    auto page = buffer_pool_manager_->NewPage(&new_page_id);
    if (page == nullptr) {
      std::cout << "[ERROR] buffer pool out of memory\n";
      throw Exception(ExceptionType::OUT_OF_MEMORY, "buffer pool out of memory");
    }
    auto new_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(page);
    new_page->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);

    new_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    root_page_id_ = new_page_id;
    UpdateRootPageId(1);

    old_node->SetParentPageId(new_page_id);
    new_node->SetParentPageId(new_page_id);

    buffer_pool_manager_->UnpinPage(new_page_id, true);

  } else {
    // non-root page
    auto page = buffer_pool_manager_->FetchPage(parent_page_id);
    auto parent_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(page);
    int parent_size = parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

    new_node->SetParentPageId(parent_page_id);

    if (parent_size == internal_max_size_ + 1) {
      // the parent is full
      auto new_parent_page = Split(parent_page);
      // fetch the middle key of the leaf page and put it into the parent page
      InsertIntoParent(parent_page, new_parent_page->KeyAt(0), new_parent_page);
      buffer_pool_manager_->UnpinPage(new_parent_page->GetPageId(), true);
    }
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
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
  if (root_page_id_ == INVALID_PAGE_ID) {
    // empty tree
    return;
  }
  auto root_page = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());

  if (root_page->IsLeafPage()) {
    // the root is a leaf
    // just traverse this leaf page and find whether this key exists
    auto root_leaf_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(root_page);
    root_leaf_page->RemoveAndDeleteRecord(key, comparator_);

  } else {
    // we need to traverse down to find the right leaf
    page_id_t leaf_page_id;
    auto page = GetLeafPageOfKey(key, &leaf_page_id);
    if (page == nullptr) {
      // the key doesn't exist
      return;
    }
    auto leaf_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page);
    // delete the key from this leaf
    int leaf_size = leaf_page->RemoveAndDeleteRecord(key, comparator_);

    if (leaf_size < leaf_page->GetMinSize()) {
      // we need to merge or redistribute the leaf  
      if (CoalesceOrRedistribute(leaf_page, transaction)) {
        // the leaf is merged with its sibling page
        // then we should check whether their parent should merge
        // TODO(greenhandzpx)
      }
    }

    buffer_pool_manager_->UnpinPage(leaf_page_id, true);

  }

  buffer_pool_manager_->UnpinPage(root_page_id_, true);
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
    return false;
  }

  auto parent_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>
    (buffer_pool_manager_->FetchPage(parent_page_id)->GetData());

  page_id_t left_page_id = INVALID_PAGE_ID;
  page_id_t right_page_id = INVALID_PAGE_ID;
  BPlusTreePage *left_page = nullptr;
  BPlusTreePage *right_page = nullptr;

  // 1) check wether we can steal one kv from sibling
  int index = parent_page->ValueAt(node->GetPageId());
  if (index > 0) {
    // check its left sibling page
    left_page_id = parent_page->ValueAt(index-1);
    left_page = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(left_page_id));
    if (left_page->GetSize() > left_page->GetMinSize()) {
      // the left sibling page can give a kv to the node
      auto left_n_page = reinterpret_cast<N*>(left_page);
      if (!node->IsLeafPage()) {
        auto intern_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>
          (node);
        // we first let the array_[0].first equal to parent seperate key
        intern_node->SetKeyAt(0, parent_page->KeyAt(index));
      }
      // modify the parent's key to the one that the sibling will give(the last one)
      parent_page->SetKeyAt(index, left_n_page->KeyAt(left_n_page->GetSize()-1));
      Redistribute(left_n_page, node, 1);

      buffer_pool_manager_->UnpinPage(parent_page_id, false);
      buffer_pool_manager_->UnpinPage(left_page_id, true);
      return false;
    }
  } 

  if (index < parent_page->GetSize()-1) {
    // check its right sibling page
    right_page_id = parent_page->ValueAt(index+1);
    right_page = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(right_page_id));
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

      buffer_pool_manager_->UnpinPage(parent_page_id, false);
      buffer_pool_manager_->UnpinPage(right_page_id, true);
      return false;
    }
  }

  // 2) or we can only coalesce the page with its left sibling page(if any)

  if (left_page_id != INVALID_PAGE_ID) {
    // merge left sibling
    auto left_n_page = reinterpret_cast<N*>(left_page);
    bool res = Coalesce(&left_n_page, &node, &parent_page, index);
    buffer_pool_manager_->UnpinPage(left_page_id, true);
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
    return res; 
  }

  // or merge right sibling
  auto right_n_page = reinterpret_cast<N*>(right_page);
  // let the right sibling merge to this node
  bool res = Coalesce(&node, &right_n_page, &parent_page, index+1);
  buffer_pool_manager_->UnpinPage(right_page_id, true);
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
  return res;
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
    // if (index == 0) {
    //   // If the node is at the leftmost, then let its right sibling merge to it
    //   neighbor_intern_node->MoveAllTo(intern_node, (*parent)->KeyAt(1), buffer_pool_manager_);
    // } else {
    //   // If the node has left sibling, it should merge to left one first.
    //   intern_node->MoveAllTo(neighbor_intern_node, (*parent)->KeyAt(index), buffer_pool_manager_);
    // }
    intern_node->MoveAllTo(neighbor_intern_node, (*parent)->KeyAt(index), buffer_pool_manager_);
  }

  buffer_pool_manager_->DeletePage((*node)->GetPageId());
  if (index == 0) {
    (*parent)->Remove(1);
  } else {
    (*parent)->Remove(index);
  }

  if ((*parent)->GetSize() < (*parent)->GetMinSize()) {
    return CoalesceOrRedistribute(*parent, transaction);
  }
  
  return false;
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
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) { return false; }

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() { return INDEXITERATOR_TYPE(); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  throw Exception(ExceptionType::NOT_IMPLEMENTED, "Implement this for test");
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
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
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
