//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager) {
  // Allocate and create individual BufferPoolManagerInstances
  for (size_t i = 0; i < num_instances; ++i) {
    buffer_pool_manager_instances_.push_back(
        new BufferPoolManagerInstance(pool_size, num_instances, i, disk_manager, log_manager));
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  for (BufferPoolManagerInstance *bmi : buffer_pool_manager_instances_) {
    delete bmi;
  }
}

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  return buffer_pool_manager_instances_[0]->GetPoolSize() * buffer_pool_manager_instances_.size();
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  return buffer_pool_manager_instances_[page_id % buffer_pool_manager_instances_.size()];
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  BufferPoolManagerInstance *bmi = dynamic_cast<BufferPoolManagerInstance *>(GetBufferPoolManager(page_id));
  return bmi->FetchPage(page_id);
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  BufferPoolManagerInstance *bmi = dynamic_cast<BufferPoolManagerInstance *>(GetBufferPoolManager(page_id));
  return bmi->UnpinPage(page_id, is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  BufferPoolManagerInstance *bmi = dynamic_cast<BufferPoolManagerInstance *>(GetBufferPoolManager(page_id));
  return bmi->FlushPage(page_id);
}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round-robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  size_t old_starting_index = starting_index_;
  starting_index_ = (starting_index_ + 1) % buffer_pool_manager_instances_.size();
  for (size_t i = old_starting_index;
       i == old_starting_index || i % buffer_pool_manager_instances_.size() != old_starting_index; ++i) {
    Page *page = buffer_pool_manager_instances_[i % buffer_pool_manager_instances_.size()]->NewPage(page_id);
    if (page != nullptr) {
      return page;
    }
  }
  return nullptr;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  BufferPoolManagerInstance *bmi = dynamic_cast<BufferPoolManagerInstance *>(GetBufferPoolManager(page_id));
  return bmi->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for (BufferPoolManagerInstance *bmi : buffer_pool_manager_instances_) {
    bmi->FlushAllPages();
  }
}

}  // namespace bustub
