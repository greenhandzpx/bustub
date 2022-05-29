//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : num_pages_(num_pages) {
  head_ = new FrameInfo(-1);
  tail_ = new FrameInfo(-1);
  head_->next_ = tail_;
  tail_->prev_ = head_;
}

LRUReplacer::~LRUReplacer() {
  FrameInfo *frame_info = head_;
  FrameInfo *dummy;
  while (frame_info != nullptr) {
    dummy = frame_info;
    frame_info = frame_info->next_;
    delete dummy;
  }
}

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> guard(latch_);

  if (frame_holders_.empty()) {
    return false;
  }
  FrameInfo *frame_info = head_->next_;
  *frame_id = frame_info->frame_id_;
  // delete the lru page;
  frame_holders_.erase(frame_info->frame_id_);
  head_->next_ = head_->next_->next_;
  head_->next_->prev_ = head_;
  delete frame_info;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);

  if (frame_holders_.find(frame_id) == frame_holders_.end()) {
    return;
  }

  FrameInfo *frame_info = frame_holders_[frame_id];
  frame_holders_.erase(frame_id);
  frame_info->prev_->next_ = frame_info->next_;
  frame_info->next_->prev_ = frame_info->prev_;
  // release the frame_info;
  delete frame_info;
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);

  if (frame_holders_.find(frame_id) == frame_holders_.end()) {
    FrameInfo *frame_info = new FrameInfo(frame_id);
    frame_holders_.insert({frame_id, frame_info});
    frame_info->prev_ = tail_->prev_;
    frame_info->next_ = tail_;
    tail_->prev_->next_ = frame_info;
    tail_->prev_ = frame_info;

    if (frame_holders_.size() > num_pages_) {
      // delete the lru page;
      frame_info = head_->next_;
      frame_holders_.erase(frame_info->frame_id_);
      head_->next_ = head_->next_->next_;
      head_->next_->prev_ = head_;
      delete frame_info;
    }
  }
  //  } else {
  //    FrameInfo* frame_info = frame_holders_[frame_id];
  //    Modify(frame_info);
  //  }
}

size_t LRUReplacer::Size() { return frame_holders_.size(); }

// void LRUReplacer::Modify(LRUReplacer::FrameInfo *frame_info) {
//   frame_info->prev_->next_ = frame_info->next_;
//   frame_info->next_->prev_ = frame_info->prev_;
//
//   frame_info->prev_ = tail_->prev_;
//   frame_info->next_ = tail_;
//   tail_->prev_->next_ = frame_info;
//   tail_->prev_ = frame_info;
// }

}  // namespace bustub
