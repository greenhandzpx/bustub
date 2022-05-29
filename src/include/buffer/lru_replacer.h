//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

 private:
  // TODO(student): implement me!
  struct FrameInfo {
    explicit FrameInfo(frame_id_t frame_id) : frame_id_(frame_id), prev_(nullptr), next_(nullptr) {}

    frame_id_t frame_id_;
    //    uint64_t last_access_time_;
    FrameInfo *prev_;
    FrameInfo *next_;
  };
  //  /**
  //   * Change the location of the frame to tail.
  //   * @param frame_info a frame info pointer
  //   */
  //  void Modify(FrameInfo *frame_info);

  FrameInfo *head_;
  FrameInfo *tail_;

  std::unordered_map<frame_id_t, FrameInfo *> frame_holders_ = {};
  size_t num_pages_;
  std::mutex latch_;
};

}  // namespace bustub
