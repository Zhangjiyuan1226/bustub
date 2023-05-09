//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <utility>
#include "common/exception.h"
#include "common/logger.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  replacer_size_ = num_frames;
  meta_info_.reserve(num_frames);
  for (size_t i = 0; i < num_frames; i++) {
    meta_info_[i] = std::make_pair(0, false);
  }
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  // 若果evict成功，返回true并把evict的frame放到frame_id中
  if (curr_size_ == 0) {
    return false;
  }
  // 先从history的tail开始evict
  for (auto it = list_history_.rbegin(); it != list_history_.rend(); it++) {
    auto node = *it;
    if (meta_info_[node].second) {
      // 可以被evict
      auto map_e_it = map_history_.find(node);
      auto list_e_it = map_e_it->second;
      map_history_.erase(map_e_it);
      list_history_.erase(list_e_it);
      meta_info_[node].first = 0;
      meta_info_[node].second = false;
      *frame_id = node;
      curr_size_--;
      return true;
    }
  }
  // 如果history为空，从cache的tail开始
  for (auto it = list_cache_.rbegin(); it != list_cache_.rend(); it++) {
    auto node = *it;
    if (meta_info_[node].second) {
      // 可以被evict
      auto map_e_it = map_cache_.find(node);
      auto list_e_it = map_e_it->second;
      map_cache_.erase(map_e_it);
      list_cache_.erase(list_e_it);
      meta_info_[node].first = 0;
      meta_info_[node].second = false;
      *frame_id = node;
      curr_size_--;
      return true;
    }
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  meta_info_[frame_id].first++;
  // LRU move the just access frame to  head of list?
  size_t time = meta_info_[frame_id].first;
  if (time == k_) {
    // move it from history to cache
    auto map_e_it = map_history_.find(frame_id);
    auto list_e_it = map_e_it->second;
    map_history_.erase(map_e_it);
    list_history_.erase(list_e_it);

    list_cache_.push_front(frame_id);
    map_cache_[frame_id] = list_cache_.begin();
  } else {
    if (time > k_) {
      if (map_cache_.count(frame_id) != 0U) {
        auto map_e_it = map_cache_.find(frame_id);
        auto list_e_it = map_e_it->second;
        list_cache_.erase(list_e_it);
      }
      list_cache_.push_front(frame_id);
      map_cache_[frame_id] = list_cache_.begin();
    } else {
      if (map_history_.count(frame_id) == 0U) {
        list_history_.push_front(frame_id);
        map_history_[frame_id] = list_history_.begin();
      }
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (meta_info_[frame_id].second == set_evictable || meta_info_[frame_id].first == 0U) {
    return;
  }
  meta_info_[frame_id].second = set_evictable;
  if (set_evictable) {
    curr_size_++;
  } else {
    curr_size_--;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (curr_size_ == 0) {
    return;
  }
  auto it = meta_info_.find(frame_id);
  if (frame_id >= static_cast<frame_id_t>(replacer_size_)) {
    LOG_INFO("...");
    return;
  }
  if (static_cast<int>(it->second.first) == 0) {
    return;
  }
  if (!it->second.second) {
    throw std::exception();
  }
  if (it->second.first >= k_) {
    // remove it from list_cache_ and map_cache_
    auto map_e_it = map_cache_.find(frame_id);
    auto list_e_it = map_e_it->second;
    map_cache_.erase(map_e_it);
    list_cache_.erase(list_e_it);
  } else if (it->second.first != 0) {
    // remove it from list_history_ and map_history_
    auto map_e_it = map_history_.find(frame_id);
    auto list_e_it = map_e_it->second;
    map_history_.erase(map_e_it);
    list_history_.erase(list_e_it);
  } else {
    throw std::exception();
  }

  curr_size_ = curr_size_ - 1;
  meta_info_[frame_id] = std::make_pair(0, false);
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
