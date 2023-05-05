//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <memory>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"
#include "type/value.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
      dir_.emplace_back(std::make_shared<Bucket>(bucket_size_, 0));
    }

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IncreaceGlobalDepth() -> void {
  global_depth_ ++;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto index = IndexOf(key);
  auto bucket_ptr = dir_[index];
  return static_cast<bool>((*bucket_ptr).Find(key, value));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto index = IndexOf(key);
  auto bucket_ptr = dir_[index];
  return static_cast<bool>((*bucket_ptr).Remove(key));
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  // before insertion
  // redistribute the pair in the bucket
  std::scoped_lock<std::mutex> lock(latch_);
  auto index = IndexOf(key);
  auto bucket_ptr = dir_[index];
  while((*bucket_ptr).IsFull()){
    auto local_depth = GetLocalDepthInternal(index);
    auto item_list =(*bucket_ptr).GetItems(); 
    // auto local_size = item_list.size();
    if(GetGlobalDepthInternal() == local_depth){
      // double the size of dir_ and bucket
      global_depth_++;
      auto capacity = dir_.size();
      dir_.resize(capacity * 2);
      for(int i = 0; i < static_cast<int>(capacity); i++){
        dir_[i + capacity] = dir_[i];
      }
    }
    (*bucket_ptr).IncrementDepth();
    int mask = 1 << local_depth;
    auto bucket_add = std::make_shared<Bucket>(bucket_size_, local_depth + 1);
    for(const auto& item : item_list){
      auto index_1 = IndexOf(item.first);
      if((index_1 & mask) != 0U){
        // erase it and move it to the new added bucket
        (*dir_[index]).Remove(item.first);
        (*bucket_add).Insert(item.first, item.second);
      }
    }
    ++ num_buckets_;

    for(size_t i = 0; i < dir_.size(); i++){
      if(dir_[i] == bucket_ptr){
        if ((i & mask) != 0U) {
          dir_[i] = bucket_add;
        } 
      }
    }

    index = IndexOf(key);
    bucket_ptr = dir_[index];
  }
  // not full, just insert it!
  (*bucket_ptr).Insert(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for(auto it = list_.begin(); it != list_.end(); it++){
    if((*it).first == key){
      value = (*it).second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  // std::scoped_lock<std::mutex> lock(latch_);
  for(auto it = list_.begin(); it != list_.end(); it++){
    if((*it).first == key){
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for(auto it = list_.begin(); it != list_.end(); it++){
    if((*it).first == key){
      (*it).second = value;
      return true;
    }
  }
  if(! IsFull()){
    list_.emplace_back(std::make_pair(key, value));
    return true;
  }
  return false;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
