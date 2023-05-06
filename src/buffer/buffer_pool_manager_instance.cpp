//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"
#include <mutex>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * { 
  std::scoped_lock lock(latch_);
  frame_id_t frame_id;
  Page* page = nullptr;
  bool is_free_page = false;
  for(size_t i = 0; i < pool_size_; i++){
    if(pages_[i].GetPinCount() == 0){
      is_free_page = true;
      break;
    }
  }
  if(!is_free_page){
    return page;
  }
  if(!free_list_.empty()){
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = &pages_[frame_id]; 
  }else{
    if(replacer_->Evict(&frame_id)){
      page = &pages_[frame_id];  
      if(page->IsDirty()){
        disk_manager_->WritePage(page->GetPageId(), page->GetData());
      }
      page_table_->Remove(page->GetPageId());

    }else{
      return nullptr;
    }
  }
  *page_id = AllocatePage();
  page_table_->Insert(*page_id, frame_id);
  page->ResetMemory();
  page->is_dirty_ = false;
  page->pin_count_ = 1;
  page->page_id_ = *page_id;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return page; 
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * { 
  std::scoped_lock lock(latch_);
  frame_id_t frame_id;
  Page* page = nullptr;
  if (page_table_->Find(page_id, frame_id)) {
    pages_[frame_id].pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];
  }
  bool is_free_page = false;
  for(size_t i = 0; i < pool_size_; i++){
    if(pages_[i].GetPinCount() == 0){
      is_free_page = true;
      break;
    }
  }
  if(!is_free_page){
    return page;
  }
  if(!free_list_.empty()){
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = &pages_[frame_id]; 
  }else{
    if(replacer_->Evict(&frame_id)){
      page = &pages_[frame_id];  
      if(page->IsDirty()){
        disk_manager_->WritePage(page->GetPageId(), page->GetData());
      }
      page_table_->Remove(page->GetPageId());
    }else{
      return nullptr;
    }
  }

  page_table_->Insert(page_id, frame_id);
  page->ResetMemory();
  page->is_dirty_ = false;
  page->pin_count_ = 1;
  page->page_id_ = page_id;
  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return page; 
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool { 
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if(page_table_->Find(page_id, frame_id)){
    // 在buffer pool中
    Page* page = &pages_[frame_id];
    page->is_dirty_ = is_dirty;
    if(page->GetPinCount() == 0){
      return false;
    }
    page->pin_count_ -- ;
    if(page->GetPinCount() == 0){
      replacer_->SetEvictable(frame_id, true);
    }
    return true;
  }
  return false;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool { 
  frame_id_t frame_id;
  if(page_id == INVALID_PAGE_ID){
    return false;
  }
  if(page_table_->Find(page_id, frame_id)){
    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
    pages_[frame_id].is_dirty_ = false;
    return true;
  }
  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t frame_id = 0; frame_id < pool_size_; frame_id++) {
    FlushPgImp(pages_[frame_id].GetPageId());
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }

  if (pages_[frame_id].GetPinCount() > 0) {
    return false;
  }

  replacer_->Remove(frame_id);

  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;

  page_table_->Remove(page_id);
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
