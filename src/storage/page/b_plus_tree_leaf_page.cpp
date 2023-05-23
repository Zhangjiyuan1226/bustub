//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/config.h"
#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::LookUp(const KeyType& key, const KeyComparator& keyComparator, MappingType& item) const -> bool { 
  for(int i = 1; i <= GetSize(); i++){
    if(keyComparator(key, array_[i].first) == 0){
      item = array_[i];
      return true;
    }
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertIntoLeaf(const KeyType& key, const ValueType& value, const KeyComparator& keyComparator) -> bool{
  // before this function, we have ensure the leaf_node will not split
  // leaf node's k-v pair is store one mapping one, 
  // so when we move k-v pair backward, move all of the k-v pair
  // if key is less than the first search-key, insert into first 
  int i = 1;
  bool flag = true;
  for(; i <= GetSize(); i++){
    if(keyComparator(key, array_[i].first) < 0){
      std::move_backward(array_ + i, array_ + GetSize(), array_ + GetSize() + 1);
      flag = false;
      break;
    }
  }
  if(flag){
    i++;
  }
  array_[i].first = key;
  array_[i].second = value;
  IncreaseSize(1);
  return true;
}
// INDEX_TEMPLATE_ARGUMENTS
// auto B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNToNewNode(){
  
// }
/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS  
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) const -> MappingType { return array_[index];}
template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
