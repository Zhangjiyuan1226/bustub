//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), tree_(dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_.get())),it_(tree_->GetBeginIterator()) {
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get());
  it_ = tree_->GetBeginIterator();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);
}

void IndexScanExecutor::Init() {

}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if(it_ != tree_->GetEndIterator()) {
    *rid = (*it_).second;
    if(table_info_->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction())){
      ++it_;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
