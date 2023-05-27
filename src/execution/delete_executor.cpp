//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "catalog/schema.h"
#include "execution/executors/delete_executor.h"
#include "type/type_id.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_{std::move(child_executor)} {
        table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
        is_end_ = false;
}

void DeleteExecutor::Init() { 
    child_executor_->Init();
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if(is_end_){
        return false;
    }    
    int count = 0;
    while(child_executor_->Next(tuple, rid)){
        LOG_DEBUG("%u",rid->GetSlotNum());
        bool result = table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction());
        if(result){
            ++count;
            // update indexes
            std::vector<IndexInfo *> indexs = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_); 
            for(auto* index : indexs){
                auto key = tuple->KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
                index->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
            }
        }
    }
    // Tuple(std::vector<Value> values, const Schema *schema);
    std::vector<Value> values{};
    values.reserve(GetOutputSchema().GetColumnCount());
    values.emplace_back(INTEGER ,count);
    *tuple = Tuple{values, &GetOutputSchema()};
    is_end_ = true;
    return true;
}

}  // namespace bustub
