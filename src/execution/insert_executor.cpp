#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), childeren_(std::move(child_executor)){
    table_info_ = exec_ctx->GetCatalog()->GetTable(plan->TableOid());
}

void InsertExecutor::Init() {
  childeren_->Init();
 }

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if(is_success_){
    return false;
  }
  int count = 0;
  while(childeren_->Next(tuple, rid)) {
    if(table_info_->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction())) {
      count++;
      auto indexs = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
      for(auto& index_info: indexs) {
        auto key = tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->InsertEntry(key, *rid, exec_ctx_->GetTransaction());
      }
    }
  }
  std::vector<Value> value;
  value.emplace_back(INTEGER, count);

  Schema schema(plan_->OutputSchema());
  *tuple = Tuple(value, &schema);
  is_success_ = true;
  return true;
}

}  // namespace bustub