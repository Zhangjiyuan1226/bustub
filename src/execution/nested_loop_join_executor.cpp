//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/type.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), 
    plan_(plan),
    left_executor_(std::move(left_executor)),
    right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  auto left = plan_->GetLeftPlan();
  plan_->GetChildren();
}

void NestedLoopJoinExecutor::Init() { 
  left_executor_->Init();
  right_executor_->Init();  
  Tuple right_tuple{};
  RID rid{};
  while(right_executor_->Next(&right_tuple, &rid)){
    right_tuples_.emplace_back(right_tuple);
  }
  right_idx_ = -1;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  RID rid_base{};
  while(right_idx_ >= 0 || left_executor_->Next(&left_tuple_, &rid_base)){
    std::vector<Value> vals;
    for(uint32_t i = (right_idx_ < 0 ? 0 : right_idx_); i < right_tuples_.size(); i++){
      Value value = plan_->Predicate().EvaluateJoin(&left_tuple_, plan_->GetLeftPlan()->OutputSchema(), &right_tuples_[i], plan_->GetRightPlan()->OutputSchema());
      if(!value.IsNull() && value.GetAs<bool>()){
        Schema schema = GetOutputSchema();
        for (uint32_t idx = 0; idx < left_executor_->GetOutputSchema().GetColumnCount(); idx++) {
          vals.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), idx));
        }
        for (uint32_t idx = 0; idx < right_executor_->GetOutputSchema().GetColumnCount(); idx++) {
          vals.push_back(right_tuples_[i].GetValue(&right_executor_->GetOutputSchema(), idx));
        }
        *tuple = {vals, &schema};
        right_idx_ = i + 1;
        return true;
      }
    }
    if (right_idx_ == -1 && plan_->GetJoinType() == JoinType::LEFT) {
      for (uint32_t idx = 0; idx < left_executor_->GetOutputSchema().GetColumnCount(); idx++) {
        vals.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), idx));
      }
      for (uint32_t idx = 0; idx < right_executor_->GetOutputSchema().GetColumnCount(); idx++) {
        vals.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(idx).GetType()));
      }
      *tuple = Tuple(vals, &GetOutputSchema());
      return true;
    }
    right_idx_ = -1;
  }
  return false;
}

}  // namespace bustub
