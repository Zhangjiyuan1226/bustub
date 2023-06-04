//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"
#include "storage/table/tuple.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child)
: AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child)), aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()), aht_iterator_(aht_.Begin()){
}

void AggregationExecutor::Init() {
    child_->Init();
    Tuple tuple{};
    RID rid{};
    while(child_->Next(&tuple, &rid)){
        // FIXME 在哪里存储rid？
        // LOG_DEBUG("in!");
        auto key = MakeAggregateKey(&tuple);
        auto value = MakeAggregateValue(&tuple);
        aht_.InsertCombine(key, value);
    }
    // 
    if (aht_.Size() == 0 && GetOutputSchema().GetColumnCount() == 1) {
        aht_.InsertIntialCombine();
    }

    aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if(aht_iterator_ == aht_.End()){
        return false;
    }    
    std::vector<Value> values;
    values.insert(values.end(), aht_iterator_.Key().group_bys_.begin(), aht_iterator_.Key().group_bys_.end());
    values.insert(values.end(), aht_iterator_.Val().aggregates_.begin(), aht_iterator_.Val().aggregates_.end());
    *tuple = Tuple{values, &GetOutputSchema()};
    // std::cout<<*rid<<std::endl;
    ++aht_iterator_;
    //FIXME 如何获得rid

    // ++aht_iteartor_;j
    return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
