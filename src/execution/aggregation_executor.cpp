//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_{plan->GetAggregates(), plan->GetAggregateTypes()},
      aht_iterator_(aht_.Begin()){}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {
  Tuple temp_tuple;
  RID temp_rid;

  child_->Init();

  while (child_->Next(&temp_tuple, &temp_rid)){
    aht_.InsertCombine(MakeKey(&temp_tuple), MakeVal(&temp_tuple));
  }
  aht_iterator_ = aht_.Begin();
}

Tuple AggregationExecutor::GenerateOutput() {
  std::vector<Value> values;
  for (const auto &col: GetOutputSchema()->GetColumns()){
    values.push_back(col.GetExpr()->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_));
  }
  return Tuple(values, GetOutputSchema());
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  while (true){
    if (aht_iterator_ == aht_.End()){
      return false;
    }

    if (plan_->GetHaving() == nullptr ||
        plan_->GetHaving()->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_).GetAs<bool>()){
      *tuple = GenerateOutput();
      ++aht_iterator_;
      return true;
    }

    ++aht_iterator_;
  }

}

}  // namespace bustub
