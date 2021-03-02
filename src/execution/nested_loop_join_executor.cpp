//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan),
      left_executor_ptr_(std::move(left_executor)),
      right_executor_ptr_(std::move(right_executor)){}

void NestedLoopJoinExecutor::Init() {
  left_executor_ptr_->Init();
  right_executor_ptr_->Init();
  RID temp_rid;
  left_ret_ = left_executor_ptr_->Next(&left_tuple_, &temp_rid);
}

Tuple NestedLoopJoinExecutor::CombineTuple(Tuple *left_tuple, Tuple *right_tuple) {
  std::vector<Value> res_values;
  for (auto const &col:GetOutputSchema()->GetColumns()){
    res_values.push_back(col.GetExpr()->EvaluateJoin(left_tuple, left_executor_ptr_->GetOutputSchema(),
                                                     right_tuple, right_executor_ptr_->GetOutputSchema()));
  }

  return Tuple{res_values, GetOutputSchema()};
}

/**
 * I also think there needs no modification. It's child node's duty to
 * manage lock
 */
bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple right_tuple;
  RID temp_rid;
  while (true){
    if (!left_ret_){
      return false;
    }

    if (!right_executor_ptr_->Next(&right_tuple, &temp_rid)){
      right_executor_ptr_->Init();
      left_ret_ = left_executor_ptr_->Next(&left_tuple_, &temp_rid);
      continue;
    }

    if (plan_->Predicate()->EvaluateJoin(&left_tuple_,
                                         left_executor_ptr_->GetOutputSchema(),
                                         &right_tuple,
                                         right_executor_ptr_->GetOutputSchema()
                                         ).GetAs<bool>()){
      *tuple = CombineTuple(&left_tuple_, &right_tuple);
      return true;
    }
  }
}
}  // namespace bustub
