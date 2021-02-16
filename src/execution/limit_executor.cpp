//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      counter_(0) {}

void LimitExecutor::Init() {
  child_executor_->Init();
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  while (true){
    if (!child_executor_->Next(tuple, rid)){
      return false;
    }

    counter_++;
    if (counter_ > plan_->GetOffset() + plan_->GetLimit()){
      return false;
    }
    if (counter_ <= plan_->GetOffset()){
      continue;
    }

    return true;
  }
}

}  // namespace bustub
