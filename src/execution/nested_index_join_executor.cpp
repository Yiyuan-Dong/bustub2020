//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

/**
 * WARNING: child executor should provide the same tuple as key tuple for index!
 *          That means, if child executor output schema has 5 columns, key attrs
 *          should be: {0, 1, 2, 3, 4}
 */

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_ptr_(std::move(child_executor)),
      index_ptr_(nullptr),
      table_ptr_(nullptr){}

void NestIndexJoinExecutor::Init() {
  child_executor_ptr_->Init();
  Catalog *catalog_ptr = GetExecutorContext()->GetCatalog();
  TableMetadata *table_metadata_ptr = catalog_ptr->GetTable(plan_->GetInnerTableOid());
  const std::string &table_name = table_metadata_ptr->name_;
  IndexInfo *index_info_ptr = catalog_ptr->GetIndex(plan_->GetIndexName(), table_name);
  table_ptr_ = table_metadata_ptr->table_.get();
  index_ptr_ = reinterpret_cast<B_PLUS_TREE_INDEX_TYPE*>(index_info_ptr->index_.get());
}

Tuple NestIndexJoinExecutor::CombineTuple(Tuple *left_tuple, Tuple *right_tuple) {
    std::vector<Value> res_values;
    for (auto const &col:GetOutputSchema()->GetColumns()){
      res_values.push_back(col.GetExpr()->
                           EvaluateJoin(left_tuple, plan_->OuterTableSchema(),
                                        right_tuple, plan_->InnerTableSchema()));
    }

    return Tuple{res_values, GetOutputSchema()};
}

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  while (true){
    if (!result_vector_.empty()){
      RID right_rid = result_vector_.back();
      result_vector_.pop_back();

      Tuple right_tuple;
      table_ptr_->GetTuple(right_rid, &right_tuple, GetExecutorContext()->GetTransaction());
      *tuple = CombineTuple(&left_tuple_, &right_tuple);

      return true;
    }

    if (!child_executor_ptr_->Next(&left_tuple_, rid)){
      return false;
    }

    index_ptr_->ScanKey(left_tuple_,
                        &result_vector_,
                        GetExecutorContext()->GetTransaction());
  }
}

}  // namespace bustub
