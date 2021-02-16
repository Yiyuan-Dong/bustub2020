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
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  index_oid_t index_id = plan_->GetIndexOid();
  Catalog *catalog = GetExecutorContext()->GetCatalog();
  IndexInfo *index_info_ptr = catalog->GetIndex(index_id);
  Index* index_ptr = index_info_ptr->index_.get();
  B_PLUS_TREE_INDEX_TYPE *b_plus_tree_index_ptr = reinterpret_cast<B_PLUS_TREE_INDEX_TYPE*>(index_ptr);
  table_metadata_ = catalog->GetTable(index_info_ptr->table_name_);
  index_iter_ = b_plus_tree_index_ptr->GetBeginIterator();
  end_iter_ = b_plus_tree_index_ptr->GetEndIterator();
  table_heap_ptr_ = table_metadata_->table_.get();
}

Tuple IndexScanExecutor::GenerateTuple(Tuple &tuple) {
  std::vector<Value> res_values;
  for (auto const &col:GetOutputSchema()->GetColumns()){
    res_values.push_back(col.GetExpr()->Evaluate(&tuple, &table_metadata_->schema_));
  }
  return {res_values, GetOutputSchema()};
}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (index_iter_!= end_iter_){
    *rid = (*index_iter_).second;
    ++index_iter_;
    table_heap_ptr_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
    if ((plan_->GetPredicate() == nullptr) ||
        plan_->GetPredicate()->Evaluate(tuple, &table_metadata_->schema_).GetAs<bool>()) {
      *tuple = GenerateTuple(*tuple);
      return true;
    }
  }
  return false;
}

}  // namespace bustub
