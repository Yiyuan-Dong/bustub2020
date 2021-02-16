//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan){}

void SeqScanExecutor::Init() {
  BufferPoolManager *bpm_ptr = exec_ctx_->GetBufferPoolManager();
  table_oid_t table_id = plan_->GetTableOid();
  table_metadata_ptr_ = GetExecutorContext()->GetCatalog()->GetTable(table_id);
  table_heap_ptr_ = table_metadata_ptr_->table_.get();
  page_id_t first_page_id = table_heap_ptr_->GetFirstPageId();


  Page *page_ptr = bpm_ptr->FetchPage(first_page_id);

  RID rid{};
  TablePage *table_page_ptr = reinterpret_cast<TablePage *>(page_ptr);
  assert(table_page_ptr->GetFirstTupleRid(&rid));

  bpm_ptr->UnpinPage(first_page_id, false);


  table_iter_ = TableIterator(table_heap_ptr_, rid, exec_ctx_->GetTransaction());
}

Tuple SeqScanExecutor::GenerateTuple(Tuple &tuple) {
  std::vector<Value> res_values;
  for (auto const &col:GetOutputSchema()->GetColumns()){
    res_values.push_back(col.GetExpr()->Evaluate(&tuple, &table_metadata_ptr_->schema_));
  }
  return {res_values, GetOutputSchema()};
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (table_iter_ != table_heap_ptr_->End()){
    *tuple = *(table_iter_++);
    *rid = tuple->GetRid();
    if ((plan_->GetPredicate() == nullptr) ||
        plan_->GetPredicate()->Evaluate(tuple, plan_->OutputSchema()).GetAs<bool>()) {
      *tuple = GenerateTuple(*tuple);
      return true;
    }
  }
  return false;
}

}  // namespace bustub
