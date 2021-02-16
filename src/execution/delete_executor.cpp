//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)){}

void DeleteExecutor::Init() {
  Catalog *catalog = GetExecutorContext()->GetCatalog();
  table_info_ = catalog->GetTable(plan_->TableOid());
  const std::string &table_name = table_info_->name_;
  index_info_vector_ = catalog->GetTableIndexes(table_name);
  child_executor_->Init();
}

void DeleteExecutor::DeleteTuple(Tuple &tuple, RID &rid) {
  // Mark delete in table heap
  TableHeap *table_heap_ptr = table_info_->table_.get();
  table_heap_ptr->MarkDelete(rid, GetExecutorContext()->GetTransaction());

  // I think I should also delete tuples from indexes
  // If an index keeps a deleted RID, when try to fetch this tuple, the transaction
  // will abort!
  for (auto &index_info : index_info_vector_){
    B_PLUS_TREE_INDEX_TYPE *b_plus_tree_index
        = reinterpret_cast<B_PLUS_TREE_INDEX_TYPE*>(index_info->index_.get());
    b_plus_tree_index->DeleteEntry(tuple.KeyFromTuple(table_info_->schema_,
                                                      index_info->key_schema_,
                                                      index_info->index_->GetMetadata()->GetKeyAttrs()),
                                   rid, GetExecutorContext()->GetTransaction());
  }
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (!child_executor_->Next(tuple, rid)){
    return false;
  }

  DeleteTuple(*tuple, *rid);
  return true;
}

}  // namespace bustub
