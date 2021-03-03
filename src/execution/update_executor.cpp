//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
    Catalog *catalog = GetExecutorContext()->GetCatalog();
    table_info_ = catalog->GetTable(plan_->TableOid());
    const std::string &table_name = table_info_->name_;
    index_info_vector_ = catalog->GetTableIndexes(table_name);
    child_executor_->Init();
}

void UpdateExecutor::LockInNode(RID &rid) {
  Transaction* txn = GetExecutorContext()->GetTransaction();
  if (txn->GetSharedLockSet()->find(rid) != txn->GetSharedLockSet()->end()){
    GetExecutorContext()->GetLockManager()->LockUpgrade(txn, rid);
    return;
  }
  if (txn->GetExclusiveLockSet()->find(rid) == txn->GetExclusiveLockSet()->end()){
    GetExecutorContext()->GetLockManager()->LockExclusive(txn, rid);
  }
}

void UpdateExecutor::UpdateTuple(Tuple &tuple, RID &rid){
  Tuple updated_tuple{GenerateUpdatedTuple(tuple)};
  TableHeap* table_heap_ptr = table_info_->table_.get();
  // UpdateTuple will add the update record into txn write set
  table_heap_ptr->UpdateTuple(updated_tuple, rid, GetExecutorContext()->GetTransaction());
  for (auto &index_info:index_info_vector_){
      B_PLUS_TREE_INDEX_TYPE *b_plus_tree_index
              = reinterpret_cast<B_PLUS_TREE_INDEX_TYPE*>(index_info->index_.get());
    IndexWriteRecord index_record{rid,
                                  plan_->TableOid(),
                                  WType::UPDATE,
                                  tuple,
                                  index_info->index_oid_,
                                  GetExecutorContext()->GetCatalog()};
    GetExecutorContext()->GetTransaction()->AppendTableWriteRecord(index_record);
      // Update in index means delete and insert ?
      b_plus_tree_index->DeleteEntry(tuple.KeyFromTuple(table_info_->schema_,
                                                        index_info->key_schema_,
                                                        index_info->index_->GetMetadata()->GetKeyAttrs()),
                                     rid,
                                     GetExecutorContext()->GetTransaction());
      b_plus_tree_index->InsertEntry(updated_tuple.KeyFromTuple(table_info_->schema_,
                                                               index_info->key_schema_,
                                                               index_info->index_->GetMetadata()->GetKeyAttrs()),
                                     rid,
                                     GetExecutorContext()->GetTransaction());
  }
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (!child_executor_->Next(tuple, rid)){
    return false;
  }

  LockInNode(*rid);
  UpdateTuple(*tuple, *rid);
  return true;
}

}  // namespace bustub
