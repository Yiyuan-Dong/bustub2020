/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator()
    :page_id_(INVALID_PAGE_ID), index_(-1), buffer_pool_manager_(nullptr){};

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t page_id, int index, BufferPoolManager* bpm_ptr)
    :page_id_(page_id), index_(index), buffer_pool_manager_(bpm_ptr) {
  leaf_ptr_ = SafelyGetLeafPage();
};

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator(){
  if (page_id_ != INVALID_PAGE_ID){
    buffer_pool_manager_->UnpinPage(page_id_, false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() { return page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
  assert(leaf_ptr_ != nullptr);
  return leaf_ptr_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if (leaf_ptr_ == nullptr){
    return *this;
  }

  if (index_ == leaf_ptr_->GetSize() - 1){
    page_id_t old_page_id = page_id_;
    page_id_ = leaf_ptr_->GetNextPageId();
    buffer_pool_manager_->UnpinPage(old_page_id, false);
    leaf_ptr_ = SafelyGetLeafPage();
    index_ = 0;
  }
  else{
    index_++;
  }

  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE* INDEXITERATOR_TYPE::SafelyGetLeafPage()
{
  if (page_id_ == INVALID_PAGE_ID){
    return nullptr;
  }

  Page* page_ptr = buffer_pool_manager_->FetchPage(page_id_);
  if (page_ptr == nullptr){
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Out of memory in iterator");
  }

  return reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page_ptr->GetData());
}
template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
