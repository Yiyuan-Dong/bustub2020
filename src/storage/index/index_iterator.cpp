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
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t page_id, int index, BufferPoolManager* bfm_ptr)
    :page_id_(page_id), index_(index), buffer_pool_manager_(bfm_ptr){};

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() { return page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
  B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_ptr = SafelyGetLeafPage();
  assert(leaf_ptr != nullptr);
  return leaf_ptr->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_ptr = SafelyGetLeafPage();
  if (leaf_ptr == nullptr){
    return *this;
  }

  if (index_ == leaf_ptr->GetSize() - 1){
    page_id_ = leaf_ptr->GetNextPageId();
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
