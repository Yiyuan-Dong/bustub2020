//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 * Done!
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 *
 * DYY:
 * Remember to unpin the page!!!
 * Get a vector, well, maybe I hope I could append the result at the back?
 * Done... Maybe?
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  if (IsEmpty()){
    return false;
  }

  Page* page_ptr = FindLeafPage(key, 0);
  page_id_t page_id = page_ptr->GetPageId();
  IN_TREE_LEAF_PAGE_TYPE* leaf_page_ptr
      = reinterpret_cast<IN_TREE_LEAF_PAGE_TYPE*>(page_ptr->GetData());

  RID temp_rid;
  bool ret = leaf_page_ptr->Lookup(key, &temp_rid, comparator_);

  buffer_pool_manager_->UnpinPage(page_id, false);

  if (!ret){
    return false;
  }

  result->push_back(temp_rid);

  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  if (IsEmpty()){
    StartNewTree(key, value);
    return true;
  }

  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t root_page_id;
  Page* root_page_ptr = buffer_pool_manager_->NewPage(&root_page_id);
  if (root_page_ptr == nullptr){
    throw Exception(ExceptionType::OUT_OF_MEMORY,
                    "In func `StartNewTree`, buffer pool run out of memory");
  }

  root_page_id_ = root_page_id;
  UpdateRootPageId(1);

  IN_TREE_LEAF_PAGE_TYPE* leaf_ptr =
      reinterpret_cast<IN_TREE_LEAF_PAGE_TYPE*>(root_page_ptr->GetData());
  leaf_ptr->Init(root_page_id, INVALID_PAGE_ID);

  leaf_ptr->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(root_page_id, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 *
 * UNPIN!
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  Page* page_ptr = FindLeafPage(key);
  if (page_ptr == nullptr){
    return false;
  }
  IN_TREE_LEAF_PAGE_TYPE* leaf_page_ptr =
      reinterpret_cast<IN_TREE_LEAF_PAGE_TYPE*>(page_ptr->GetData());
  page_id_t page_id = page_ptr->GetPageId();

  if (leaf_page_ptr->CheckDuplicated(key, comparator_)){
    return false;
  }

  int size = leaf_page_ptr->Insert(key, value, comparator_);
  if (size > leaf_page_ptr->GetMaxSize()){

    IN_TREE_LEAF_PAGE_TYPE* new_leaf_page_ptr = Split<IN_TREE_LEAF_PAGE_TYPE>(leaf_page_ptr);

    leaf_page_ptr->MoveHalfTo(new_leaf_page_ptr);
    new_leaf_page_ptr->SetNextPageId(leaf_page_ptr->GetNextPageId());
    leaf_page_ptr->SetNextPageId(new_leaf_page_ptr->GetPageId());

    InsertIntoParent(leaf_page_ptr, new_leaf_page_ptr->KeyAt(0),
                     new_leaf_page_ptr, transaction);

    buffer_pool_manager_->UnpinPage(new_leaf_page_ptr->GetPageId(), true);
  }

  buffer_pool_manager_->UnpinPage(page_id, true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 *
 * Dyy:
 * I **DO NOT** call `MoveHalfTo` here since internal page and leaf page have different
 * function interface
 * Just set `page_id` and `parent_page_id`
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t new_page_id;
  Page* new_page_ptr = buffer_pool_manager_->NewPage(&new_page_id);
  if (new_page_ptr == nullptr){
    throw Exception(ExceptionType::OUT_OF_MEMORY,
                    "In func `Split`: buffer pool run out of memory");
  }

  N* type_n_page_ptr = reinterpret_cast<N*>(new_page_ptr->GetData());
  type_n_page_ptr->Init(new_page_id, node->GetParentPageId());

  return type_n_page_ptr;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  page_id_t parent_page_id = old_node->GetParentPageId();

  Page *page_ptr = SafelyGetFrame(parent_page_id,
                                  "Out of memory in `InsertIntoParent` !");
  IN_TREE_INTERNAL_PAGE_TYPE *parent_page_ptr =
      reinterpret_cast<IN_TREE_INTERNAL_PAGE_TYPE*>(page_ptr->GetData());

  int new_size = parent_page_ptr->InsertNodeAfter(old_node->GetPageId(),
                                                  key,
                                                  new_node->GetPageId());

  if (new_size > parent_page_ptr->GetMaxSize()){
    IN_TREE_INTERNAL_PAGE_TYPE* new_parent_page_ptr = Split<IN_TREE_INTERNAL_PAGE_TYPE>(parent_page_ptr);
    parent_page_ptr->MoveHalfTo(new_parent_page_ptr, buffer_pool_manager_);

    if (parent_page_ptr->IsRootPage()){
      page_id_t new_root_page_id;
      Page* page_ptr_root;
      page_ptr_root = SafelyNewPage(&new_root_page_id,
                                    "Out of memory in `InsertIntoParent`");
      IN_TREE_INTERNAL_PAGE_TYPE *root_page_ptr =
        reinterpret_cast<IN_TREE_INTERNAL_PAGE_TYPE*>(page_ptr_root->GetData());
      root_page_ptr->Init(root_page_id_);

      parent_page_ptr->SetParentPageId(new_root_page_id);
      new_parent_page_ptr->SetParentPageId(new_root_page_id);
      root_page_ptr->PopulateNewRoot(parent_page_id,
                                     new_parent_page_ptr->KeyAt(0),
                                     new_parent_page_ptr->GetPageId());

      SetRootPageId(new_root_page_id);
      UpdateRootPageId(0);
      buffer_pool_manager_->UnpinPage(new_root_page_id, true);
    }
    else{
      InsertIntoParent(parent_page_ptr,
                       new_parent_page_ptr->KeyAt(0),
                       new_parent_page_ptr,
                       transaction);
    }

    buffer_pool_manager_->UnpinPage(new_parent_page_ptr->GetPageId(), true);
  }

  buffer_pool_manager_->UnpinPage(parent_page_id, true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) { return false; }

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { return INDEXITERATOR_TYPE(); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
Page* BPLUSTREE_TYPE::SafelyGetFrame(page_id_t page_id, const std::string &logout_string) {
  Page *page_ptr = buffer_pool_manager_->FetchPage(page_id);
  if (page_ptr == nullptr){
    throw Exception(ExceptionType::OUT_OF_MEMORY, logout_string);
  }
  return page_ptr;
}

INDEX_TEMPLATE_ARGUMENTS
Page* BPLUSTREE_TYPE::SafelyNewPage(page_id_t *page_id, const std::string &logout_string) {
  Page* new_page_ptr = buffer_pool_manager_->NewPage(page_id);
  if (new_page_ptr == nullptr){
    throw Exception(ExceptionType::OUT_OF_MEMORY,logout_string);
  }
  return new_page_ptr;
}
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {

  Page* page_ptr = buffer_pool_manager_->FetchPage(root_page_id_);
  page_id_t page_id = root_page_id_;
  page_id_t next_page_id;
  BPlusTreePage* tree_page = reinterpret_cast<BPlusTreePage*>(page_ptr->GetData());

  while (!tree_page->IsLeafPage()){
    IN_TREE_INTERNAL_PAGE_TYPE* internal_page
    = reinterpret_cast<IN_TREE_INTERNAL_PAGE_TYPE*>(tree_page);

    if (leftMost){
      next_page_id = internal_page->ValueAt(0);
    }
    else{
      next_page_id = internal_page->Lookup(key, comparator_);
    }

    buffer_pool_manager_->UnpinPage(page_id, false);

    if (next_page_id == INVALID_PAGE_ID){
      return nullptr;
    }
    page_ptr = buffer_pool_manager_->FetchPage(next_page_id);
    page_id = next_page_id;
    tree_page = reinterpret_cast<BPlusTreePage*>(page_ptr->GetData());
  }

  return page_ptr;
}


INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SetRootPageId(int root_page_id) {
  root_page_id_ = root_page_id;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
