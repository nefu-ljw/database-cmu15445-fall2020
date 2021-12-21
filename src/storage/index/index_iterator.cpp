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
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, Page *page, int index)
    : buffer_pool_manager_(bpm), page_(page), index_(index) {
  leaf_ = reinterpret_cast<LeafPage *>(page_->GetData());
  // LOG_INFO("ENTER IndexIterator()");
  // LOG_INFO("LEAVE IndexIterator()");
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  // LOG_INFO("ENTER ~IndexIterator()");
  // assert(leaf_ != nullptr);
  // assert(leaf_->GetPageId());
  page_->RUnlatch();
  buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
  // LOG_INFO("LEAVE ~IndexIterator()");
}  // 记得unpin leaf

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() { return leaf_->GetNextPageId() == INVALID_PAGE_ID && index_ == leaf_->GetSize(); }

// 取出leaf中的array[index]，为pair类型
INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() { return leaf_->GetItem(index_); }

/**
 * index++，如果增加到当前leaf node末尾，则进入next leaf node且index置0
 * @return IndexIterator<KeyType, ValueType, KeyComparator>
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  // 若index加1后指向当前leaf末尾（但不是整个叶子层的末尾），则进入下一个leaf且index置0
  index_++;
  if (index_ == leaf_->GetSize() && leaf_->GetNextPageId() != INVALID_PAGE_ID) {
    Page *next_page = buffer_pool_manager_->FetchPage(leaf_->GetNextPageId());  // pin next leaf page
    next_page->RLatch();

    page_->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);  // unpin current leaf page

    page_ = next_page;
    leaf_ = reinterpret_cast<LeafPage *>(page_->GetData());  // update leaf page to next page
    index_ = 0;                                              // reset index to zero
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
  return leaf_->GetPageId() == itr.leaf_->GetPageId() && index_ == itr.index_;  // leaf page和index均相同
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const { return !(*this == itr); }  // 此处可用之前重载的==

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
