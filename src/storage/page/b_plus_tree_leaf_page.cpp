//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * 叶页存储有序的m个键(key)条目和m个值(value)条目。
 * 值应该只是用于定位实际元组存储位置的64位record_id，请参阅src/include/common/rid.h中RID定义的类。
 * 叶页和内部页一样，对键/值对的数量有限制，应该遵循相同的合并、重新分配和拆分操作。
 * 重要提示：即使叶页面和内部页面包含相同类型的键，它们可能具有不同的值类型，因此叶页面和内部页面的max_size可能不同。
 * 每一个B+树的叶/内部页都对应着缓冲池取出的一个内存页的内容（即data_部分）。
 * 因此，每次尝试读取或写入叶/内部页面时，您都需要首先使用唯一的page_id从缓冲池中获取(fetch)页面，
 * 然后将其重新解释为叶或内部页面，并在任何写入或读取操作后取消固定(unpin)页面。
 */

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  // 缺省：page_id_t parent_id = INVALID_PAGE_ID, int max_size = LEAF_PAGE_SIZE;
  SetPageType(IndexPageType::LEAF_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetSize(0);                      // 最开始current size为0
  SetMaxSize(max_size);            // max_size=LEAF_PAGE_SIZE-1 这里也可以减1，方便后续的拆分(Split)函数
  SetNextPageId(INVALID_PAGE_ID);  // 疑问：是这样初始化吗？
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/**
 * 返回leaf page的array中第一个>=key的下标
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  // 这里手写二分查找lower_bound，速度快于for循环的顺序查找
  // array类型为std::pair<KeyType, ValueType>
  // 叶结点的下标范围是[0,size-1]
  int left = 0;
  int right = GetSize() - 1;
  while (left <= right) {
    int mid = left + (right - left) / 2;
    if (comparator(KeyAt(mid), key) >= 0) {  // 下标还需要减小
      right = mid - 1;
    } else {  // 下标还需要增大
      left = mid + 1;
    }
  }  // lower_bound
  int target_index = right + 1;
  return target_index;  // 返回array中第一个>=key的下标（如果key大于所有array，则找到下标为size）
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  return array[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
  return array[index];
}

/*****************************************************************************
 * INSERTION 将(key,value)插入到leaf page中，返回插入后的size
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  // 疑问：此处是否允许 > MaxSize ？？？
  // if (GetSize() == GetMaxSize()) {  // 边界
  //   throw std::runtime_error("out of memory");
  // }
  int insert_index = KeyIndex(key, comparator);  // 查找第一个>=key的的下标
  // 数组下标>=insert_index的元素整体后移1位
  // [insert_index, size - 1] --> [insert_index + 1, size]
  for (int i = GetSize(); i > insert_index; i--) {
    array[i] = array[i - 1];
  }
  array[insert_index] = std::make_pair(key, value);  // insert pair
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  int start_index = GetMinSize();  // (0,1,2) start index is 1; (0,1,2,3) start index is 2;
  int move_num = GetSize() - start_index;
  // 将this page的从array+start_index开始的move_num个元素复制到recipient的array
  recipient->CopyNFrom(array + start_index, move_num);  // this page array [start_index, size) copy to recipient page
  // NOTE: recipient page size has been updated in recipient->CopyNFrom
  IncreaseSize(-move_num);  // update this page size
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  std::copy(items, items + size, array + GetSize());  // [items,items+size)复制到该page的array最后一个之后的空间
  IncreaseSize(size);                                 // 复制后空间增大了size
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * 功能：得到leaf page中key对应的value（传出参数），返回key是否在leaf page中存在
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const {
  int target_index = KeyIndex(key, comparator);                                  // 查找第一个>=key的的下标
  if (target_index == GetSize() || comparator(KeyAt(target_index), key) != 0) {  // =key的下标不存在（只有>key的下标）
    return false;
  }
  *value = array[target_index].second;  // value是传出参数
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) { return 0; }

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {}

/*
 * Insert item at the front of my items. Move items accordingly.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
