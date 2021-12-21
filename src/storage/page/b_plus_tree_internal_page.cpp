//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * 内部页不存储任何实际数据，而是存储有序的m个键条目(key entries)和m+1个子指针(又名page_id)。
 * 由于指针的数量不等于键的数量，因此将第一个键设置为无效，查找方法应始终从第二个键开始。
 * 在任何时候，每个内部页面至少是半满的。在删除过程中，可以将两个半满的页面合并成一个合法的页面，也可以重新分配以避免合并，
 * 而在插入过程中，可以将一个完整的页面一分为二。
 * 总结：
 * 在插入删除的时候检查节点大小，如果溢出则进行拆分(Split)，不足则视兄弟节点情况进行合并或者重分配。
 * 在实现的时候注意维护好有序关系以及KV对应关系
 * 注意：
 * 内部页面的第一个key（即array[0]）是无效的，任何search/lookup都忽略第一个key
 */

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  // 缺省：page_id_t parent_id = INVALID_PAGE_ID, int max_size = INTERNAL_PAGE_SIZE);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetSize(0);            // 最开始current size为0
  SetMaxSize(max_size);  // max_size=INTERNAL_PAGE_SIZE-1 这里一定要减1，因为内部页面的第一个key是无效的
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array[index].first = key; }

/*
 * 找到value对应的下标
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  // 对于内部页面，key有序可以比较，但value无法比较，只能顺序查找
  for (int i = 0; i < GetSize(); i++) {  // 疑问：value应该是从0开始查找吧？key从1开始查找
    if (array[i].second == value) {
      return i;  // 找到相同value
    }
  }
  // 找不到value，直接返回-1
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const { return array[index].second; }

/*****************************************************************************
 * LOOKUP 查找key应该在哪个value指向的子树中
 *****************************************************************************/
/*
 * 查找internal page的array中第一个>key(注意不是>=)的下标，然后据其确定value
 * 注意：value指向的是子树，或者说指向的是当前内部结点的下一层某个结点
 * 假设下标为i的子树中的所有key为subtree(value(i))，下标为i的关键字为key(i)
 * 那么满足 key(i-1) <= subtree(value(i)) < key(i)
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  // 这里手写二分查找upper_bound，速度快于for循环的顺序查找
  // array类型为std::pair<KeyType, ValueType>
  // 正常来说下标范围是[0,size-1]，但是0位置设为无效
  // 所以直接从1位置开始，作为下界，下标范围是[1,size-1]
  // assert(GetSize() >= 1);  // 这里总是容易出现错误
  int left = 1;
  int right = GetSize() - 1;
  while (left <= right) {
    int mid = left + (right - left) / 2;
    if (comparator(KeyAt(mid), key) > 0) {  // 下标还需要减小
      right = mid - 1;
    } else {  // 下标还需要增大
      left = mid + 1;
    }
  }  // upper_bound
  int target_index = left;
  assert(target_index - 1 >= 0);
  // 注意，返回的value下标要减1，这样才能满足key(i-1) <= subtree(value(i)) < key(i)
  return ValueAt(target_index - 1);
  // int target_index = -1;
  // for (int i = 1; i < GetSize(); ++i) {
  //   // 找到第一个比key大的
  //   if (comparator(array[i].first, key) > 0) {
  //     // 返回key范围所属的page id
  //     target_index = i - 1;
  //     break;
  //   }
  // }
  // // 如果所有array比key都小，则返回最后一个
  // if (target_index == -1) {
  //   // LOG_INFO("Internal Lookup: index=%d GetSize()=%d", target_index, GetSize());
  //   target_index = GetSize() - 1;
  //   LOG_INFO("Internal Lookup: index=%d GetSize()=%d", target_index, GetSize());
  //   assert(target_index != -1);
  // }
  // LOG_INFO("Internal Lookup: index=%d GetSize()=%d", target_index, GetSize());
  // assert(target_index != -1);
  // return array[target_index].second;
}

/*****************************************************************************
 * INSERTION 将当前page重置为2个key+1个value（size=2），第1个关键字不管，其他按参数赋值
 *****************************************************************************/
/*
 * Populate(填充) new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  array[0].second = old_value;
  array[1].first = new_key;
  array[1].second = new_value;
  SetSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  // 疑问：此处是否允许 > MaxSize ？？？
  // if (GetSize() == GetMaxSize()) {  // 边界
  //   throw std::runtime_error("out of memory");
  // }
  int insert_index = ValueIndex(old_value);  // 得到 =old_value 的下标
  // assert(insert_index != -1);                // 下标存在
  insert_index++;  // 插入位置在 =old_value的下标 的后面一个
  // 数组下标>=insert_index的元素整体后移1位
  // [insert_index, size - 1] --> [insert_index + 1, size]
  for (int i = GetSize(); i > insert_index; i--) {
    array[i] = array[i - 1];
  }
  array[insert_index] = MappingType{new_key, new_value};  // insert pair
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 * 上层调用：
 * this page是old_node，recipient page是new_node
 * old_node的右半部分array复制给new_node
 * 并且，将new_node（原old_node的右半部分）的所有孩子结点的父指针更新为指向new_node
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  // 疑问：这里不用+1
  int start_index = GetMinSize();  // (0,1,2) start index is 1; (0,1,2,3) start index is 2;
  int move_num = GetSize() - start_index;
  // 将this page的从array+start_index开始的move_num个元素复制到recipient page的array尾部
  // NOTE：同时，将recipient page的array中每个value指向的孩子结点的父指针更新为recipient page id
  // this page array [start_index, size) copy to recipient page
  recipient->CopyNFrom(array + start_index, move_num, buffer_pool_manager);
  // NOTE: recipient page size has been updated in recipient->CopyNFrom
  IncreaseSize(-move_num);  // update this page size
}

/*
 * 从items指向的位置开始，复制size个，到当前调用该函数的page的array尾部（本函数由recipient page调用）
 * 并且，找到调用该函数的page的array中每个value指向的孩子结点，其父指针更新为调用该函数的page id
 * Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  // [items,items+size)复制到当前page的array最后一个之后的空间
  std::copy(items, items + size, array + GetSize());
  // 修改array中的value的parent page id，其中array范围为[GetSize(), GetSize() + size)
  for (int i = GetSize(); i < GetSize() + size; i++) {
    // ValueAt(i)得到的是array中的value指向的孩子结点的page id
    Page *child_page = buffer_pool_manager->FetchPage(ValueAt(i));
    BPlusTreePage *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());  // 记得加上GetData()
    // Since it is an internal page, the moved entry(page)'s parent needs to be updated
    child_node->SetParentPageId(GetPageId());  // 特别注意这里，别写成child_page->GetPageId()
    // 注意，UnpinPage的dirty参数要为true，因为修改了page->data转为node后的ParentPageId，即修改了page->data
    buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
  }
  // 复制后空间增大了size
  IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  // delete array[index], move array after index to front by 1 size
  IncreaseSize(-1);
  for (int i = index; i < GetSize(); i++) {
    array[i] = array[i + 1];
  }
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  SetSize(0);
  return ValueAt(0);
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  // 当前node的第一个key(即array[0].first)本是无效值(因为是内部结点)，但由于要移动当前node的整个array到recipient
  // 那么必须在移动前将当前node的第一个key 赋值为 父结点中下标为index的middle_key
  SetKeyAt(0, middle_key);  // 将分隔key设置在0的位置
  recipient->CopyNFrom(array, GetSize(), buffer_pool_manager);
  // 对于内部结点的合并操作，要把需要删除的内部结点的叶子结点转移过去
  // recipient->SetKeyAt(GetSize(), middle_key);
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  // 当前node的第一个key本是无效值(因为是内部结点)，但由于要移动当前node的array[0]到recipient尾部
  // 那么必须在移动前将当前node的第一个key 赋值为 父结点中下标为1的middle_key
  SetKeyAt(0, middle_key);
  // first item (array[0]) of this page array copied to recipient page last
  recipient->CopyLastFrom(array[0], buffer_pool_manager);
  // delete array[0]
  Remove(0);  // 函数复用
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &item, BufferPoolManager *buffer_pool_manager) {
  array[GetSize()] = item;

  // update parent page id of child page
  Page *child_page = buffer_pool_manager->FetchPage(ValueAt(GetSize()));
  BPlusTreePage *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
  child_node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);

  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  // recipient的第一个key本是无效值(因为是内部结点)，但由于要移动当前node的array[GetSize()-1]到recipient首部
  // 那么必须在移动前将recipient的第一个key 赋值为 父结点中下标为index的middle_key
  recipient->SetKeyAt(0, middle_key);
  // last item (array[size-1]) of this page array inserted to recipient page first
  recipient->CopyFirstFrom(array[GetSize() - 1], buffer_pool_manager);
  // remove last item of this page
  IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &item, BufferPoolManager *buffer_pool_manager) {
  // move array after index=0 to back by 1 size
  for (int i = GetSize(); i >= 0; i--) {
    array[i + 1] = array[i];
  }
  // insert item to array[0]
  array[0] = item;

  // update parent page id of child page
  Page *child_page = buffer_pool_manager->FetchPage(ValueAt(0));
  BPlusTreePage *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
  child_node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);

  IncreaseSize(1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
