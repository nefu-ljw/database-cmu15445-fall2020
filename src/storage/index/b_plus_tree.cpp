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
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH 最终要实现的目标函数之一
 *****************************************************************************/
/*
 * project2 检查点1
 * B+树的点查询。《数据库系统概念》P431有伪代码
 * 功能：查询key在对应leaf page中的value，并将value存入result
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  // std::scoped_lock lock{latch_};  // DEBUG
  // LOG_INFO("ENTER GetValue");  // DEBUG
  // 1 先找到leaf page，这里面会调用fetch page
  Page *leaf_page = FindLeafPage(key, false);  // pin leaf page
  assert(leaf_page != nullptr);
  // 2 在leaf page里找这个key
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());  // 记得加上GetData()
  assert(leaf_node != nullptr);
  ValueType value{};
  bool is_exist = leaf_node->Lookup(key, &value, comparator_);
  // 3 page用完后记得unpin page（疑问：unpin这句话一定要放在这里吗？也就是用完之后马上unpin？）
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);  // unpin leaf page
  if (is_exist) {
    result->push_back(value);  // 将得到的value添加到result中
  }
  assert(is_exist == true);  // DEBUG
  return is_exist;           // 返回leaf page中key是否存在
}

/*****************************************************************************
 * INSERTION 最终要实现的目标函数之一
 *****************************************************************************/
/*
 * project2 检查点1
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  // std::scoped_lock lock{latch_};  // DEBUG
  // LOG_INFO("ENTER Insert");  // DEBUG
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }
  // insert key into correct leaf node and return the key exist or not
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * 创建新树，即创建root page
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  // 1 缓冲池申请一个new page，作为root page
  page_id_t new_page_id = INVALID_PAGE_ID;
  Page *root_page = buffer_pool_manager_->NewPage(&new_page_id);  // 注意new page的pin_count=1，之后记得unpin page
  if (nullptr == root_page) {
    throw std::runtime_error("out of memory");
  }
  // 2 page id赋值给root page id，并插入header page的root page id
  root_page_id_ = new_page_id;
  UpdateRootPageId(1);  // insert root page id in header page
  // 3 使用leaf page的Insert函数插入(key,value)
  LeafPage *root_node = reinterpret_cast<LeafPage *>(root_page->GetData());  // 记得加上GetData()
  root_node->Init(new_page_id, INVALID_PAGE_ID, leaf_max_size_);             // 记得初始化为leaf_max_size
  root_node->Insert(key, value, comparator_);
  // 4 unpin root page
  assert(root_page->GetPinCount() != 0);                          // Debug UnpinPage
  buffer_pool_manager_->UnpinPage(root_page->GetPageId(), true);  // 注意：这里dirty要置为true！
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  // 1 find the leaf page as insertion target
  Page *leaf_page = FindLeafPage(key, false);  // pin leaf page
  assert(leaf_page != nullptr);
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());  // 注意，记得加上GetData()
  assert(leaf_node != nullptr);
  ValueType lookup_value{};  // not used
  bool is_exist = leaf_node->Lookup(key, &lookup_value, comparator_);
  if (is_exist) {
    assert(leaf_page->GetPinCount() != 0);                           // Debug UnpinPage
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);  // unpin leaf page
    // LOG_INFO("InsertIntoLeaf duplicate keys!");
    return false;
  }
  // 2 the key not exist, so we can insert (key,value) to leaf node
  int new_size = leaf_node->Insert(key, value, comparator_);
  // 疑问：加上等于号试试
  if (new_size >= leaf_node->GetMaxSize()) {
    LeafPage *new_leaf_node = Split(leaf_node);  // pin new leaf node
    InsertIntoParent(leaf_node, new_leaf_node->KeyAt(0), new_leaf_node,
                     transaction);  // NOTE: insert new leaf node to parent!
    // buffer_pool_manager_->UnpinPage(new_leaf_node->GetPageId(), true);  // DEBUG: unpin new leaf node
    // 疑问：这里别人似乎没有unpin？？？(我觉得必须unpin，InsertIntoParent函数里面并不会unpin old node和new node)
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);  // unpin leaf page
  return true;
}

/*
 * 将传入的一个node拆分(Split)成两个结点，会产生一个新结点new node
 * 首先初始化新结点的parent id和max size
 * 接下来注意分情况讨论node是叶结点还是内部结点
 * 如果node为internal page，则产生的新结点的孩子结点的父指针要更新为新结点
 * 如果node为leaf page，则产生的新结点要连接原结点，即更新这两个结点的next page id
 * 注意：本函数执行完毕后，原node和new node都需要在函数外面进行unpin
 * （此处看具体实现方式，也可将new node在函数内部进行unpin）
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  // 1 缓冲池申请一个new page
  page_id_t new_page_id = INVALID_PAGE_ID;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);  // 注意new page的pin_count=1，之后记得unpin page
  if (nullptr == new_page) {
    throw std::runtime_error("out of memory");
  }
  // 2 分情况进行拆分
  N *new_node = reinterpret_cast<N *>(new_page->GetData());  // 记得加上GetData()
  if (node->IsLeafPage()) {                                  // leaf page
    LeafPage *old_leaf_node = reinterpret_cast<LeafPage *>(node);
    LeafPage *new_leaf_node = reinterpret_cast<LeafPage *>(new_node);
    new_leaf_node->Init(new_page_id, node->GetParentPageId(), leaf_max_size_);  // 注意初始化parent id和max_size
    // old_leaf_node右半部分 移动至 new_leaf_node
    old_leaf_node->MoveHalfTo(new_leaf_node);
    // 更新叶子层的链表，示意如下：
    // 原来：old node ---> next node
    // 最新：old node ---> new node ---> next node
    new_leaf_node->SetNextPageId(old_leaf_node->GetNextPageId());  // 完成连接new node ---> next node
    old_leaf_node->SetNextPageId(new_leaf_node->GetPageId());      // 完成连接old node ---> new node
    new_node = reinterpret_cast<N *>(new_leaf_node);
  } else {  // internal page
    InternalPage *old_internal_node = reinterpret_cast<InternalPage *>(node);
    InternalPage *new_internal_node = reinterpret_cast<InternalPage *>(new_node);
    new_internal_node->Init(new_page_id, node->GetParentPageId(), internal_max_size_);  // 注意初始化parent id和max_size
    // old_internal_node右半部分 移动至 new_internal_node
    // new_node（原old_node的右半部分）的所有孩子结点的父指针更新为指向new_node
    old_internal_node->MoveHalfTo(new_internal_node, buffer_pool_manager_);
    new_node = reinterpret_cast<N *>(new_internal_node);
  }
  // fetch page and new page need to unpin page (do it outside)
  return new_node;  // 注意，此时new_node还没有unpin
}

/*
 * 拆分(Split)后，向上找到old_node的父结点
 * 将new_node的第一个key插入到父结点，其位置在 父结点指向old_node的孩子指针 之后
 * 如果插入后>maxsize，则必须继续拆分父结点，然后在其父结点的父结点再插入，即需要递归
 * 直到找到的old_node为根结点时，结束递归（此时将会新建一个根R，关键字为key，old_node和new_node为其孩子）
 * 注意：本函数执行完毕后，new node和old node都需要在函数外面进行unpin
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
  // 1 old_node是根结点，那么整棵树直接升高一层
  // 具体操作是创建一个新结点R当作根结点，其关键字为key，左右孩子结点分别为old_node和new_node
  if (old_node->IsRootPage()) {  // old node为根结点
    page_id_t new_page_id = INVALID_PAGE_ID;
    Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);  // 这里应该是NewPage，不是FetchPage！
    assert(new_page != nullptr);
    assert(new_page->GetPinCount() == 1);
    root_page_id_ = new_page_id;
    UpdateRootPageId(0);  // update root page id in header page
    InternalPage *new_root_node = reinterpret_cast<InternalPage *>(new_page->GetData());
    new_root_node->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);  // 注意初始化parent page id和max_size
    // 修改新的根结点的孩子指针，即array[0].second指向old_node，array[1].second指向new_node；对于array[1].first则赋值为key
    new_root_node->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    // 修改old_node和new_node的父指针
    old_node->SetParentPageId(new_page_id);
    new_node->SetParentPageId(new_page_id);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);  // DEBUG
    buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);  // 修改了new_page->data，所以dirty置为true
    // LOG_INFO("InsertIntoParent old node is root: completed");
    return;  // 结束递归
  }
  // 2 old_node不是根结点
  // 找到old_node的父结点进行操作
  // a. 先直接插入(key,new_node->page_id)到父结点
  // b. 如果插入后父结点满了，则需要对父结点再进行拆分(Split)，并继续递归
  buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);                      // DEBUG
  Page *parent_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());  // pin parent page
  assert(parent_page != nullptr);
  InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  // 将(key,new_node->page_id)插入到父结点中 value==old_node->page_id 的下标之后
  parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());  // size+1
  // 这时父结点如果满了，则需要先拆分(Split)，再递归InsertIntoParent
  // 疑问：这里加上等于号试试？
  if (parent_node->GetSize() >= parent_node->GetMaxSize()) {  // 父结点已满(注意，之前的insert使得size+1)，需要拆分
    // parent_node拆分成两个，分别是parent_node和new_parent_node
    InternalPage *new_parent_node = Split(parent_node);  // pin new parent node
    // buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);  // DEBUG: unpin new parent node
    // 继续递归，下一层递归是将拆分后新结点new_parent_node的第一个key插入到parent_node的父结点
    InsertIntoParent(parent_node, new_parent_node->KeyAt(0), new_parent_node, transaction);
    // buffer_pool_manager_->UnpinPage(new_parent_node->GetPageId(), true);  // unpin new parent node
  }
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);  // unpin parent page
}

/*****************************************************************************
 * REMOVE 最终要实现的目标函数之一
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  std::scoped_lock lock{latch_};  // DEBUG
  if (IsEmpty()) {
    return;
  }
  // find the leaf page as deletion target
  Page *leaf_page = FindLeafPage(key, false);  // pin leaf page
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int old_size = leaf_node->GetSize();
  int new_size = leaf_node->RemoveAndDeleteRecord(key, comparator_);  // 在leaf中删除key（如果不存在该key，则size不变）
  if (new_size < old_size) {
    // 删除成功，然后调用CoalesceOrRedistribute
    CoalesceOrRedistribute(leaf_node, transaction);
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);  // unpin leaf page
  } else {
    // 删除失败，直接unpin即可
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);  // unpin leaf page
  }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size >= page's max size, then redistribute. Otherwise, merge(Coalesce).
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  if (node->IsRootPage()) {
    return AdjustRoot(node);  // NOTE: size of root page can be less than min size
  }

  // 不需要合并或者重分配，直接返回false
  if (node->GetSize() >= node->GetMinSize()) {
    return false;
  }

  // 需要合并或者重分配
  // 先获取node的parent page
  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());

  // 获得node在parent的孩子指针(value)的index
  int index = parent->ValueIndex(node->GetPageId());
  // 寻找兄弟结点，尽量找到前一个结点(前驱结点)
  page_id_t sibling_page_id = parent->ValueAt(index == 0 ? 1 : index - 1);
  Page *sibling_page = buffer_pool_manager_->FetchPage(sibling_page_id);
  N *sibling_node = reinterpret_cast<N *>(sibling_page->GetData());

  bool ret;  // 函数返回值
  if (node->GetSize() + sibling_node->GetSize() >= node->GetMaxSize()) {
    // 1 Redistribute 当kv总和能支撑两个Node，那么重新分配即可，不必删除node
    Redistribute(sibling_node, node, index);  // 无返回值
    ret = false;                              // node不必被删除
  } else {
    // 2 Coalesce 当sibling和node只能凑成一个Node，那么合并两个结点到sibling，删除node
    // Coalesce函数中会将node删除，并可能继续递归调用CoalesceOrRedistribute
    Coalesce(&sibling_node, &node, &parent, index, transaction);  // 返回值是parent是否需要被删除
    ret = true;                                                   // node需要被删除
  }

  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
  return ret;
}

/*
 * 合并(Coalesce)函数是和直接前驱进行合并，也就是和它左边的node进行合并
 * neighbor_node是node的前结点，node结点是需要被删除的
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
  // Assume that *neighbor_node is the left sibling of *node (neighbor -> node)
  // index表示node在parent中的孩子指针(value)的下标
  // key_index表示 交换后的 node在parent中的孩子指针(value)的下标
  // 若index=0，说明node为neighbor前驱，要保证neighbor为node的前驱，则交换变量neighbor和node，且key_index=1
  int key_index = index;
  if (index == 0) {
    std::swap(neighbor_node, node);  // 保证neighbor_node为node的前驱
    key_index = 1;
  }
  KeyType middle_key = (*parent)->KeyAt(key_index);  // middle_key only used in internal_node->MoveAllTo

  // Move items from node to neighbor_node
  if ((*node)->IsLeafPage()) {
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(*node);
    LeafPage *neighbor_leaf_node = reinterpret_cast<LeafPage *>(*neighbor_node);
    leaf_node->MoveAllTo(neighbor_leaf_node);
    neighbor_leaf_node->SetNextPageId(leaf_node->GetNextPageId());
    LOG_INFO("Coalesce leaf, index=%d, pid=%d neighbor->node", index, (*node)->GetPageId());
  } else {
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(*node);
    InternalPage *neighbor_internal_node = reinterpret_cast<InternalPage *>(*neighbor_node);
    // MoveAllTo do this: set node's first key to middle_key and move node to neighbor
    internal_node->MoveAllTo(neighbor_internal_node, middle_key, buffer_pool_manager_);
    LOG_INFO("Coalesce internal, index=%d, pid=%d neighbor->node", index, (*node)->GetPageId());
  }

  // 在缓冲池中删除node
  buffer_pool_manager_->UnpinPage((*node)->GetPageId(), true);
  buffer_pool_manager_->DeletePage((*node)->GetPageId());

  // 删除node在parent中的kv信息
  (*parent)->Remove(key_index);  // 注意，是key_index，不是index

  // 因为parent中删除了kv对，所以递归调用CoalesceOrRedistribute函数判断parent结点是否需要被删除
  return CoalesceOrRedistribute(*parent, transaction);
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
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());  // parent of node

  // node是之前刚被删除过一个key的结点
  // index=0，则neighbor是node后继结点，表示：node(left)      neighbor(right)
  // index>0，则neighbor是node前驱结点，表示：neighbor(left)  node(right)
  // 注意更新parent结点的相关kv对

  if (node->IsLeafPage()) {
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(node);
    LeafPage *neighbor_leaf_node = reinterpret_cast<LeafPage *>(neighbor_node);
    if (index == 0) {  // node -> neighbor
      LOG_INFO("Redistribute leaf, index=0, pid=%d node->neighbor", node->GetPageId());
      // move neighbor's first to node's end
      neighbor_leaf_node->MoveFirstToEndOf(leaf_node);
      parent->SetKeyAt(1, neighbor_leaf_node->KeyAt(0));
    } else {  // neighbor -> node
      // move neighbor's last to node's front
      LOG_INFO("Redistribute leaf, index=%d, pid=%d neighbor->node", index, node->GetPageId());
      neighbor_leaf_node->MoveLastToFrontOf(leaf_node);
      parent->SetKeyAt(index, leaf_node->KeyAt(0));
    }
  } else {
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(node);
    InternalPage *neighbor_internal_node = reinterpret_cast<InternalPage *>(neighbor_node);
    if (index == 0) {  // case: node(left) and neighbor(right)
      LOG_INFO("Redistribute internal, index=0, pid=%d node->neighbor", node->GetPageId());
      // MoveFirstToEndOf do this:
      // 1 set neighbor's first key to parent's second key（详见MoveFirstToEndOf函数）
      // 2 move neighbor's first to node's end
      neighbor_internal_node->MoveFirstToEndOf(internal_node, parent->KeyAt(1), buffer_pool_manager_);
      // set parent's second key to neighbor's "new" first key
      parent->SetKeyAt(1, neighbor_internal_node->KeyAt(0));
    } else {  // case: neighbor(left) and node(right)
      LOG_INFO("Redistribute internal, index=%d, pid=%d neighbor->node", index, node->GetPageId());
      // MoveLastToFrontOf do this:
      // 1 set node's first key to parent's index key（详见MoveLastToFrontOf函数）
      // 2 move neighbor's last to node's front
      neighbor_internal_node->MoveLastToFrontOf(internal_node, parent->KeyAt(index), buffer_pool_manager_);
      // set parent's index key to node's "new" first key
      parent->SetKeyAt(index, internal_node->KeyAt(0));
    }
  }
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  // LOG_INFO("END redistribute");
}
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
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  // Case 1: old_root_node是内部结点，且大小为1。表示内部结点其实已经没有key了，所以要把它的孩子更新成新的根结点
  // old_root_node (internal node) has only one size
  if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {
    LOG_INFO("AdjustRoot: delete the last element in root page, but root page still has one last child");
    // get child page as new root page
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(old_root_node);
    page_id_t child_page_id = internal_node->RemoveAndReturnOnlyChild();

    // NOTE: don't need to unpin old_root_node, this operation will be done in CoalesceOrRedistribute function
    // buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), true);

    // update root page id
    root_page_id_ = child_page_id;
    UpdateRootPageId(0);
    // update parent page id of new root node
    Page *new_root_page = buffer_pool_manager_->FetchPage(root_page_id_);
    InternalPage *new_root_node = reinterpret_cast<InternalPage *>(new_root_page->GetData());
    new_root_node->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(new_root_page->GetPageId(), true);
    return true;
  }
  // Case 2: old_root_node是叶结点，且大小为0。直接更新root page id
  // all elements deleted from the B+ tree
  if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    LOG_INFO("AdjustRoot: all elements deleted from the B+ tree");
    // NOTE: don't need to unpin old_root_node, this operation will be done in Remove function
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);
    return true;
  }
  // 否则不需要有page被删除，直接返回false
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  // find leftmost leaf page
  KeyType key{};                              // not used
  Page *leaf_page = FindLeafPage(key, true);  // pin leftmost leaf page
  assert(leaf_page != nullptr);
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_node, 0);  // 最左边的叶子且index=0
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  // find leaf page that contains the input key
  Page *leaf_page = FindLeafPage(key, false);  // pin leaf page
  assert(leaf_page != nullptr);
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  assert(leaf_node != nullptr);
  int index = leaf_node->KeyIndex(key, comparator_);  // 此处直接用KeyIndex，而不是Lookup
  // LOG_INFO("Tree.Begin before return INDEX class, index=%d leaf page id=%d leaf node page id=%d", index,
  //          leaf_page->GetPageId(), leaf_node->GetPageId());
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_node, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() {
  // LOG_INFO("Enter tree.end()");
  // find leftmost leaf page
  KeyType key{};                              // not used
  Page *leaf_page = FindLeafPage(key, true);  // pin leftmost leaf page
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  assert(leaf_node != nullptr);
  // 从左向右开始遍历叶子层结点，直到最后一个
  while (leaf_node->GetNextPageId() != INVALID_PAGE_ID) {
    int next_page_id = leaf_node->GetNextPageId();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);        // unpin current leaf page
    Page *next_leaf_page = buffer_pool_manager_->FetchPage(next_page_id);  // pin next leaf page
    leaf_page = next_leaf_page;                                            // update current leaf page to next leaf page
    leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  }  // 结束循环时，leaf_node为叶子层的最后一个结点（rightmost leaf page）
  // 注意传入的index为leaf_node->GetSize()
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_node, leaf_node->GetSize());  // 注意：此时leaf_node没有unpin
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 * 注意，本函数结束时返回的leaf page会被pin，一定记得在函数外进行unpin
 * 从整个B+树的根结点开始，一直向下找到叶子结点
 * 因为B+树是多路搜索树，所以整个向下搜索就是通过key值进行比较
 * 其中内部结点向下搜索的过程中调用InternalPage的Lookup函数
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  // throw Exception(ExceptionType::NOT_IMPLEMENTED, "Implement this for test");
  if (IsEmpty()) {
    return nullptr;
  }
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);               // pin root page
  BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());  // transform page to tree node
  assert(node != nullptr);
  while (!node->IsLeafPage()) {
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(node);

    page_id_t child_page_id = INVALID_PAGE_ID;
    // get child page id
    if (leftMost) {
      child_page_id = internal_node->ValueAt(0);  // ValueType被指定为page_id_t类型
      assert(child_page_id != 0);                 // DEBUG
    } else {
      child_page_id = internal_node->Lookup(key, comparator_);
      assert(child_page_id != 0);  // DEBUG   这里有问题
    }

    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);          // unpin now page
    Page *child_page = buffer_pool_manager_->FetchPage(child_page_id);  // pin child page
    BPlusTreePage *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());

    // DEBUG
    // if (child_node->GetParentPageId() != node->GetPageId()) {
    //   LOG_INFO(
    //       "leftmost=%d child_page_id=%d node->GetPageId()=%d child_node->GetParentPageId()=%d "
    //       "child_node->GetPageId()=%d",
    //       leftMost, child_page_id, node->GetPageId(), child_node->GetParentPageId(), child_node->GetPageId());
    //   assert(child_node->GetParentPageId() == node->GetPageId());  // DEBUG
    // }

    node = child_node;  // update now node to child node
    page = child_page;  // update now page to child page
  }
  return page;
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
