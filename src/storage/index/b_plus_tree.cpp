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
  // 1 先找到leaf page，这里面会调用fetch page
  Page *page = FindLeafPage(key, false);
  // 2 在leaf page里找这个key
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());  // 记得加上GetData()
  ValueType value;
  bool is_exist = leaf_page->Lookup(key, &value, comparator_);
  // 3 page用完后记得unpin page
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  if (is_exist) {
    result->push_back(value);  // 将得到的value添加到result中
  }
  return is_exist;  // 返回leaf page中key是否存在
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
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }
  // insert and return the key exist or not
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
  page_id_t page_id = INVALID_PAGE_ID;
  Page *root_page = buffer_pool_manager_->NewPage(&page_id);  // 注意new page的pin_count=1，之后记得unpin page
  if (nullptr == root_page) {
    throw std::runtime_error("out of memory");
  }
  // 2 page id赋值给root page id，并插入header page的root page id
  root_page_id_ = page_id;
  UpdateRootPageId(1);  // insert root page id in header page
  // 3 使用leaf page的Insert函数插入(key,value)
  LeafPage *root_node = reinterpret_cast<LeafPage *>(root_page->GetData());  // 记得加上GetData()
  root_node->Init(root_page_id_);                                            // 记得初始化
  root_node->Insert(key, value, comparator_);
  // 4 unpin root page
  buffer_pool_manager_->UnpinPage(root_page_id_, false);
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
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page);
  ValueType lookup_value;  // not used
  bool is_exist = leaf_node->Lookup(key, &lookup_value, comparator_);
  if (is_exist) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);  // unpin leaf page
    return false;
  }
  // 2 the key not exist, so we can insert (key,value) to leaf node
  int new_size = leaf_node->Insert(key, value, comparator_);
  if (new_size > leaf_node->GetMaxSize()) {
    LeafPage *new_leaf_node = Split(leaf_node);  // pin new leaf node
    InsertIntoParent(leaf_node, new_leaf_node->KeyAt(0), new_leaf_node,
                     transaction);                                      // NOTE: insert new leaf node to parent!
    buffer_pool_manager_->UnpinPage(new_leaf_node->GetPageId(), true);  // unpin new leaf node
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);  // unpin leaf page
  return true;
}

/*
 * 将传入的一个node拆分(Split)成两个结点，会产生一个新结点
 * 注意要区分叶子结点和内部结点
 * 如果node为internal page，则产生的新结点作为其孩子结点（疑问：？）
 * 如果node为leaf page，则产生的新结点要连接原结点，即更新这两个结点的next page id
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
    new_leaf_node->Init(new_page_id);
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
    new_internal_node->Init(new_page_id);
    // old_internal_node右半部分 移动至 new_internal_node
    // new_node（原old_node的右半部分）的所有孩子结点的父指针更新为指向new_node
    old_internal_node->MoveHalfTo(new_internal_node, buffer_pool_manager_);
    new_node = reinterpret_cast<N *>(new_internal_node);
  }
  return new_node;  // 注意，此时new_node还没有unpin
}

/*
 * 拆分(Split)后，向上找到old_node的父结点
 * 将new_node的第一个key插入到父结点，其位置在 父结点指向old_node的孩子指针 之后
 * 如果插入后>maxsize，则必须继续拆分父结点，然后在其父结点的父结点再插入，即需要递归
 * 直到找到的old_node为根结点时，结束递归（此时将会新建一个根R，关键字为key，old_node和new_node为其孩子）
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
  // 1 old_node是根结点
  // 创建一个新结点R当作根结点，其关键字为key。修改old_node和new_node的父指针，以及根结点R的孩子指针
  if (old_node->IsRootPage()) {  // old node为根结点
    page_id_t new_page_id = INVALID_PAGE_ID;
    Page *new_page = buffer_pool_manager_->FetchPage(new_page_id);
    assert(new_page != nullptr);
    root_page_id_ = new_page_id;
    UpdateRootPageId(0);  // update root page id in header page
    InternalPage *new_root_node = reinterpret_cast<InternalPage *>(new_page->GetData());
    new_root_node->Init(new_page_id);
    // 修改新的根结点的孩子指针，即array[0].second指向old_node，array[1].second指向new_node；对于array[1].first则赋值为key
    new_root_node->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    // 修改old_node和new_node的父指针
    old_node->SetParentPageId(new_page_id);
    new_node->SetParentPageId(new_page_id);
    buffer_pool_manager_->UnpinPage(new_page_id, true);  // 修改了new_page->data，所以dirty置为true
    return;                                              // 结束递归
  }
  // 2 old_node不是根结点
  // 找到old_node的父结点进行操作
  // a. 先直接插入(key,new_node->page_id)到父结点
  // b. 如果插入后父结点满了，则需要对父结点在进行拆分(Split)，并继续递归
  Page *parent_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());  // pin parent page
  assert(parent_page != nullptr);
  InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  // 将(key,new_node->page_id)插入到父结点中 value==old_node->page_id 的下标之后
  parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());  // size+1
  // 这时父结点如果满了，则需要先拆分(Split)，再递归InsertIntoParent
  if (parent_node->GetSize() > parent_node->GetMaxSize()) {  // 父结点已满(注意，之前的insert使得size+1)，需要拆分
    // parent_node拆分成两个，分别是parent_node和new_parent_node
    InternalPage *new_parent_node = Split(parent_node);  // pin new parent node
    // 继续递归，下一层递归是将拆分后新结点new_parent_node的第一个key插入到parent_node的父结点
    InsertIntoParent(parent_node, new_parent_node->KeyAt(0), new_parent_node, transaction);
    buffer_pool_manager_->UnpinPage(new_parent_node->GetPageId(), true);  // unpin new parent node
  }
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);  // unpin parent page
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
  page_id_t now_page_id = root_page_id_;  // initial now page id
  page_id_t next_page_id = INVALID_PAGE_ID;
  while (true) {
    Page *now_page = buffer_pool_manager_->FetchPage(now_page_id);  // pin now page
    // 注意此处Page到BPlusTreePage的转换，一定要加上GetData()！
    BPlusTreePage *tree_node = reinterpret_cast<BPlusTreePage *>(now_page->GetData());  // transform page to tree node
    if (tree_node->IsLeafPage()) {  // end while and return leaf page
      return now_page;              // 注意，此时还有一个fetch page没有unpin
    }
    // 注意此处BPlusTreePage到InternalPage的转换，不用加GetData()
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(tree_node);  // transform tree node to internal node
    // get next page id
    if (leftMost) {
      next_page_id = internal_node->ValueAt(0);  // ValueType被指定为page_id_t类型
    } else {
      next_page_id = internal_node->Lookup(key, comparator_);
    }
    buffer_pool_manager_->UnpinPage(now_page_id, false);  // unpin now page
    now_page_id = next_page_id;                           // update now page id to next page id
  }
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
