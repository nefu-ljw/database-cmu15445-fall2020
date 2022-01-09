//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <string>
#include <utility>  // for std::pair
#include <vector>

#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

enum class Operation { FIND = 0, INSERT, DELETE };  // 三种操作：查找、插入、删除

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  bool IsEmpty() const;

  // Insert a key-value pair into this B+ tree.
  bool Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr);

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // return the value associated with a given key
  bool GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr);

  // index iterator
  INDEXITERATOR_TYPE begin();
  INDEXITERATOR_TYPE Begin(const KeyType &key);
  INDEXITERATOR_TYPE end();

  void Print(BufferPoolManager *bpm) {
    ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
  }

  void Draw(BufferPoolManager *bpm, const std::string &outf) {
    std::ofstream out(outf);
    out << "digraph G {" << std::endl;
    ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
    out << "}" << std::endl;
    out.close();
  }

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);
  // expose for test purpose
  // Page *FindLeafPage(const KeyType &key, bool leftMost = false, Transaction *transaction = nullptr,
  //                    Operation op = Operation::FIND, bool *root_is_latched_ = nullptr, bool rightMost = false);

  Page *FindLeafPage(const KeyType &key, bool leftMost = false);

  std::pair<Page *, bool> FindLeafPageByOperation(const KeyType &key, Operation operation = Operation::FIND,
                                                  Transaction *transaction = nullptr, bool leftMost = false,
                                                  bool rightMost = false);

  // BufferPoolManager *getBPM() { return buffer_pool_manager_; }  // only for DEBUG

  // uint64_t getThreadId() {  // only for DEBUG
  //   // std::scoped_lock latch{latch_};

  //   std::stringstream ss;
  //   ss << std::this_thread::get_id();
  //   // ss << transaction->GetThreadId();
  //   uint64_t thread_id = std::stoull(ss.str());
  //   return thread_id % 13;
  //   // LOG_INFO("Thread=%lu", thread_id % 131);
  // }

  // int OpToString(Operation op) {  // only for debug
  //   std::string res;
  //   int d;
  //   if (op == Operation::FIND) {
  //     res = "FIND";
  //     d = 0;
  //   } else if (op == Operation::INSERT) {
  //     res = "INSERT";
  //     d = 1;
  //   } else if (op == Operation::DELETE) {
  //     res = "DELETE";
  //     d = 2;
  //   }
  //   // char *c_res = res.data();
  //   return d;
  // }

 private:
  void StartNewTree(const KeyType &key, const ValueType &value);

  bool InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr);

  void InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                        Transaction *transaction = nullptr, bool *root_is_latched = nullptr);

  template <typename N>
  N *Split(N *node);

  template <typename N>
  bool CoalesceOrRedistribute(N *node, Transaction *transaction = nullptr, bool *root_is_latched = nullptr);

  template <typename N>
  bool Coalesce(N **neighbor_node, N **node, BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent,
                int index, Transaction *transaction = nullptr, bool *root_is_latched = nullptr);

  template <typename N>
  void Redistribute(N *neighbor_node, N *node, int index);

  bool AdjustRoot(BPlusTreePage *node);

  void UpdateRootPageId(int insert_record = 0);

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  void UnlockPages(Transaction *transaction);

  // unlock 和 unpin 事务中经过的所有parent page
  void UnlockUnpinPages(Transaction *transaction);

  // 判断node是否安全
  template <typename N>
  bool IsSafe(N *node, Operation op);

  // member variable
  std::string index_name_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;
  std::mutex root_latch_;  // 保护root page id不被改变
  // bool root_is_latched_;   // static thread_local
  // std::mutex latch_;  // DEBUG
};

}  // namespace bustub
