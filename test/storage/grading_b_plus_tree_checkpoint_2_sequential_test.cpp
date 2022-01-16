/**
 * grading_b_plus_tree_checkpoint_2_sequential_test.cpp
 *
 * TEST in project #2 checkpoint #2 on gradescope (30.0/70.0):
 * test_BPlusTree_InsertTest1 (__main__.TestProject2) (5.0/5.0)
 * test_BPlusTree_InsertTest2 (__main__.TestProject2) (5.0/5.0)
 * test_BPlusTree_DeleteTest1 (__main__.TestProject2) (5.0/5.0)
 * test_BPlusTree_DeleteTest2 (__main__.TestProject2) (5.0/5.0)
 * test_BPlusTree_ScaleTest (__main__.TestProject2) (5.0/5.0)
 * test_BPlusTree_MixTest (__main__.TestProject2) (5.0/5.0)
 */

#include <algorithm>
#include <cstdio>
#include <random>  // for std::default_random_engine

#include "b_plus_tree_test_util.h"  // NOLINT
#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

/*
 * Score: 5
 * Description: The same test that has been run for checkpoint 1,
 * but added iterator for value checking
 */
TEST(BPlusTreeTests, InsertTest1) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  for (auto pair : tree) {
    auto location = pair.second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

/*
 * Score: 5
 * Description: The same test that has been run for checkpoint 1
 * but added iterator for value checking
 */
TEST(BPlusTreeTests, InsertTest2) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {5, 4, 3, 2, 1};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  for (auto pair : tree) {
    auto location = pair.second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  start_key = 3;
  current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); !iterator.isEnd(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

/*
 * Score: 10
 * Description: Insert a set of keys, use GetValue and iterator to
 * check the the inserted keys. Then delete a subset of the keys.
 * Finally use the iterator to check the remained keys.
 */
TEST(BPlusTreeTests, DeleteTest1) {
  // create KeyComparator and index schema
  std::string createStmt = "a bigint";
  Schema *key_schema = ParseCreateStatement(createStmt);
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);  // 传入了transaction
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  // for (auto pair : tree) {
  //   auto location = pair.second;
  //   EXPECT_EQ(location.GetPageId(), 0);
  //   EXPECT_EQ(location.GetSlotNum(), current_key);
  //   current_key = current_key + 1;
  // }
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); !iterator.isEnd(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(0, location.GetPageId());
    EXPECT_EQ(current_key, location.GetSlotNum());
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  std::vector<int64_t> remove_keys = {1, 5};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  start_key = 2;
  current_key = start_key;
  int64_t size = 0;
  // for (auto pair : tree) {
  //   auto location = pair.second;
  //   EXPECT_EQ(location.GetPageId(), 0);
  //   EXPECT_EQ(location.GetSlotNum(), current_key);
  //   current_key = current_key + 1;
  //   size = size + 1;
  // }
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); !iterator.isEnd(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(0, location.GetPageId());
    EXPECT_EQ(current_key, location.GetSlotNum());
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 3);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

/*
 * Score: 10
 * Description: Similar to DeleteTest2, except that, during the Remove step,
 * a different subset of keys are removed.
 */
TEST(BPlusTreeTests, DeleteTest2) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  // for (auto pair : tree) {
  //   auto location = pair.second;
  //   EXPECT_EQ(location.GetPageId(), 0);
  //   EXPECT_EQ(location.GetSlotNum(), current_key);
  //   current_key = current_key + 1;
  // }
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); !iterator.isEnd(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(0, location.GetPageId());
    EXPECT_EQ(current_key, location.GetSlotNum());
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  std::vector<int64_t> remove_keys = {1, 5, 3, 4};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  start_key = 2;
  current_key = start_key;
  int64_t size = 0;
  // for (auto pair : tree) {
  //   auto location = pair.second;
  //   EXPECT_EQ(location.GetPageId(), 0);
  //   EXPECT_EQ(location.GetSlotNum(), current_key);
  //   current_key = current_key + 1;
  //   size = size + 1;
  // }
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); !iterator.isEnd(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(0, location.GetPageId());
    EXPECT_EQ(current_key, location.GetSlotNum());
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

/*
 * Score: 10
 * Description: Insert 10000 keys. Use GetValue and the iterator to iterate
 * through the inserted keys. Then remove 9900 inserted keys. Finally, use
 * the iterator to check the correctness of the remaining keys.
 */
TEST(BPlusTreeTests, ScaleTest) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(30, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  int64_t scale = 10000;
  std::vector<int64_t> keys;
  for (int64_t key = 1; key < scale; key++) {
    keys.push_back(key);
  }

  // shuffle keys
  // std::random_shuffle(keys.begin(), keys.end());
  // NOTE: 'std::random_shuffle' has been removed in C++17; use 'std::shuffle' instead
  // std::shuffle(keys.begin(), keys.end(), std::mt19937(std::random_device()));
  auto rng = std::default_random_engine{};
  std::shuffle(keys.begin(), keys.end(), rng);
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }
  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  // for (auto pair : tree) {
  //   (void)pair;
  //   current_key = current_key + 1;
  // }
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); !iterator.isEnd(); ++iterator) {
    current_key = current_key + 1;
  }
  EXPECT_EQ(current_key, keys.size() + 1);

  int64_t remove_scale = 9900;
  std::vector<int64_t> remove_keys;
  for (int64_t key = 1; key < remove_scale; key++) {
    remove_keys.push_back(key);
  }

  // shuffle remove_keys
  std::shuffle(remove_keys.begin(), remove_keys.end(), rng);
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  start_key = remove_scale;
  int64_t size = 0;
  // for (auto pair : tree) {
  //   (void)pair;
  //   size = size + 1;
  // }
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); !iterator.isEnd(); ++iterator) {
    size = size + 1;
  }
  EXPECT_EQ(size, 100);

  remove_keys.clear();
  for (int64_t key = remove_scale; key < scale; key++) {
    remove_keys.push_back(key);
  }
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }
  EXPECT_EQ(true, tree.IsEmpty());

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

/*
 * Score: 10
 * Description: Insert a set of keys. Concurrently insert and delete
 * a different set of keys.
 * At the same time, concurrently get the previously inserted keys.
 * Check all the keys get are the same set of keys as previously
 * inserted.
 */
TEST(BPlusTreeTests, SequentialMixTest) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // first, populate index
  std::vector<int64_t> for_insert;
  std::vector<int64_t> for_delete;
  size_t sieve = 2;  // divide evenly
  size_t total_keys = 1000;
  for (size_t i = 1; i <= total_keys; i++) {
    if (i % sieve == 0) {
      for_insert.push_back(i);
    } else {
      for_delete.push_back(i);
    }
  }

  // Insert all the keys, including the ones that will remain at the end and
  // the ones that are going to be removed next.
  for (size_t i = 0; i < total_keys / 2; i++) {
    int64_t insert_key = for_insert[i];
    int64_t insert_value = insert_key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(insert_key >> 32), insert_value);
    index_key.SetFromInteger(insert_key);
    tree.Insert(index_key, rid, transaction);

    int64_t delete_key = for_delete[i];
    int64_t delete_value = delete_key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(delete_key >> 32), delete_value);
    index_key.SetFromInteger(delete_key);
    tree.Insert(index_key, rid, transaction);
  }

  // Remove the keys in for_delete
  for (auto key : for_delete) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  // Only half of the keys should remain
  int64_t start_key = 2;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto pair : tree) {
    EXPECT_EQ((pair.first).ToString(), for_insert[size]);
    size++;
  }

  EXPECT_EQ(size, for_insert.size());

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

}  // namespace bustub
