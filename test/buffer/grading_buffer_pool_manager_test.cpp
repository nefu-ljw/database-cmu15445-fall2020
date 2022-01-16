/**
 * grading_buffer_pool_manager_test.cpp
 *
 * TEST in project #1 on gradescope (40.0/160.0):
 * test_BufferPoolManager_SampleTest (__main__.TestProject1) (5.0/5.0)
 * test_BufferPoolManager_NewPage (__main__.TestProject1) (5.0/5.0)
 * test_BufferPoolManager_UnpinPage (__main__.TestProject1) (5.0/5.0)
 * test_BufferPoolManager_FetchPage (__main__.TestProject1) (5.0/5.0)
 * test_BufferPoolManager_DeletePage (__main__.TestProject1) (5.0/5.0)
 * test_BufferPoolManager_IsDirty (__main__.TestProject1) (5.0/5.0)
 * test_BufferPoolManager_IntegratedTest (__main__.TestProject1) (10.0/10.0)
 *
 * NOT test on gradescope:
 * BufferPoolManagerTest.BinaryDataTest
 */

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_test.cpp
//
// Identification: test/buffer/buffer_pool_manager_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <thread>  // NOLINT
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "mock_buffer_pool_manager.h"  // NOLINT

namespace bustub {

#define BufferPoolManager MockBufferPoolManager

// NOLINTNEXTLINE
TEST(BufferPoolManagerTest, SampleTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManager(buffer_pool_size, disk_manager);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);

  // Scenario: Once we have a page, we should be able to read and write content.
  snprintf(page0->GetData(), sizeof(page0->GetData()), "Hello");
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning another 4 new pages,
  // there would still be one cache frame left for reading page 0.
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm->UnpinPage(i, true));
  }
  for (int i = 0; i < 4; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: We should be able to fetch the data we wrote a while ago.
  page0 = bpm->FetchPage(0);
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));
  EXPECT_EQ(true, bpm->UnpinPage(0, true));
  // NewPage again, and now all buffers are pinned. Page 0 would be failed to fetch.
  EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  EXPECT_EQ(nullptr, bpm->FetchPage(0));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerTest, BinaryDataTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManager(buffer_pool_size, disk_manager);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);

  char random_binary_data[PAGE_SIZE];
  unsigned int seed = 15645;
  for (char &i : random_binary_data) {
    i = static_cast<char>(rand_r(&seed) % 256);
  }

  random_binary_data[PAGE_SIZE / 2] = '\0';
  random_binary_data[PAGE_SIZE - 1] = '\0';

  // Scenario: Once we have a page, we should be able to read and write content.
  std::strncpy(page0->GetData(), random_binary_data, PAGE_SIZE);
  EXPECT_EQ(0, std::strcmp(page0->GetData(), random_binary_data));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning another 4 new pages,
  // there would still be one cache frame left for reading page 0.
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm->UnpinPage(i, true));
    bpm->FlushPage(i);
  }
  for (int i = 0; i < 5; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
    bpm->UnpinPage(page_id_temp, false);
  }
  // Scenario: We should be able to fetch the data we wrote a while ago.
  page0 = bpm->FetchPage(0);
  EXPECT_EQ(0, strcmp(page0->GetData(), random_binary_data));
  EXPECT_EQ(true, bpm->UnpinPage(0, true));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerTest, NewPage) {
  page_id_t temp_page_id;
  DiskManager *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManager(10, disk_manager);

  std::vector<page_id_t> page_ids;

  for (int i = 0; i < 10; ++i) {
    auto new_page = bpm->NewPage(&temp_page_id);
    EXPECT_NE(nullptr, new_page);
    ASSERT_NE(nullptr, new_page);
    strcpy(new_page->GetData(), std::to_string(i).c_str());  // NOLINT
    page_ids.push_back(temp_page_id);
  }

  // all the pages are pinned, the buffer pool is full
  for (int i = 0; i < 100; ++i) {
    auto new_page = bpm->NewPage(&temp_page_id);
    EXPECT_EQ(nullptr, new_page);
  }

  // upin the first five pages, add them to LRU list, set as dirty
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm->UnpinPage(page_ids[i], true));
  }

  // we have 5 empty slots in LRU list, evict page zero out of buffer pool
  for (int i = 0; i < 5; ++i) {
    auto new_page = bpm->NewPage(&temp_page_id);
    EXPECT_NE(nullptr, new_page);
    ASSERT_NE(nullptr, new_page);
    page_ids[i] = temp_page_id;
  }

  // all the pages are pinned, the buffer pool is full
  for (int i = 0; i < 100; ++i) {
    auto new_page = bpm->NewPage(&temp_page_id);
    EXPECT_EQ(nullptr, new_page);
  }

  // upin the first five pages, add them to LRU list
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm->UnpinPage(page_ids[i], false));
  }

  // we have 5 empty slots in LRU list, evict page zero out of buffer pool
  for (int i = 0; i < 5; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&temp_page_id));
  }

  // all the pages are pinned, the buffer pool is full
  for (int i = 0; i < 100; ++i) {
    auto new_page = bpm->NewPage(&temp_page_id);
    EXPECT_EQ(nullptr, new_page);
  }

  remove("test.db");
  remove("test.log");
  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerTest, UnpinPage) {
  DiskManager *disk_manager = new DiskManager("test.db");
  auto bpm = new BufferPoolManager(2, disk_manager);

  page_id_t pageid0;
  auto page0 = bpm->NewPage(&pageid0);
  ASSERT_NE(nullptr, page0);
  strcpy(page0->GetData(), "page0");  // NOLINT

  page_id_t pageid1;
  auto page1 = bpm->NewPage(&pageid1);
  ASSERT_NE(nullptr, page1);
  strcpy(page1->GetData(), "page1");  // NOLINT

  EXPECT_EQ(1, bpm->UnpinPage(pageid0, true));
  EXPECT_EQ(1, bpm->UnpinPage(pageid1, true));

  for (int i = 0; i < 2; i++) {
    page_id_t temp_page_id;
    auto new_page = bpm->NewPage(&temp_page_id);
    ASSERT_NE(nullptr, new_page);
    bpm->UnpinPage(temp_page_id, true);
  }

  auto page = bpm->FetchPage(pageid0);
  EXPECT_EQ(0, strcmp(page->GetData(), "page0"));
  strcpy(page->GetData(), "page0updated");  // NOLINT

  page = bpm->FetchPage(pageid1);
  EXPECT_EQ(0, strcmp(page->GetData(), "page1"));
  strcpy(page->GetData(), "page1updated");  // NOLINT

  EXPECT_EQ(1, bpm->UnpinPage(pageid0, false));
  EXPECT_EQ(1, bpm->UnpinPage(pageid1, true));

  for (int i = 0; i < 2; i++) {
    page_id_t temp_page_id;
    auto new_page = bpm->NewPage(&temp_page_id);
    ASSERT_NE(nullptr, new_page);
    bpm->UnpinPage(temp_page_id, true);
  }

  page = bpm->FetchPage(pageid0);
  EXPECT_EQ(0, strcmp(page->GetData(), "page0"));
  strcpy(page->GetData(), "page0updated");  // NOLINT

  page = bpm->FetchPage(pageid1);
  EXPECT_EQ(0, strcmp(page->GetData(), "page1updated"));
  strcpy(page->GetData(), "page1againupdated");  // NOLINT

  remove("test.db");
  remove("test.log");
  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerTest, FetchPage) {
  page_id_t temp_page_id;
  DiskManager *disk_manager = new DiskManager("test.db");
  auto bpm = new BufferPoolManager(10, disk_manager);

  std::vector<Page *> pages;
  std::vector<page_id_t> page_ids;
  std::vector<std::string> content;

  for (int i = 0; i < 10; ++i) {
    auto new_page = bpm->NewPage(&temp_page_id);
    ASSERT_NE(nullptr, new_page);
    strcpy(new_page->GetData(), std::to_string(i).c_str());  // NOLINT
    pages.push_back(new_page);
    page_ids.push_back(temp_page_id);
    content.push_back(std::to_string(i));
  }

  for (int i = 0; i < 10; ++i) {
    auto page = bpm->FetchPage(page_ids[i]);
    ASSERT_NE(nullptr, page);
    EXPECT_EQ(pages[i], page);
    EXPECT_EQ(0, std::strcmp(std::to_string(i).c_str(), (page->GetData())));
    EXPECT_EQ(1, bpm->UnpinPage(page_ids[i], true));
    EXPECT_EQ(1, bpm->UnpinPage(page_ids[i], true));
    bpm->FlushPage(page_ids[i]);
  }

  for (int i = 0; i < 10; ++i) {
    auto new_page = bpm->NewPage(&temp_page_id);
    ASSERT_NE(nullptr, new_page);
    bpm->UnpinPage(temp_page_id, true);
  }

  for (int i = 0; i < 10; ++i) {
    auto page = bpm->FetchPage(page_ids[i]);
    ASSERT_NE(nullptr, page);
  }

  EXPECT_EQ(1, bpm->UnpinPage(page_ids[4], true));
  auto new_page = bpm->NewPage(&temp_page_id);
  ASSERT_NE(nullptr, new_page);
  EXPECT_EQ(nullptr, bpm->FetchPage(page_ids[4]));

  // Check Clock
  auto page5 = bpm->FetchPage(page_ids[5]);
  auto page6 = bpm->FetchPage(page_ids[6]);
  auto page7 = bpm->FetchPage(page_ids[7]);
  EXPECT_NE(nullptr, page5);
  EXPECT_NE(nullptr, page6);
  EXPECT_NE(nullptr, page7);
  strcpy(page5->GetData(), "updatedpage5");  // NOLINT
  strcpy(page6->GetData(), "updatedpage6");  // NOLINT
  strcpy(page7->GetData(), "updatedpage7");  // NOLINT
  EXPECT_EQ(1, bpm->UnpinPage(page_ids[5], false));
  EXPECT_EQ(1, bpm->UnpinPage(page_ids[6], false));
  EXPECT_EQ(1, bpm->UnpinPage(page_ids[7], false));

  EXPECT_EQ(1, bpm->UnpinPage(page_ids[5], false));
  EXPECT_EQ(1, bpm->UnpinPage(page_ids[6], false));
  EXPECT_EQ(1, bpm->UnpinPage(page_ids[7], false));

  // page5 would be evicted.
  new_page = bpm->NewPage(&temp_page_id);
  ASSERT_NE(nullptr, new_page);
  // page6 would be evicted.
  page5 = bpm->FetchPage(page_ids[5]);
  EXPECT_NE(nullptr, page5);
  EXPECT_EQ(0, std::strcmp("5", (page5->GetData())));
  page7 = bpm->FetchPage(page_ids[7]);
  EXPECT_NE(nullptr, page7);
  EXPECT_EQ(0, std::strcmp("updatedpage7", (page7->GetData())));
  // All pages pinned
  EXPECT_EQ(nullptr, bpm->FetchPage(page_ids[6]));
  bpm->UnpinPage(temp_page_id, false);
  page6 = bpm->FetchPage(page_ids[6]);
  EXPECT_NE(nullptr, page6);
  EXPECT_EQ(0, std::strcmp("6", page6->GetData()));

  strcpy(page6->GetData(), "updatedpage6");  // NOLINT

  // Remove from LRU and update pin_count on fetch
  new_page = bpm->NewPage(&temp_page_id);
  EXPECT_EQ(nullptr, new_page);

  EXPECT_EQ(1, bpm->UnpinPage(page_ids[7], false));
  EXPECT_EQ(1, bpm->UnpinPage(page_ids[6], false));

  new_page = bpm->NewPage(&temp_page_id);
  ASSERT_NE(nullptr, new_page);
  page6 = bpm->FetchPage(page_ids[6]);
  EXPECT_NE(nullptr, page6);
  EXPECT_EQ(0, std::strcmp("updatedpage6", page6->GetData()));
  page7 = bpm->FetchPage(page_ids[7]);
  EXPECT_EQ(nullptr, page7);
  bpm->UnpinPage(temp_page_id, false);
  page7 = bpm->FetchPage(page_ids[7]);
  EXPECT_NE(nullptr, page7);
  EXPECT_EQ(0, std::strcmp("7", (page7->GetData())));

  remove("test.db");
  remove("test.log");
  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerTest, DeletePage) {
  page_id_t temp_page_id;
  DiskManager *disk_manager = new DiskManager("test.db");
  auto bpm = new BufferPoolManager(10, disk_manager);

  std::vector<Page *> pages;
  std::vector<page_id_t> page_ids;
  std::vector<std::string> content;

  for (int i = 0; i < 10; ++i) {
    auto new_page = bpm->NewPage(&temp_page_id);
    ASSERT_NE(nullptr, new_page);
    strcpy(new_page->GetData(), std::to_string(i).c_str());  // NOLINT
    pages.push_back(new_page);
    page_ids.push_back(temp_page_id);
    content.push_back(std::to_string(i));
  }

  for (int i = 0; i < 10; ++i) {
    auto page = bpm->FetchPage(page_ids[i]);
    ASSERT_NE(nullptr, page);
    EXPECT_EQ(1, bpm->UnpinPage(page_ids[i], true));
    EXPECT_EQ(1, bpm->UnpinPage(page_ids[i], true));
  }

  for (int i = 0; i < 10; ++i) {
    auto new_page = bpm->NewPage(&temp_page_id);
    ASSERT_NE(nullptr, new_page);
    bpm->UnpinPage(temp_page_id, true);
  }

  for (int i = 0; i < 10; ++i) {
    auto page = bpm->FetchPage(page_ids[i]);
    ASSERT_NE(nullptr, page);
  }

  auto new_page = bpm->NewPage(&temp_page_id);
  EXPECT_EQ(nullptr, new_page);

  EXPECT_EQ(0, bpm->DeletePage(page_ids[4]));
  bpm->UnpinPage(4, false);
  EXPECT_EQ(1, bpm->DeletePage(page_ids[4]));

  new_page = bpm->NewPage(&temp_page_id);
  EXPECT_NE(nullptr, new_page);
  ASSERT_NE(nullptr, new_page);

  auto page5 = bpm->FetchPage(page_ids[5]);
  ASSERT_NE(nullptr, page5);
  auto page6 = bpm->FetchPage(page_ids[6]);
  ASSERT_NE(nullptr, page6);
  auto page7 = bpm->FetchPage(page_ids[7]);
  ASSERT_NE(nullptr, page7);
  strcpy(page5->GetData(), "updatedpage5");  // NOLINT
  strcpy(page6->GetData(), "updatedpage6");  // NOLINT
  strcpy(page7->GetData(), "updatedpage7");  // NOLINT
  bpm->UnpinPage(5, false);
  bpm->UnpinPage(6, false);
  bpm->UnpinPage(7, false);

  bpm->UnpinPage(5, false);
  bpm->UnpinPage(6, false);
  bpm->UnpinPage(7, false);
  EXPECT_EQ(1, bpm->DeletePage(page_ids[7]));

  bpm->NewPage(&temp_page_id);
  page5 = bpm->FetchPage(page_ids[5]);
  page6 = bpm->FetchPage(page_ids[6]);
  ASSERT_NE(nullptr, page5);
  ASSERT_NE(nullptr, page6);
  EXPECT_EQ(0, std::strcmp(page5->GetData(), "updatedpage5"));
  EXPECT_EQ(0, std::strcmp(page6->GetData(), "updatedpage6"));

  remove("test.db");
  remove("test.log");
  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerTest, IsDirty) {
  DiskManager *disk_manager = new DiskManager("test.db");
  auto bpm = new BufferPoolManager(1, disk_manager);

  // Make new page and write to it
  page_id_t pageid0;
  auto page0 = bpm->NewPage(&pageid0);
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page0->IsDirty());
  strcpy(page0->GetData(), "page0");  // NOLINT
  EXPECT_EQ(1, bpm->UnpinPage(pageid0, true));

  // Fetch again but don't write. Assert it is still marked as dirty
  page0 = bpm->FetchPage(pageid0);
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(1, page0->IsDirty());
  EXPECT_EQ(1, bpm->UnpinPage(pageid0, false));

  // Fetch and assert it is still dirty
  page0 = bpm->FetchPage(pageid0);
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(1, page0->IsDirty());
  EXPECT_EQ(1, bpm->UnpinPage(pageid0, false));

  // Create a new page, assert it's not dirty
  page_id_t pageid1;
  auto page1 = bpm->NewPage(&pageid1);
  ASSERT_NE(nullptr, page1);
  EXPECT_EQ(0, page1->IsDirty());

  // Write to the page, and then delete it
  strcpy(page1->GetData(), "page1");  // NOLINT
  EXPECT_EQ(1, bpm->UnpinPage(pageid1, true));
  EXPECT_EQ(1, page1->IsDirty());
  EXPECT_EQ(1, bpm->DeletePage(pageid1));

  // Fetch page 0 again, and confirm its not dirty
  page0 = bpm->FetchPage(pageid0);
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page0->IsDirty());

  remove("test.db");
  remove("test.log");
  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerTest, IntegratedTest) {
  page_id_t temp_page_id;
  DiskManager *disk_manager = new DiskManager("test.db");
  auto bpm = new BufferPoolManager(10, disk_manager);

  std::vector<page_id_t> page_ids;
  for (int j = 0; j < 1000; j++) {
    for (int i = 0; i < 10; i++) {
      auto new_page = bpm->NewPage(&temp_page_id);
      EXPECT_NE(nullptr, new_page);
      ASSERT_NE(nullptr, new_page);
      strcpy(new_page->GetData(), std::to_string(temp_page_id).c_str());  // NOLINT
      page_ids.push_back(temp_page_id);
    }
    for (unsigned int i = page_ids.size() - 10; i < page_ids.size(); i++) {
      EXPECT_EQ(1, bpm->UnpinPage(page_ids[i], true));
    }
  }

  for (int j = 0; j < 10000; j++) {
    auto page = bpm->FetchPage(page_ids[j]);
    EXPECT_NE(nullptr, page);
    ASSERT_NE(nullptr, page);
    EXPECT_EQ(0, std::strcmp(std::to_string(page_ids[j]).c_str(), (page->GetData())));
    EXPECT_EQ(1, bpm->UnpinPage(page_ids[j], true));
    page_ids.push_back(temp_page_id);
  }
  for (int j = 0; j < 10000; j++) {
    EXPECT_EQ(1, bpm->DeletePage(page_ids[j]));
  }
  remove("test.db");
  remove("test.log");
  delete bpm;
  delete disk_manager;
}

}  // namespace bustub
