/**
 * grading_buffer_pool_manager_concurrency_test.cpp
 *
 * TEST in project #1 on gradescope (80.0/160.0):
 * test_BufferPoolManager_ConcurrencyTest (__main__.TestProject1) (10.0/10.0)
 * test_BufferPoolManager_HardTestA (__main__.TestProject1) (15.0/15.0)
 * test_BufferPoolManager_HardTestB (__main__.TestProject1) (15.0/15.0)
 * test_BufferPoolManager_HardTestC (__main__.TestProject1) (15.0/15.0)
 * test_BufferPoolManager_HardTestD (__main__.TestProject1) (15.0/15.0)
 * test_memory_safety (__main__.TestProject1) (10.0/10.0) [Valgrind TEST]
 */

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_concurrency_test.cpp
//
// Identification: test/buffer/buffer_pool_manager_concurrency_test.cpp
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

TEST(BufferPoolManagerConcurrencyTest, ConcurrencyTest) {
  const int num_threads = 5;
  const int num_runs = 50;
  for (int run = 0; run < num_runs; run++) {
    DiskManager *disk_manager = new DiskManager("test.db");
    std::shared_ptr<BufferPoolManager> bpm{new BufferPoolManager(50, disk_manager)};
    std::vector<std::thread> threads;

    for (int tid = 0; tid < num_threads; tid++) {
      threads.push_back(std::thread([&bpm]() {  // NOLINT
        page_id_t temp_page_id;
        std::vector<page_id_t> page_ids;
        for (int i = 0; i < 10; i++) {
          auto new_page = bpm->NewPage(&temp_page_id, nullptr);
          EXPECT_NE(nullptr, new_page);
          ASSERT_NE(nullptr, new_page);
          strcpy(new_page->GetData(), std::to_string(temp_page_id).c_str());  // NOLINT
          page_ids.push_back(temp_page_id);
        }
        for (int i = 0; i < 10; i++) {
          EXPECT_EQ(1, bpm->UnpinPage(page_ids[i], true, nullptr));
        }
        for (int j = 0; j < 10; j++) {
          auto page = bpm->FetchPage(page_ids[j], nullptr);
          EXPECT_NE(nullptr, page);
          ASSERT_NE(nullptr, page);
          EXPECT_EQ(0, std::strcmp(std::to_string(page_ids[j]).c_str(), (page->GetData())));
          EXPECT_EQ(1, bpm->UnpinPage(page_ids[j], true, nullptr));
        }
        for (int j = 0; j < 10; j++) {
          EXPECT_EQ(1, bpm->DeletePage(page_ids[j], nullptr));
        }
      }));
    }

    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    remove("test.db");
    remove("test.log");
    delete disk_manager;
  }
}

TEST(BufferPoolManagerConcurrencyTest, HardTest_1) {
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
    for (unsigned int i = page_ids.size() - 10; i < page_ids.size() - 5; i++) {
      EXPECT_EQ(1, bpm->UnpinPage(page_ids[i], false));
    }
    for (unsigned int i = page_ids.size() - 5; i < page_ids.size(); i++) {
      EXPECT_EQ(1, bpm->UnpinPage(page_ids[i], true));
    }
  }

  for (int j = 0; j < 10000; j++) {
    auto page = bpm->FetchPage(page_ids[j]);
    EXPECT_NE(nullptr, page);
    ASSERT_NE(nullptr, page);
    if (j % 10 < 5) {
      EXPECT_NE(0, std::strcmp(std::to_string(page_ids[j]).c_str(), (page->GetData())));
    } else {
      EXPECT_EQ(0, std::strcmp(std::to_string(page_ids[j]).c_str(), (page->GetData())));
    }
    EXPECT_EQ(1, bpm->UnpinPage(page_ids[j], true));
  }

  auto rng = std::default_random_engine{};
  std::shuffle(page_ids.begin(), page_ids.end(), rng);

  for (int j = 0; j < 5000; j++) {
    auto page = bpm->FetchPage(page_ids[j]);
    EXPECT_NE(nullptr, page);
    ASSERT_NE(nullptr, page);
    EXPECT_EQ(1, bpm->UnpinPage(page_ids[j], false));
    EXPECT_EQ(1, bpm->DeletePage(page_ids[j]));
  }

  for (int j = 5000; j < 10000; j++) {
    auto page = bpm->FetchPage(page_ids[j]);
    EXPECT_NE(nullptr, page);
    ASSERT_NE(nullptr, page);
    if (page_ids[j] % 10 < 5) {
      EXPECT_NE(0, std::strcmp(std::to_string(page_ids[j]).c_str(), (page->GetData())));
    } else {
      EXPECT_EQ(0, std::strcmp(std::to_string(page_ids[j]).c_str(), (page->GetData())));
    }
    EXPECT_EQ(1, bpm->UnpinPage(page_ids[j], false));
    EXPECT_EQ(1, bpm->DeletePage(page_ids[j]));
  }

  remove("test.db");
  remove("test.log");
  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerConcurrencyTest, HardTest_2) {
  const int num_threads = 5;
  const int num_runs = 50;
  for (int run = 0; run < num_runs; run++) {
    auto *disk_manager = new DiskManager("test.db");
    std::shared_ptr<BufferPoolManager> bpm{new BufferPoolManager(50, disk_manager)};
    std::vector<std::thread> threads;

    page_id_t temp_page_id;
    std::vector<page_id_t> page_ids;
    for (int i = 0; i < 50; i++) {
      auto *new_page = bpm->NewPage(&temp_page_id, nullptr);
      EXPECT_NE(nullptr, new_page);
      ASSERT_NE(nullptr, new_page);
      strcpy(new_page->GetData(), std::to_string(temp_page_id).c_str());  // NOLINT
      page_ids.push_back(temp_page_id);
    }

    for (int i = 0; i < 50; i++) {
      if (i % 2 == 0) {
        EXPECT_EQ(1, bpm->UnpinPage(page_ids[i], true, nullptr));
      } else {
        EXPECT_EQ(1, bpm->UnpinPage(page_ids[i], false, nullptr));
      }
    }

    for (int i = 0; i < 50; i++) {
      auto *new_page = bpm->NewPage(&temp_page_id, nullptr);
      EXPECT_NE(nullptr, new_page);
      ASSERT_NE(nullptr, new_page);
      EXPECT_EQ(1, bpm->UnpinPage(temp_page_id, true, nullptr));
    }

    for (int j = 0; j < 50; j++) {
      auto *page = bpm->FetchPage(page_ids[j], nullptr);
      EXPECT_NE(nullptr, page);
      ASSERT_NE(nullptr, page);
      strcpy(page->GetData(), (std::string("Hard") + std::to_string(page_ids[j])).c_str());  // NOLINT
    }

    for (int i = 0; i < 50; i++) {
      if (i % 2 == 0) {
        EXPECT_EQ(1, bpm->UnpinPage(page_ids[i], false, nullptr));
      } else {
        EXPECT_EQ(1, bpm->UnpinPage(page_ids[i], true, nullptr));
      }
    }

    for (int i = 0; i < 50; i++) {
      auto *new_page = bpm->NewPage(&temp_page_id, nullptr);
      EXPECT_NE(nullptr, new_page);
      ASSERT_NE(nullptr, new_page);
      EXPECT_EQ(1, bpm->UnpinPage(temp_page_id, true, nullptr));
    }

    for (int tid = 0; tid < num_threads; tid++) {
      threads.push_back(std::thread([&bpm, tid, page_ids]() {  // NOLINT
        int j = (tid * 10);
        while (j < 50) {
          auto *page = bpm->FetchPage(page_ids[j], nullptr);
          while (page == nullptr) {
            page = bpm->FetchPage(page_ids[j], nullptr);
          }
          EXPECT_NE(nullptr, page);
          ASSERT_NE(nullptr, page);
          if (j % 2 == 0) {
            EXPECT_EQ(0, std::strcmp(std::to_string(page_ids[j]).c_str(), (page->GetData())));
            EXPECT_EQ(1, bpm->UnpinPage(page_ids[j], false, nullptr));
          } else {
            EXPECT_EQ(0, std::strcmp((std::string("Hard") + std::to_string(page_ids[j])).c_str(), (page->GetData())));
            EXPECT_EQ(1, bpm->UnpinPage(page_ids[j], false, nullptr));
          }
          j = (j + 1);
        }
      }));
    }

    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    for (int j = 0; j < 50; j++) {
      EXPECT_EQ(1, bpm->DeletePage(page_ids[j], nullptr));
    }

    remove("test.db");
    remove("test.log");
    delete disk_manager;
  }
}

TEST(BufferPoolManagerConcurrencyTest, HardTest_3) {
  const int num_threads = 5;
  const int num_runs = 50;
  for (int run = 0; run < num_runs; run++) {
    auto *disk_manager = new DiskManager("test.db");
    std::shared_ptr<BufferPoolManager> bpm{new BufferPoolManager(50, disk_manager)};
    std::vector<std::thread> threads;

    page_id_t temp_page_id;
    std::vector<page_id_t> page_ids;
    for (int i = 0; i < 50; i++) {
      auto *new_page = bpm->NewPage(&temp_page_id, nullptr);
      EXPECT_NE(nullptr, new_page);
      ASSERT_NE(nullptr, new_page);
      strcpy(new_page->GetData(), std::to_string(temp_page_id).c_str());  // NOLINT
      page_ids.push_back(temp_page_id);
    }

    for (int i = 0; i < 50; i++) {
      if (i % 2 == 0) {
        EXPECT_EQ(1, bpm->UnpinPage(page_ids[i], true, nullptr));
      } else {
        EXPECT_EQ(1, bpm->UnpinPage(page_ids[i], false, nullptr));
      }
    }

    for (int i = 0; i < 50; i++) {
      auto *new_page = bpm->NewPage(&temp_page_id, nullptr);
      EXPECT_NE(nullptr, new_page);
      ASSERT_NE(nullptr, new_page);
      EXPECT_EQ(1, bpm->UnpinPage(temp_page_id, true, nullptr));
    }

    for (int j = 0; j < 50; j++) {
      auto *page = bpm->FetchPage(page_ids[j], nullptr);
      EXPECT_NE(nullptr, page);
      ASSERT_NE(nullptr, page);
      strcpy(page->GetData(), (std::string("Hard") + std::to_string(page_ids[j])).c_str());  // NOLINT
    }

    for (int i = 0; i < 50; i++) {
      if (i % 2 == 0) {
        EXPECT_EQ(1, bpm->UnpinPage(page_ids[i], false, nullptr));
      } else {
        EXPECT_EQ(1, bpm->UnpinPage(page_ids[i], true, nullptr));
      }
    }

    for (int i = 0; i < 50; i++) {
      auto *new_page = bpm->NewPage(&temp_page_id, nullptr);
      EXPECT_NE(nullptr, new_page);
      ASSERT_NE(nullptr, new_page);
      EXPECT_EQ(1, bpm->UnpinPage(temp_page_id, true, nullptr));
    }

    for (int tid = 0; tid < num_threads; tid++) {
      threads.push_back(std::thread([&bpm, tid, page_ids]() {  // NOLINT
        page_id_t temp_page_id;
        int j = (tid * 10);
        while (j < 50) {
          if (j != tid * 10) {
            auto *page_local = bpm->FetchPage(temp_page_id, nullptr);
            while (page_local == nullptr) {
              page_local = bpm->FetchPage(temp_page_id, nullptr);
            }
            EXPECT_NE(nullptr, page_local);
            ASSERT_NE(nullptr, page_local);
            EXPECT_EQ(0, std::strcmp(std::to_string(temp_page_id).c_str(), (page_local->GetData())));
            EXPECT_EQ(1, bpm->UnpinPage(temp_page_id, false, nullptr));
            // If the page is still in buffer pool then put it in free list,
            // else also we are happy
            EXPECT_EQ(1, bpm->DeletePage(temp_page_id, nullptr));
          }

          auto *page = bpm->FetchPage(page_ids[j], nullptr);
          while (page == nullptr) {
            page = bpm->FetchPage(page_ids[j], nullptr);
          }
          EXPECT_NE(nullptr, page);
          ASSERT_NE(nullptr, page);
          if (j % 2 == 0) {
            EXPECT_EQ(0, std::strcmp(std::to_string(page_ids[j]).c_str(), (page->GetData())));
            EXPECT_EQ(1, bpm->UnpinPage(page_ids[j], false, nullptr));
          } else {
            EXPECT_EQ(0, std::strcmp((std::string("Hard") + std::to_string(page_ids[j])).c_str(), (page->GetData())));
            EXPECT_EQ(1, bpm->UnpinPage(page_ids[j], false, nullptr));
          }
          j = (j + 1);

          page = bpm->NewPage(&temp_page_id, nullptr);
          while (page == nullptr) {
            page = bpm->NewPage(&temp_page_id, nullptr);
          }
          EXPECT_NE(nullptr, page);
          ASSERT_NE(nullptr, page);
          strcpy(page->GetData(), std::to_string(temp_page_id).c_str());  // NOLINT
          EXPECT_EQ(1, bpm->UnpinPage(temp_page_id, true, nullptr));
        }
      }));
    }

    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    for (int j = 0; j < 50; j++) {
      EXPECT_EQ(1, bpm->DeletePage(page_ids[j], nullptr));
    }

    remove("test.db");
    remove("test.log");
    delete disk_manager;
  }
}

TEST(BufferPoolManagerConcurrencyTest, HardTest_4) {
  const int num_threads = 5;
  const int num_runs = 50;
  for (int run = 0; run < num_runs; run++) {
    auto *disk_manager = new DiskManager("test.db");
    std::shared_ptr<BufferPoolManager> bpm{new BufferPoolManager(50, disk_manager)};
    std::vector<std::thread> threads;

    page_id_t temp_page_id;
    std::vector<page_id_t> page_ids;
    for (int i = 0; i < 50; i++) {
      auto *new_page = bpm->NewPage(&temp_page_id, nullptr);
      EXPECT_NE(nullptr, new_page);
      ASSERT_NE(nullptr, new_page);
      strcpy(new_page->GetData(), std::to_string(temp_page_id).c_str());  // NOLINT
      page_ids.push_back(temp_page_id);
    }

    for (int i = 0; i < 50; i++) {
      if (i % 2 == 0) {
        EXPECT_EQ(1, bpm->UnpinPage(page_ids[i], true, nullptr));
      } else {
        EXPECT_EQ(1, bpm->UnpinPage(page_ids[i], false, nullptr));
      }
    }

    for (int i = 0; i < 50; i++) {
      auto *new_page = bpm->NewPage(&temp_page_id, nullptr);
      EXPECT_NE(nullptr, new_page);
      ASSERT_NE(nullptr, new_page);
      EXPECT_EQ(1, bpm->UnpinPage(temp_page_id, true, nullptr));
    }

    for (int j = 0; j < 50; j++) {
      auto *page = bpm->FetchPage(page_ids[j], nullptr);
      EXPECT_NE(nullptr, page);
      ASSERT_NE(nullptr, page);
      strcpy(page->GetData(), (std::string("Hard") + std::to_string(page_ids[j])).c_str());  // NOLINT
    }

    for (int i = 0; i < 50; i++) {
      if (i % 2 == 0) {
        EXPECT_EQ(1, bpm->UnpinPage(page_ids[i], false, nullptr));
      } else {
        EXPECT_EQ(1, bpm->UnpinPage(page_ids[i], true, nullptr));
      }
    }

    for (int i = 0; i < 50; i++) {
      auto *new_page = bpm->NewPage(&temp_page_id, nullptr);
      EXPECT_NE(nullptr, new_page);
      ASSERT_NE(nullptr, new_page);
      EXPECT_EQ(1, bpm->UnpinPage(temp_page_id, true, nullptr));
    }

    for (int tid = 0; tid < num_threads; tid++) {
      threads.push_back(std::thread([&bpm, tid, page_ids]() {  // NOLINT
        page_id_t temp_page_id;
        int j = (tid * 10);
        while (j < 50) {
          if (j != tid * 10) {
            auto *page_local = bpm->FetchPage(temp_page_id, nullptr);
            while (page_local == nullptr) {
              page_local = bpm->FetchPage(temp_page_id, nullptr);
            }
            EXPECT_NE(nullptr, page_local);
            ASSERT_NE(nullptr, page_local);
            EXPECT_EQ(0, std::strcmp(std::to_string(temp_page_id).c_str(), (page_local->GetData())));
            EXPECT_EQ(1, bpm->UnpinPage(temp_page_id, false, nullptr));
            // If the page is still in buffer pool then put it in free list,
            // else also we are happy
            EXPECT_EQ(1, bpm->DeletePage(temp_page_id, nullptr));
          }

          auto *page = bpm->FetchPage(page_ids[j], nullptr);
          while (page == nullptr) {
            page = bpm->FetchPage(page_ids[j], nullptr);
          }
          EXPECT_NE(nullptr, page);
          ASSERT_NE(nullptr, page);
          if (j % 2 == 0) {
            EXPECT_EQ(0, std::strcmp(std::to_string(page_ids[j]).c_str(), (page->GetData())));
            EXPECT_EQ(1, bpm->UnpinPage(page_ids[j], false, nullptr));
          } else {
            EXPECT_EQ(0, std::strcmp((std::string("Hard") + std::to_string(page_ids[j])).c_str(), (page->GetData())));
            EXPECT_EQ(1, bpm->UnpinPage(page_ids[j], false, nullptr));
          }
          j = (j + 1);

          page = bpm->NewPage(&temp_page_id, nullptr);
          while (page == nullptr) {
            page = bpm->NewPage(&temp_page_id, nullptr);
          }
          EXPECT_NE(nullptr, page);
          ASSERT_NE(nullptr, page);
          strcpy(page->GetData(), std::to_string(temp_page_id).c_str());  // NOLINT
          // FLush page instead of unpining with true
          EXPECT_EQ(1, bpm->FlushPage(temp_page_id, nullptr));
          EXPECT_EQ(1, bpm->UnpinPage(temp_page_id, false, nullptr));

          // Flood with new pages
          for (int k = 0; k < 10; k++) {
            page_id_t flood_page_id;
            auto *flood_page = bpm->NewPage(&flood_page_id, nullptr);
            while (flood_page == nullptr) {
              flood_page = bpm->NewPage(&flood_page_id, nullptr);
            }
            EXPECT_NE(nullptr, flood_page);
            ASSERT_NE(nullptr, flood_page);
            EXPECT_EQ(1, bpm->UnpinPage(flood_page_id, false, nullptr));
            // If the page is still in buffer pool then put it in free list,
            // else also we are happy
            EXPECT_EQ(1, bpm->DeletePage(flood_page_id, nullptr));
          }
        }
      }));
    }

    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    for (int j = 0; j < 50; j++) {
      EXPECT_EQ(1, bpm->DeletePage(page_ids[j], nullptr));
    }

    remove("test.db");
    remove("test.log");
    delete disk_manager;
  }
}

}  // namespace bustub
