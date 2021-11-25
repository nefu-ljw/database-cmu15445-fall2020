//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer_test.cpp
//
// Identification: test/buffer/lru_replacer_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <thread>  // NOLINT
#include <vector>

#include "buffer/lru_replacer.h"
#include "gtest/gtest.h"

namespace bustub {

// TEST(LRUReplacerTest, DISABLED_SampleTest) {
TEST(LRUReplacerTest, SampleTest) {
  LRUReplacer lru_replacer(7);

  // Scenario: unpin six elements, i.e. add them to the replacer.
  lru_replacer.Unpin(1);
  lru_replacer.Unpin(2);
  lru_replacer.Unpin(3);
  lru_replacer.Unpin(4);
  lru_replacer.Unpin(5);
  lru_replacer.Unpin(6);
  lru_replacer.Unpin(1);
  EXPECT_EQ(6, lru_replacer.Size());  // 此刻还有6 5 4 3 2 1

  // Scenario: get three victims from the lru.
  int value;
  lru_replacer.Victim(&value);
  EXPECT_EQ(1, value);  // 此刻还有6 5 4 3 2
  lru_replacer.Victim(&value);
  EXPECT_EQ(2, value);  // 此刻还有6 5 4 3
  lru_replacer.Victim(&value);
  EXPECT_EQ(3, value);  // 此刻还有6 5 4

  // Scenario: pin elements in the replacer.
  // Note that 3 has already been victimized, so pinning 3 should have no effect.
  lru_replacer.Pin(3);                // 此刻还有6 5 4（该操作无效）
  lru_replacer.Pin(4);                // 此刻还有6 5
  EXPECT_EQ(2, lru_replacer.Size());  // ok

  // Scenario: unpin 4. We expect that the reference bit of 4 will be set to 1.
  lru_replacer.Unpin(4);  // 此刻还有4 6 5

  EXPECT_EQ(3, lru_replacer.Size());  // fail

  // Scenario: continue looking for victims. We expect these victims.
  lru_replacer.Victim(&value);
  EXPECT_EQ(5, value);
  lru_replacer.Victim(&value);
  EXPECT_EQ(6, value);

  EXPECT_EQ(1, lru_replacer.Size());

  lru_replacer.Victim(&value);
  EXPECT_EQ(4, value);
}

}  // namespace bustub
