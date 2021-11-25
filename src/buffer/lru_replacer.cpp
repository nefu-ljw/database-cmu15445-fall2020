//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) { max_size = num_pages; }

LRUReplacer::~LRUReplacer() = default;  // 折构函数似乎不用修改？

/**
 * 使用LRU策略删除一个victim frame，这个函数能得到frame_id
 * @param[out] frame_id id of frame that was removed, nullptr if no victim was found
 * @return true if a victim frame was found, false otherwise
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  // C++17 std::scoped_lock
  // 它能够避免死锁发生，其构造函数能够自动进行上锁操作，析构函数会对互斥量进行解锁操作，保证线程安全。
  std::scoped_lock lock{mut};
  if (LRUlist.empty()) {
    frame_id = nullptr;
    return false;
  }
  // list<int>a，那么a.back()取出的是int类型
  *frame_id = LRUlist.back();  // 取出最后一个给frame_id（对传入的参数进行修改）
  LRUhash.erase(*frame_id);    // 哈希表中删除其映射关系
  // 以上均要加*，才能改变函数外调用时传入的参数
  LRUlist.pop_back();  // 链表中删除最后一个
  return true;
}

/**
 * 固定一个frame, 表明它不应该成为victim（即在replacer中移除该frame_id）
 * @param frame_id the id of the frame to pin
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock lock{mut};
  // 哈希表中找不到该frame_id
  if (LRUhash.count(frame_id) == 0) {
    return;
  }
  auto iter = LRUhash[frame_id];
  LRUlist.erase(iter);
  LRUhash.erase(frame_id);
}

/**
 * 取消固定一个frame, 表明它可以成为victim（即将该frame_id添加到replacer）
 * @param frame_id the id of the frame to unpin
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::scoped_lock lock{mut};
  // 哈希表中已有该frame_id，直接退出，避免重复添加到replacer
  if (LRUhash.count(frame_id) != 0) {
    return;
  }
  // 已达最大容量，无法添加到replacer
  if (LRUlist.size() == max_size) {
    return;
  }
  // 正常添加到replacer
  LRUlist.push_front(frame_id);  // 注意是添加到首部还是尾部呢？
  // 首部是最近被使用，尾部是最久未被使用
  LRUhash.emplace(frame_id, LRUlist.begin());
}

/** @return replacer中能够victim的数量 */
size_t LRUReplacer::Size() { return LRUlist.size(); }

}  // namespace bustub
