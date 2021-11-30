//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>
#include "include/common/logger.h"  // 日志调试

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));  // static_cast<int>转换数据类型为int
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

/*
将脏页写入磁盘，更新page元数据(data, is_dirty, page_id)和page table
（自己补充的函数）
*/
void BufferPoolManager::UpdatePage(Page *page, page_id_t page_id, frame_id_t frame_id) {
  // 1 如果是脏页，一定要写回磁盘
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->page_id_, page->data_);
  }
  page_table_.erase(page->page_id_);  // 删除页表中原page_id和其对应frame_id
  // 2 重置page的元数据，包括data和dirty状态
  page->ResetMemory();
  page->is_dirty_ = false;
  // 3 根据传入的参数更新page id和page table
  page->page_id_ = page_id;
  page_table_.emplace(page_id, frame_id);  // 更新页表为新的page_id和其对应frame_id
}

/*
从free_list或replacer中得到*frame_id；返回bool类型
（自己补充的函数）
*/
bool BufferPoolManager::FindVictimPage(frame_id_t *frame_id) {
  // 1 缓冲池还有freepages（缓冲池未满），即free_list非空，直接从free_list取出一个
  // 注意，在此函数中从free_list首部取出frame_id，在DeletePage函数中从free_list尾部添加frame_id
  if (!free_list_.empty()) {
    *frame_id = free_list_.front();
    free_list_.pop_front();
    return true;
  }
  // 2 缓冲池已满，根据LRU策略计算是否有victim frame_id
  return replacer_->Victim(frame_id);
}

/**
 * Fetch the requested page from the buffer pool.
 * 如果页表中存在page_id（说明该page在缓冲池中），并且pin_count++。
 * 如果页表不存在page_id（说明该page在磁盘中），则找缓冲池victim page，将其替换为磁盘中读取的page，pin_count置1。
 * @param page_id id of page to be fetched
 * @return the requested page
 */
Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::scoped_lock lock{latch_};
  auto iter = page_table_.find(page_id);
  // 1 该page在页表中存在（说明该page在缓冲池中）
  if (iter != page_table_.end()) {
    frame_id_t frame_id = iter->second;  // iter是pair类型，其second是page_id对应的frame_id
    Page *page = &pages_[frame_id];      // 由frame_id得到page
    replacer_->Pin(frame_id);            // pin it
    page->pin_count_++;                  // 更新pin_count
    return page;
  }
  // 2 该page在页表中不存在（说明该page不在缓冲池中，而在磁盘中）
  frame_id_t frame_id = -1;
  // 2.1 没有找到victim page
  if (!FindVictimPage(&frame_id)) {
    return nullptr;
  }
  // 2.2 找到victim page，将其data替换为磁盘中该page的内容
  Page *page = &pages_[frame_id];
  UpdatePage(page, page_id, frame_id);  // data置为空，dirty页写入磁盘，然后dirty状态置false
  disk_manager_->ReadPage(page_id, page->data_);  // 注意，从磁盘文件database file中page_id的位置读取内容到新page->data
  replacer_->Pin(frame_id);                       // pin it
  page->pin_count_ = 1;                           // pin_count置1
  return page;
}

/**
 * Unpin the target page from the buffer pool. 取消固定pin_count>0的在缓冲池中的page
 * @param page_id id of page to be unpinned
 * @param is_dirty true if the page should be marked as dirty, false otherwise
 * @return false if the page pin count is <= 0 before this call, true otherwise
 */
bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::scoped_lock lock{latch_};
  auto iter = page_table_.find(page_id);
  // 1 该page在页表中不存在
  if (iter == page_table_.end()) {
    return false;
  }
  // 2 该page在页表中存在
  frame_id_t frame_id = iter->second;  // iter是pair类型，其second是page_id对应的frame_id
  Page *page = &pages_[frame_id];      // 由frame_id得到page
  // 2.1 pin_count <= 0
  if (page->GetPinCount() <= 0) {
    return false;
  }
  // 2.2 pin_count > 0
  // 只有pin_count>0才能进行pin_count--，如果pin_count<=0之前就直接返回了
  page->pin_count_--;  // 这里特别注意，只有pin_count减到0的时候才让replacer进行unpin
  if (page->pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }
  // page->is_dirty_ = is_dirty;
  if (is_dirty) {
    page->is_dirty_ = true;  // 这个地方要看它is_dirty到底是怎么置的
  }
  return true;
}

/**
 * Flushes the target page to disk. 将page写入磁盘；不考虑pin_count
 * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
 * @return false if the page could not be found in the page table, true otherwise
 */
bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::scoped_lock lock{latch_};
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto iter = page_table_.find(page_id);
  // 1 该page在页表中不存在
  if (iter == page_table_.end()) {
    return false;
  }
  // 2 该page在页表中存在
  frame_id_t frame_id = iter->second;  // iter是pair类型，其second是page_id对应的frame_id
  Page *page = &pages_[frame_id];      // 由frame_id得到page
  // 不管dirty状态如何，都写入磁盘
  disk_manager_->WritePage(page->page_id_, page->data_);
  page->is_dirty_ = false;  // 注意这句话！刷新到磁盘后，dirty要重置false
  // 注意，不能调用UpdatePage，因为那个函数里面还进行了元数据的重置
  // UpdatePage(page, page_id, frame_id);
  return true;
}

/**
 * Creates a new page in the buffer pool. 相当于从磁盘中移动一个新建的空page到缓冲池某个位置
 * @param[out] page_id id of created page
 * @return nullptr if no new pages could be created, otherwise pointer to new page
 */
Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::scoped_lock lock{latch_};
  frame_id_t frame_id = -1;
  // 1 无法得到victim frame_id
  if (!FindVictimPage(&frame_id)) {
    // LOG_INFO("无victim frame_id");
    return nullptr;
  }
  // 2 得到victim frame_id（从free_list或replacer中得到）
  *page_id = disk_manager_->AllocatePage();  // 分配一个新的page_id（修改了外部参数*page_id）
  Page *page = &pages_[frame_id];            // 由frame_id得到page
  // pages_[frame_id]就是首地址偏移frame_id，左边的*page表示是一个指针指向那个地址，所以右边加&
  UpdatePage(page, *page_id, frame_id);
  page->pin_count_ = 1;  // 这里特别注意！每个新建page的pin_count初始为1
  // LOG_INFO("得到victim page_id=%u victim frame_id=%u", *page_id, frame_id);
  // page_id_t = signed int；frame_id_t = signed int
  return page;
}

/**
 * Deletes a page from the buffer pool.
 * @param page_id id of page to be deleted
 * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
 */
bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::scoped_lock lock{latch_};
  auto iter = page_table_.find(page_id);
  // 1 该page在页表中不存在
  if (iter == page_table_.end()) {
    return true;
  }
  // 2 该page在页表中存在
  frame_id_t frame_id = iter->second;  // iter是pair类型，其second是page_id对应的frame_id
  Page *page = &pages_[frame_id];      // 由frame_id得到page
  if (page->GetPinCount() > 0) {
    return false;
  }
  disk_manager_->DeallocatePage(page_id);
  UpdatePage(page, INVALID_PAGE_ID, frame_id);
  page->pin_count_ = 0;            // 删除page后，pin_count置0
  free_list_.push_back(frame_id);  // 加到尾部
  return true;
}

/**
 * Flushes all the pages in the buffer pool to disk.
 */
void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  std::scoped_lock lock{latch_};
  for (size_t i = 0; i < pool_size_; i++) {
    FlushPageImpl(i);
  }
}

}  // namespace bustub
