//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.h
//
// Identification: src/include/buffer/buffer_pool_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>

#include "buffer/lru_replacer.h"
#include "recovery/log_manager.h"
#include "storage/disk/disk_manager.h"
#include "storage/page/page.h"

/* PROJECT #1 - BUFFER POOL | TASK #2 - BUFFER POOL MANAGER
您需要在系统中实现BufferPoolManager。
BufferPoolManager负责从DiskManager读取数据库页并将它们存储在内存中。
BufferPoolManager也可以在明确指示这样做或当它需要驱逐页面为新页面腾出空间时，将脏页面写入磁盘。
为了确保您的实现与系统的其余部分一起正常工作，我们将为您提供一些已经填充的功能。
您也不需要实现实际将数据读取和写入磁盘的代码（这在我们的实现中称为DiskManager）。我们将为您提供该功能。
系统中的所有内存页面都由Page对象表示。在BufferPoolManager并不需要了解这些页面的内容。
但作为系统开发人员，了解Page对象只是缓冲池中内存的容器，因此并不特定于唯一页面，这一点很重要。
也就是说，每个Page对象都包含一块内存，DiskManager将使用该内存块来复制它从磁盘读取的物理页的内容。
当它来回移动到磁盘时，BufferPoolManager将重用相同的Page对象来存储数据。
这意味着在系统的整个生命周期中，同一个Page对象可能包含不同的物理页面。
该Page对象的标识符(page_id)会跟踪它包含的物理页面；如果Page对象不包含物理页，则其page_id必须设置为INVALID_PAGE_ID。
每个Page对象还为“固定”该页面的线程数维护一个计数器。您的BufferPoolManager不允许释放被固定的页面，并且跟踪每个Page对象是否脏。
您的工作是在取消固定页面之前记录页面是否被修改。您的BufferPoolManager必须先将脏页的内容写回磁盘，然后才能重用该对象。
您的BufferPoolManager实现将使用您在本作业的前面步骤中创建的LRUReplacer类。
使用LRUReplacer来跟踪何时访问Page对象，以便可以决定在必须释放frame以腾出空间来从磁盘复制新物理页时驱逐哪个对象。
*/

namespace bustub {

/**
 * 主要数据结构是一个page数组(pages_)，frame_id作为其下标。
 * 还有一个哈希表(page_table_)，表示从page_id到frame_id的映射。
 * BufferPoolManager reads disk pages to and from its internal buffer pool.
 */
class BufferPoolManager {
 public:
  enum class CallbackType { BEFORE, AFTER };
  using bufferpool_callback_fn = void (*)(enum CallbackType, const page_id_t page_id);

  /**
   * Creates a new BufferPoolManager.
   * @param pool_size the size of the buffer pool
   * @param disk_manager the disk manager
   * @param log_manager the log manager (for testing only: nullptr = disable logging)
   */
  BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager = nullptr);

  /**
   * Destroys an existing BufferPoolManager.
   */
  ~BufferPoolManager();

  /** Grading function. Do not modify! */
  Page *FetchPage(page_id_t page_id, bufferpool_callback_fn callback = nullptr) {
    GradingCallback(callback, CallbackType::BEFORE, page_id);
    auto *result = FetchPageImpl(page_id);
    GradingCallback(callback, CallbackType::AFTER, page_id);
    return result;
  }

  /** Grading function. Do not modify! */
  bool UnpinPage(page_id_t page_id, bool is_dirty, bufferpool_callback_fn callback = nullptr) {
    GradingCallback(callback, CallbackType::BEFORE, page_id);
    auto result = UnpinPageImpl(page_id, is_dirty);
    GradingCallback(callback, CallbackType::AFTER, page_id);
    return result;
  }

  /** Grading function. Do not modify! */
  bool FlushPage(page_id_t page_id, bufferpool_callback_fn callback = nullptr) {
    GradingCallback(callback, CallbackType::BEFORE, page_id);
    auto result = FlushPageImpl(page_id);
    GradingCallback(callback, CallbackType::AFTER, page_id);
    return result;
  }

  /** Grading function. Do not modify! */
  Page *NewPage(page_id_t *page_id, bufferpool_callback_fn callback = nullptr) {
    GradingCallback(callback, CallbackType::BEFORE, INVALID_PAGE_ID);
    auto *result = NewPageImpl(page_id);
    GradingCallback(callback, CallbackType::AFTER, *page_id);
    return result;
  }

  /** Grading function. Do not modify! */
  bool DeletePage(page_id_t page_id, bufferpool_callback_fn callback = nullptr) {
    GradingCallback(callback, CallbackType::BEFORE, page_id);
    auto result = DeletePageImpl(page_id);
    GradingCallback(callback, CallbackType::AFTER, page_id);
    return result;
  }

  /** Grading function. Do not modify! */
  void FlushAllPages(bufferpool_callback_fn callback = nullptr) {
    GradingCallback(callback, CallbackType::BEFORE, INVALID_PAGE_ID);
    FlushAllPagesImpl();
    GradingCallback(callback, CallbackType::AFTER, INVALID_PAGE_ID);
  }

  /** @return pointer to all the pages in the buffer pool */
  Page *GetPages() { return pages_; }

  /** @return size of the buffer pool */
  size_t GetPoolSize() { return pool_size_; }

 protected:
  /**
   * Grading function. Do not modify!
   * Invokes the callback function if it is not null.
   * @param callback callback function to be invoked
   * @param callback_type BEFORE or AFTER
   * @param page_id the page id to invoke the callback with
   */
  void GradingCallback(bufferpool_callback_fn callback, CallbackType callback_type, page_id_t page_id) {
    if (callback != nullptr) {
      callback(callback_type, page_id);
    }
  }

  /**
   * Fetch the requested page from the buffer pool.
   * @param page_id id of page to be fetched
   * @return the requested page
   */
  Page *FetchPageImpl(page_id_t page_id);

  /**
   * Unpin the target page from the buffer pool.
   * @param page_id id of page to be unpinned
   * @param is_dirty true if the page should be marked as dirty, false otherwise
   * @return false if the page pin count is <= 0 before this call, true otherwise
   */
  bool UnpinPageImpl(page_id_t page_id, bool is_dirty);

  /**
   * Flushes the target page to disk.
   * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
   * @return false if the page could not be found in the page table, true otherwise
   */
  bool FlushPageImpl(page_id_t page_id);

  /**
   * Creates a new page in the buffer pool.
   * @param[out] page_id id of created page
   * @return nullptr if no new pages could be created, otherwise pointer to new page
   */
  Page *NewPageImpl(page_id_t *page_id);

  /**
   * Deletes a page from the buffer pool.
   * @param page_id id of page to be deleted
   * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
   */
  bool DeletePageImpl(page_id_t page_id);

  /**
   * Flushes all the pages in the buffer pool to disk.
   */
  void FlushAllPagesImpl();

  bool FindVictimPage(frame_id_t *frame_id);
  void UpdatePage(Page *page, page_id_t page_id, frame_id_t frame_id);

  /** Number of pages in the buffer pool. */
  size_t pool_size_;
  /** Array of buffer pool pages. 大小为pool_size_，下标为[0,pool_size_) */
  Page *pages_;
  /** Pointer to the disk manager. */
  DiskManager *disk_manager_ __attribute__((__unused__));
  /** Pointer to the log manager. */
  LogManager *log_manager_ __attribute__((__unused__));
  /** Page table for keeping track of buffer pool pages. */
  std::unordered_map<page_id_t, frame_id_t> page_table_;
  /** Replacer to find unpinned pages for replacement. 大小为pool_size_*/
  Replacer *replacer_;
  /** List of free pages. 最开始，所有页都在free_list中*/
  std::list<frame_id_t> free_list_;
  /** This latch protects shared data structures. We recommend updating this comment to describe what it protects. */
  std::mutex latch_;
};
}  // namespace bustub
