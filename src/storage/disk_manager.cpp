#include "storage/disk_manager.h"

#include <sys/stat.h>

#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::AllocatePage() {
  DiskFileMetaPage  *Meta_info;
  // 从meta_data_中读取meta信息
  Meta_info = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  size_t extent_page_num = Meta_info->GetExtentNums();
  uint32_t extent_now;
  // 查找空闲的extent
  for(extent_now = 0; extent_now < extent_page_num && Meta_info->extent_used_page_[extent_now] == BITMAP_SIZE; extent_now++);
  //如果此时没有空闲的extent，则增加一个extent
  if(extent_now == extent_page_num) {
    Meta_info->num_extents_++;
    Meta_info->extent_used_page_[extent_now] = 0;
  }
  BitmapPage<PAGE_SIZE> *bitmap_page;

  char BitmapPageData[PAGE_SIZE];
  // 找到对应的bitmap page
  char meta_page_data[PAGE_SIZE];
  ReadPhysicalPage(0, meta_page_data);

  page_id_t bitmap_physical_page_id = extent_now * (BITMAP_SIZE+1) + 1;
  ReadPhysicalPage( bitmap_physical_page_id, BitmapPageData);
  bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(BitmapPageData);
  // 遍历当前分区的bitmap page，找到第一个空闲的page
  uint32_t offset ;
  if(!bitmap_page->AllocatePage(offset)) {
    return INVALID_PAGE_ID;
  }
  else {
    Meta_info->num_allocated_pages_++;
    Meta_info->extent_used_page_[extent_now]++;
    WritePhysicalPage(extent_now * (BITMAP_SIZE+1) + 1, reinterpret_cast<char *>(bitmap_page));
    return (extent_now * BITMAP_SIZE + offset) ;
  }

}

/**
 * TODO: Student Implement
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  DiskFileMetaPage  *Meta_info;
  Meta_info = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  Meta_info->num_allocated_pages_--;

  size_t n = BITMAP_SIZE;
  page_id_t physical_pageid = MapPageId(logical_page_id);
  page_id_t physical_pageid_of_bitmap = physical_pageid - (logical_page_id % n + 1);

  char BitmapPageData[PAGE_SIZE];
  ReadPhysicalPage(physical_pageid_of_bitmap, BitmapPageData);
  BitmapPage<PAGE_SIZE> *bitmap_page;
  bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(BitmapPageData);
  size_t offset = logical_page_id % n;

  if(bitmap_page->DeAllocatePage(offset)) {
    size_t extent_num = logical_page_id / n;
    Meta_info->extent_used_page_[extent_num]--;
    WritePhysicalPage(physical_pageid_of_bitmap, reinterpret_cast<char *>(BitmapPageData));
  }
}
/**
 * TODO: Student Implement
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  size_t n = BITMAP_SIZE;
  page_id_t physical_pageid = MapPageId(logical_page_id);
  page_id_t physical_pageid_of_bitmap = physical_pageid - (logical_page_id % n + 1);

  char BitmapPageData[PAGE_SIZE];
  ReadPhysicalPage(physical_pageid_of_bitmap, BitmapPageData);
  BitmapPage<PAGE_SIZE> *bitmap_page;
  bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(BitmapPageData);
  size_t offset = logical_page_id % n;
  return bitmap_page->IsPageFree(offset);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  size_t n = BITMAP_SIZE;
  size_t offset = (logical_page_id / n) * (n+1);
  offset += logical_page_id % n;
  offset += 2;
  return offset;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}

