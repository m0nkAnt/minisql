#include "page/bitmap_page.h"

#include "glog/logging.h"

template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
    size_t max_page_size = BitmapPage<PageSize>::MaxPageSize();
    for (uint32_t i = 0; i < max_page_size; ++i) {
        // 在循环中查找空余的位置
        if (IsPageFree(i)) {
            size_t bytes_index = i / 8; // 将 i 替换为 page_offset
            uint8_t bit_index = i % 8;
            // 将指定的位设置为1，表示该页已经被分配
            bytes[bytes_index] |= (1 << bit_index);
            page_offset = i; // 将 page_offset 赋值为 i
            return true;
        }
    }
    return false;
}

template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
    if (page_offset >= PageSize)
        return false; // 如果偏移量大于等于页面大小，则返回false

    uint32_t byte_index = page_offset / 8;
    uint8_t bit_index = page_offset % 8;
    // 如果指定位为1则说明该页已经被分配，然后将指定的位设置为0，表示该页已经被释放
    if (!IsPageFree(page_offset)) { // 修正 if 语句
        bytes[byte_index] &= ~(1 << bit_index);
        return true;
    }
    return false;
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
    if (page_offset >= PageSize)
        return false; // 如果偏移量大于等于页面大小，则返回false
    uint32_t byte_index = page_offset / 8;
    uint8_t bit_index = page_offset % 8;
    return IsPageFreeLow(byte_index, bit_index);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
    return !(bytes[byte_index] & (1 << bit_index));
}

template class BitmapPage<64>;
template class BitmapPage<128>;
template class BitmapPage<256>;
template class BitmapPage<512>;
template class BitmapPage<1024>;
template class BitmapPage<2048>;
template class BitmapPage<4096>;
