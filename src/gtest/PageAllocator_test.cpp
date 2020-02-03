#include <gtest/gtest.h>
#include "../PageAllocator.h"
using namespace bptdb;

TEST(TestPageAllocator, TestAllocFree) {
    u32 page_size = 4096;
    FileManager fm("my.db");
    //PageAllocator::newOnDisk(0, &fm, page_size, 1);
    PageAllocator pa(0, &fm, page_size);
    //ASSERT_EQ(pa.allocPage(2), 1);
    //ASSERT_EQ(pa.allocPage(4), 3);
    //ASSERT_EQ(pa.allocPage(1), 7);
    //ASSERT_EQ(pa.allocPage(1), 8);

    pa.freePage(3, 4);
    pa.freePage(8, 1);
    pa.freePage(1, 2);
    pa.freePage(7, 1);
    //ASSERT_EQ(pa.allocPage(6), 1);
}
