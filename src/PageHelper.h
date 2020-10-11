#ifndef __PAGE_HELPER_H
#define __PAGE_HELPER_H

#include <atomic>
#include <vector>
#include <shared_mutex>
#include <memory>
#include <cstdlib>
#include "common.h"

namespace bptdb {

class PageAllocator;

class PageHelper { 
    friend class PageAllocator;
public:
    PageHelper(pgid_t id, u32 data_pgs);
    PageHelper(pgid_t id);
    ~PageHelper();
    void *read();
    void *extend(u32 extbytes);
    void write();
    void free();
    static void writeBatch(std::vector<std::shared_ptr<PageHelper>> &pages);
    bool   overFlow(u32 extbytes);
    void   *data() { return _data; }
    pgid_t getId() { return _id; }

private:
    u32    byte2page(u32 bytes);
    // read by page_size
    void _readPage(char *buf, u32 cnt, u32 pos);
    // write by page_size
    void _writePage(char *buf, u32 cnt, u32 pos);

    pgid_t  _id{0};
    u32     _data_pgs{0}; // page len of _data
    char    *_data{nullptr};
};

using PageHelperPtr = std::shared_ptr<PageHelper>;

}// namespace bptdb

#endif
