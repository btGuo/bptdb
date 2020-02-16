#ifndef __PAGE_ALLOCATOR_H
#define __PAGE_ALLOCATOR_H

#include <algorithm>
#include <mutex>
#include <memory>
#include "common.h"
#include "FileManager.h"
#include "Page.h"

namespace bptdb {

class PageAllocator {
public:
    struct Elem {
        pgid_t pos;
        u32    len;
    }; 
    static void newOnDisk(pgid_t root, FileManager *fm, 
            u32 page_size, u32 start_pos);
    PageAllocator(pgid_t root, FileManager *fm, u32 page_size);
    pgid_t allocPage(u32 len);
    // free page at pos of len.
    void freePage(pgid_t pos, u32 len);
    pgid_t reallocPage(pgid_t pos, u32 len, u32 newlen);
    void show();
private:
    // extend page of self.
    void *extendPage(u32 extbytes);
    // if p1 and p2 could merge
    bool adjacent(Elem *p1, Elem *p2) {
        return p1->pos + p1->len == p2->pos;
    }
    u32                   _page_size{0};
    pgid_t                _root{0};
    FileManager           *_fm{nullptr};
    Elem                  _tmp;
    std::recursive_mutex  _mtx;
    std::unique_ptr<Page> _pg{nullptr};
};

}// namespace bptdb

#endif
