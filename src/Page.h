#ifndef __PAGE_H
#define __PAGE_H

#include <atomic>
#include <vector>
#include <shared_mutex>
#include <memory>
#include <cstdlib>
#include "List.h"
#include "FileManager.h"
#include "common.h"

namespace bptdb {

struct PageHeader {
    u32    hdrpages;
    u32    realpages;
    u32    bytes;   ///< 总字节数
    u32    checksum;
    pgid_t res;
    u32    size;
    pgid_t next;

    static void init(PageHeader *hdr, u32 len, pgid_t next) {
        hdr->hdrpages  = len;
        hdr->realpages = len;
        hdr->bytes     = sizeof(PageHeader);
        hdr->checksum  = 0;
        hdr->res       = 0;
        hdr->size      = 0;
        hdr->next      = next;
    }
};


class PageAllocator;

class Page { 
public:
    Page(pgid_t id, u32 page_size, u32 data_pgs);
    Page(pgid_t id, u32 page_size);
    ~Page();
    void *read(FileManager *fm);
    void *extend(PageAllocator *pa, u32 extbytes);
    void write(FileManager *fm);
    void free(PageAllocator *pa);
    static void writeBatch(FileManager *fm, 
                           std::vector<std::shared_ptr<Page>> &pages);
    bool   overFlow(u32 extbytes);
    void   *data() { return _data; }
    pgid_t getId() { return _id; }
    tag_declare(lru_tag, Page, _lru_tag); 
private:
    u32    byte2page(u32 bytes);
    // only write buffer
    void _write(FileManager *fm);
    // read by page_size
    void _readPage(FileManager *fm, char *buf, u32 cnt, u32 pos);
    // write by page_size
    void _writePage(FileManager *fm, char *buf, u32 cnt, u32 pos);

    pgid_t  _id{0};
    u32     _page_size{0};
    u32     _data_pgs{0}; // page len of _data
    char    *_data{nullptr};
    ListTag _lru_tag;
};

using PagePtr = std::shared_ptr<Page>;

}// namespace bptdb

#endif

