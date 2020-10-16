#ifndef __PAGE_HEADER_H
#define __PAGE_HEADER_H

#include "common.h"
#include "PageHelper.h"

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

    static void newOnDisk(pgid_t id, u32 len = 1, pgid_t next = 0) {
        PageHelper pg(id, 1);
        auto hdr = (PageHeader *)pg.data();
        PageHeader::init(hdr, len, next);
        pg.write();
    }
};


}// namespace bptdb

#endif
