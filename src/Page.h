#ifndef __PAGE_H
#define __PAGE_H

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <shared_mutex>

#include "FileManager.h"
#include "common.h"
#include "List.h"
#include "Option.h"

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
    
    void show() {
        DEBUGOUT("hdrpages %d, bytes %d, realpages %d", hdrpages, bytes, realpages);
    }
};

class Page {
public:
    Page(pgid_t id): _id(id){
    }
    ~Page() { 
        if (_data) {
            std::free(_data); 
        }
    }
    void read(void *dest) { 
        std::shared_lock lg(_shmtx);
        if (!_data) {
            _data = std::malloc(g_option.page_size);
            g_fm->read((char*)_data, g_option.page_size, _id * g_option.page_size);
        }
        std::memcpy(dest, _data, g_option.page_size); 
    }
    void write(void *src) {
        std::unique_lock lg(_shmtx);
        if (!_data) {
            _data = std::malloc(g_option.page_size);
        }
        std::memcpy(_data, src, g_option.page_size);
        _dirty = true;
    }
    void flush() {
        std::shared_lock lg(_shmtx);
        if (!_dirty) {
            return;
        }
        if (_data) {
            g_fm->write((char*)_data, g_option.page_size, _id * g_option.page_size);
        }
        _dirty.store(false);
    }
    pgid_t getId() {
        return _id;
    }
    bool dirty() {
        return _dirty.load();
    }
    tag_declare(lru_tag, Page, _lru_tag);
private:
    pgid_t _id{0};
    void   *_data{nullptr};
    ListTag _lru_tag;
    std::shared_mutex _shmtx;
    std::atomic_bool  _dirty{false};
};

using PagePtr = std::shared_ptr<Page>;

}// namespace bptdb

#endif

