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
 
class Page {
public:
    Page(pgid_t id): _id(id){
        _data = std::malloc(g_option.page_size);
        g_fm->read((char*)_data, g_option.page_size, _id * g_option.page_size);
    }
    ~Page() { 
        if (_dirty) {
            g_fm->write((char*)_data, g_option.page_size, _id * g_option.page_size);
        }
        std::free(_data); 
    }
    void read(void *dest) { 
        std::shared_lock lg(_shmtx);
        std::memcpy(dest, _data, g_option.page_size); 
    }
    void write(void *src) {
        std::unique_lock lg(_shmtx);
        std::memcpy(_data, src, g_option.page_size);
        _dirty = true;
    }
    void flush() {
        std::shared_lock lg(_shmtx);
        if (!_dirty) {
            return;
        }
        g_fm->write((char*)_data, g_option.page_size, _id * g_option.page_size);
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

