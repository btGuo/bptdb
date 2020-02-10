#ifndef __NODE_H
#define __NODE_H

#include <vector>
#include <type_traits>
#include <cstring>
#include <shared_mutex>
#include <set>
#include <memory>
#include <tuple>
#include <cstdlib>
#include <iostream>

#include "common.h"
#include "Status.h"
#include "FileManager.h"
#include "PageAllocator.h"
#include "PageCache.h"
#include "Page.h"
#include "DB.h"

namespace bptdb {

template <typename NodeType>
class NodeMap {
public:
    NodeMap(u32 order, DB *db) {
        _db = db;
        _order = order;
    }
    NodeType *get(pgid_t id) {
        std::lock_guard<std::mutex> lg(_mtx);
        auto ret = _map.find(id);
        if(ret != _map.end()) {
            return ret->second.get();
        }
        auto node = std::make_unique<NodeType>(id, _order, _db, this);
        auto raw = node.get();
        _map.insert({id, std::move(node)});
        return raw;
    }
    void del(pgid_t id) {
        std::lock_guard<std::mutex> lg(_mtx);
        _map.erase(id);
    }
private:
    u32 _order{0};
    DB *_db{nullptr};
    std::mutex _mtx;
    std::unordered_map<pgid_t, 
        std::unique_ptr<NodeType>>  _map;
};

template <typename KType>
struct PutEntry {
    bool update{false};
    KType  key;
    pgid_t val;
};

template <typename KType>
struct DelEntry {
    bool update{false}; // child to parent
    bool del{false};    // child to parent
    KType key;     // child to parent
    bool last{false};  // parent to child
    KType delim;   // parent to child
};

class Node {
public:
    Node(pgid_t id, u32 maxsize, DB *db) {
        _id      = id;
        _maxsize = maxsize;
        _db      = db;
    }
    std::shared_mutex &getMutex() {
        return _shmtx;
    }
protected:
    template <typename ContainerType>
    static void containerReset(PageHeader *hdr, ContainerType &con) {
        con.reset(&hdr->bytes, &hdr->size, (char *)(hdr + 1));
    }

    template <typename ContainerType, typename KType>
    void split(PageHeader *hdr, PutEntry<KType> &entry, 
               ContainerType &container) {

        u32 len = byte2page(hdr->bytes);
        auto pg = _db->getPageCache()->alloc(
            _db->getPageAllocator()->allocPage(len),
            _db->getPageSize(), len);
        auto newhdr = (PageHeader *)pg->data();

        PageHeader::init(newhdr, len, hdr->next); 
        hdr->next = pg->getId();

        ContainerType other;
        containerReset(newhdr, other);

        entry.val = pg->getId();
        entry.key = container.splitTo(other);
        
        pg->write(_db->getFileManager());
    }

    template <typename ContainerType, typename KType>
    bool borrow(PageHeader *hdr, PagePtr pg, 
            DelEntry<KType> &entry, 
            ContainerType &container,
            ContainerType &next_container) {
        
        auto [next_hdr, next_pg] = handlePage(next_container, hdr->next);

        if(!hasmore(next_hdr)) {
            return false;
        }

        auto extbytes = container.elemSize(
                next_container.key(0), next_container.val(0));
        if(pg->overFlow(extbytes)) {
            //std::cout << "page realloc\n";
            hdr = (PageHeader *)pg->extend(_db->getPageAllocator(), extbytes);
            containerReset(hdr, container);
        }

        entry.key = container.borrowFrom(next_container, entry.delim);
        entry.update = true;
        pg->write(_db->getFileManager());
        return true;
    }

    template <typename ContainerType, typename KType>
    pgid_t merge(PageHeader *hdr, PagePtr pg, 
            DelEntry<KType> &entry, 
            ContainerType &container,
            ContainerType &next_container) {

        auto [next_hdr, next_pg] = handlePage(next_container, hdr->next);
        // not exact.
        auto extbytes = next_hdr->bytes;
        if(pg->overFlow(extbytes)) {
            hdr = (PageHeader *)pg->extend(_db->getPageAllocator(), extbytes);
            containerReset(hdr, container);
        }
        container.mergeFrom(next_container, entry.delim);
        entry.del = true;

        auto ret = hdr->next;
        // set next
        hdr->next = next_hdr->next;
        // 1. free page buffer on memory and page id on disk
        next_pg->free(_db->getPageAllocator());
        // 2. del page at cache
        _db->getPageCache()->del(ret);
        return ret;
    }

    template <typename ContainerType>
    std::tuple<PageHeader *, PagePtr > handlePage(ContainerType &container) {
        return handlePage(container, _id);
    }

    template <typename ContainerType>
    std::tuple<PageHeader *, PagePtr> handlePage(ContainerType &container, pgid_t id) {
        PageHeader *hdr = nullptr;
        PagePtr pg = _db->getPageCache()->get(id);
        if(!pg) {
            pg = _db->getPageCache()->alloc(id, _db->getPageSize());
            //_db->getPageCache()->put(pg);
            hdr = (PageHeader *)pg->read(_db->getFileManager());
            containerReset(hdr, container);
        }else {
            hdr = (PageHeader *)pg->data();
            if(container.raw()) {
                containerReset(hdr, container);
            }
        }
        return std::make_tuple(hdr, pg);
    }
    u32 byte2page(u32 bytes) {
        return (bytes + _db->getPageSize() - 1) / _db->getPageSize();
    }
    bool safetoput(PageHeader *hdr) {
        return hdr->size < _maxsize;
    }
    bool ifsplit(PageHeader *hdr) {
        return hdr->size > _maxsize;
    }
    bool safetodel(PageHeader *hdr) {
        return hdr->size > _maxsize / 2;
    }
    bool ifmerge(PageHeader *hdr) {
        return hdr->size < _maxsize / 2;
    }
    bool hasmore(PageHeader *hdr) {
        return hdr->size > _maxsize / 2;
    }

    pgid_t            _id{0};
    DB                *_db{nullptr};
    u32               _maxsize{0};
    std::shared_mutex _shmtx;
};

}// namespace bptdb

#endif
