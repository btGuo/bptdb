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
    NodeMap(u32 order, DB *db, comparator_t cmp) {
        _db = db;
        _order = order;
        _cmp = cmp;
    }
    NodeType *get(pgid_t id) {
        std::lock_guard<std::mutex> lg(_mtx);
        auto ret = _map.find(id);
        if(ret != _map.end()) {
            return ret->second.get();
        }
        auto node = std::make_unique<NodeType>(id, _order, _db, this, _cmp);
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
    comparator_t _cmp;
    std::mutex _mtx;
    std::unordered_map<pgid_t, 
        std::unique_ptr<NodeType>>  _map;
};

struct PutEntry {
    bool update{false};
    std::string  key;
    pgid_t val;
};

struct DelEntry {
    bool update{false}; // child to parent
    bool del{false};    // child to parent
    std::string key;     // child to parent
    bool last{false};  // parent to child
    std::string delim;   // parent to child
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
    u32 byte2page(u32 bytes) {
        return (bytes + _db->getPageSize() - 1) / _db->getPageSize();
    }

    pgid_t            _id{0};
    DB                *_db{nullptr};
    u32               _maxsize{0};
    std::shared_mutex _shmtx;
};

}// namespace bptdb

#endif
