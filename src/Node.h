#ifndef __NODE_H
#define __NODE_H

#include <type_traits>
#include <common.h>
#include <cstring>
#include <shared_mutex>

#include "Status.h"
#include "FileManager.h"
#include "PageCache.h"
#include "Page.h"
#include "DB.h"
//#include "PageAllocator.h"

namespace bptdb {


template <typename KType>
struct PutEntry {
    bool update{false};
    KType  key;
    pgid_t val;
};

template <typename ContainerType>
class Node {
public:
    using KType = typename ContainerType::key_t;
    using VType = typename ContainerType::value_t;
    using Iter_t = typename ContainerType::Iterator;
    enum class NodeType {
        Leaf, Inner,
    };
    enum NodeHeaderFlagMask {
        _nodeTypeMask = 1,
    };
    struct NodeHeader {
        Page::PageHeader pghdr;
        u16 flag;
        pgid_t        next;
        u32 size;    ///< key 个数
    };

    /**
     * 构造函数
     * @param id 节点位置
     * @param tree 所在的树
     */
    Node(pgid_t id, u32 maxsize, DB *db) {
        _id      = id;
        _tree    = tree;
        _maxsize = maxsize;
        _db      = db;
    }
    /**
     * 初始化节点头部
     * @param hdr 待初始化项
     * @param type 节点类型
     * @param len 初始节点长度
     * @param next 该节点的next节点号
     */
    static void initHeader(NodeHeader *hdr, NodeType type, u32 len, pgid_t next) {
        if(type == NodeType::Leaf) {
            hdr->flag |= _nodeTypeMask;
        }
        hdr->size = 0;
        hdr->next = next;
        hdr->pghdr.bytes = sizeof(NodeHeader);
    }
    static void newLeafNodeOnDisk(pgid_t id, DB *db, u32 len = 1) {
        Page *pg = Page::newRaw(id, db, len);
        auto hdr = (NodeHeader *)pg->data();
        initHeader(hdr, NodeType::Leaf, len, 0);
        pg->writeForce();
        _db->getPageCache()->put(pg);
    }
    static void newInnerNodeOnDisk(KType &key, pgid_t child1, 
            pgid_t child2, DB *db, u32 len = 1) {

        pgid_t id = _db->getPageAllocator()->allocPage(len);
        auto pg = Page::newRaw(id, db, len);
        auto hdr = (NodeHeader *)pg->data();
        initHeader(hdr, NodeType::Inner, len, 0);
        ContainerType con;
        containerReset(hdr, con);
        con.init(key, child1, child2);
        pg->writeForce();
        _db->getPageCache()->put(pg);
    }
    /**
     * 分裂节点
     */
    void split(NodeHeader *hdr, PutEntry<KType> &entry) {
        u32 len = byte2page(hdr->pghdr.bytes);
        pgid_t id = _db->getPageAllocator()->allocPage(len);
        entry.val = id;
        auto pg = Page::newRaw(id, _db, len);

        auto nhdr = (NodeHeader *)pg->data();
        initHeader(hdr, NodeType::Leaf, len, hdr->next); 
        hdr->next = id;

        ContainerType other;
        containerReset(nhdr, other);
        entry.key = _container.splitTo(other);
        _db->getPageCache()->put(pg);
    }

    Status put(KType &key, VType &val, PutEntry<KType> &entry) {
        Page *pg = _pc->get(_id);
        NodeHeader *hdr = nullptr;
        if(pg == nullptr) {
            pg = Page::newFromDisk(_id, _db);
            hdr = (NodeHeader *)pg->read();
        }else {
            hdr = (NodeHeader *)pg->data();
        }

        this->containerReset(hdr, _container);

        if(_container.find(key)) {
            return Status(error::keyRepeat);
        }

        auto extbytes = _container.elemSize(key, val);
        // there enought space
        if(pg->overflow(extbytes)) {
            hdr = pg->extend(extbytes);
            this->containerReset(hdr, _container);
        }
        _container.put(key, val);

        if(!this->legal(hdr)) {
            this->split(hdr, entry);
        }
        return Status();
    } 

    Status get(KType &key, VType &val) {
        return Status();
    }

    void updateIter(Iter_t &iter, IterMoveType type) {
        // lock here
        _container->updateIter(iter, type);    
    }

    Iter_t begin() {
        return _container.begin();
    }
    Iter_t upper(KType &key) {
        return _container.upper(key);
    }
    Iter_t lower(KType &key) {
        return _container.lower(key);
    }
    Iter_t at(KType &key) {
        return _container.at(key);
    }

    /**
     * 是否叶子节点
     */
    bool leaf(NodeHeader *hdr) {
        return (hdr->flag & _nodeTypeMask);
    }
    /**
     * 是否为空
     */
    bool empty(NodeHeader *hdr) {
        return hdr->size == 0;
    }
    /**
     * 字节数转换为页数
     */
    u32 byte2page(u32 bytes) {
        return (bytes + _db->getPageSize() - 1) / _db->getPageSize();
    }
    /**
     * 性质是否满足
     */
    bool legal(NodeHeader *hdr) {
        return hdr->size <= _maxsize;
    }
    static void containerReset(NodeHeader *hdr, ContainerType &con) {
        con.reset(&hdr->pghdr.bytes,
                        &hdr->size, (char *)(hdr + 1));
    }
    void lock() {
        _shmtx.lock();
    }
    void unlock() {
        _shmtx.unlock();
    }
    void lock_shared() {
        _shmtx.lock_shared();
    }
    void unlock_shared() {
        _shmtx.unlock_shared();
    }
protected:
    pgid_t            _id{0};
    DB                *_db{nullptr};
    u32               _maxsize{0};
    ContainerType     _container{nullptr};
    std::shared_mutex _shmtx;
};

}// namespace bptdb
#endif
