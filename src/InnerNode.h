#ifndef __INNER_NODE_H
#define __INNER_NODE_H

#include "__Node.h"
#include "InnerContainer.h"

namespace bptdb {

template <typename KType, typename Comp>
class InnerNode: public Node {
public:
    //using UnRLockGuardVec_t = std::vector<UnReadLockGuard>;
    using UnWLockGuardVec_t = UnLockGuardArray<UnWriteLockGuard>;
    using Mutex_t = std::shared_mutex;

    InnerNode(pgid_t id, u32 maxsize, DB *db, 
            NodeMap<InnerNode> *map): Node(id, maxsize, db){
        _map = map;
    }

    // the mutex must locked by func get() at here.
    Status put(u32 pos, KType &key, pgid_t &val, 
            PutEntry<KType> &entry, UnWLockGuardVec_t &lg_tlb) {

        auto [hdr, pg] = handlePage(_container);
        auto extbytes = _container.elemSize(key, val);
        // there enought space
        if(pg->overFlow(extbytes)) {
            hdr = (PageHeader *)pg->extend(_db->getPageAllocator(), extbytes);
            containerReset(hdr, _container);
        }
        _container.putat(pos, key, val);

        if(ifsplit(hdr)) {
            DEBUGOUT("===> innernode split");
            split(hdr, entry, _container);
            entry.update = true;
        }
        pg->write(_db->getFileManager());
        // unlock self.
        lg_tlb.pop_back();
        return Status();
    }

    // the mutex must locked by func get() at here.
    void del(u32 pos, DelEntry<KType> &entry,
            UnWLockGuardVec_t &lg_tlb) {

        auto [hdr, pg] = handlePage(_container);
        // delete key at pos
        _container.delat(pos);

        auto &next_container = _map->get(hdr->next)->_container;
        // if legal or we are the last child of parent
        // return once.
        if(!ifmerge(hdr) || entry.last || !hdr->next) {
            goto done;
        }

        DEBUGOUT("===> innernode borrow");
        if(borrow(hdr, pg, entry, _container, next_container)) {
            goto done;
        }

        DEBUGOUT("===> innernode merge");
        _map->del(merge(hdr, pg, entry, _container, next_container));

done:
        pg->write(_db->getFileManager());
        lg_tlb.pop_back();
    }

    // the mutex must locked by func get() at here.
    void update(u32 pos, KType &newkey, 
            UnWLockGuardVec_t &lg_tlb) {

        auto [hdr, pg] = handlePage(_container);
        auto extbytes = _container.elemSize(newkey, 0);
        if(pg->overFlow(extbytes)) {
            hdr = (PageHeader *)pg->extend(_db->getPageAllocator(), extbytes);
            containerReset(hdr, _container);
        }
        _container.updateKeyat(pos, newkey);

        pg->write(_db->getFileManager());
        lg_tlb.pop_back();
    }

    // for put
    std::tuple<pgid_t, u32> 
    get(KType &key, UnWLockGuardVec_t &lg_tlb) {

        _shmtx.lock();
        auto [hdr, pg] = handlePage(_container);
        if(safetoput(hdr)) {
            lg_tlb.clear();
        }
        lg_tlb.emplace_back(_shmtx);

        handlePage(_container);
        return _container.get(key);
    }

    // for del
    std::tuple<pgid_t, u32> 
    get(KType &key, DelEntry<KType> &entry, 
        UnWLockGuardVec_t &lg_tlb) {

        _shmtx.lock();
        auto [hdr, pg] = handlePage(_container);
        if(safetodel(hdr)) {
            lg_tlb.clear();
        }
        lg_tlb.emplace_back(_shmtx);

        handlePage(_container);
        return _container.get(key, entry);
    }

    // for search
    std::tuple<pgid_t, u32> 
    get(KType &key, Mutex_t &par_mtx) {

        //lock self and release parent.
        _shmtx.lock_shared();
        par_mtx.unlock_shared();

        handlePage(_container);
        return _container.get(key);
    }

    //==================================================

    static void newOnDisk(
        pgid_t id, FileManager *fm, u32 page_size, 
        KType &key, pgid_t child1, pgid_t child2) {

        Page pg(id, page_size, 1);
        auto hdr = (PageHeader *)pg.data();
        PageHeader::init(hdr, 1, 0);

        // 初始化容器
        ContainerType con;
        containerReset(hdr, con);
        con.init(key, child1, child2);

        pg.write(fm);
    }

    //InnerNode *next() {
    //    auto [hdr, pg] = handlePage(_container);
    //    if(hdr->next == 0) {
    //        return nullptr;
    //    }
    //    return _map->get(hdr->next);
    //}

    bool empty() {
        auto [hdr, pg] = handlePage(_container);
        return hdr->size == 0;
    }

    // XXX
    // return the only child in node
    pgid_t tochild() {
        //std::cout << "tochild\n";
        auto [hdr, pg] = handlePage(_container);
        assert(hdr->next == 0);
        auto ret =  _container.head();
        // free self page
        pg->free(_db->getPageAllocator());
        _db->getPageCache()->del(ret);
        return ret;
    }
private:
    using ContainerType = InnerContainer<KType, Comp>;
    ContainerType _container;
    NodeMap<InnerNode> *_map;
};

}// namespace bptdb

#endif
