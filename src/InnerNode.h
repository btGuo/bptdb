#ifndef __INNER_NODE_H
#define __INNER_NODE_H

#include "Node.h"
#include "InnerContainer.h"
#include "LockHelper.h"

namespace bptdb {

class InnerNode: public Node {
public:
    //using UnRLockGuardVec_t = std::vector<UnReadLockGuard>;
    using UnWLockGuardVec_t = UnLockGuardArray<UnWriteLockGuard>;
    using Mutex_t = std::shared_mutex;

    InnerNode(pgid_t id, u32 maxsize, DB *db, 
            NodeMap<InnerNode> *map, comparator_t cmp): 
        Node(id, maxsize, db), _container(cmp), _map(map){}

    // ==================================================================

    static void containerReset(PageHeader *hdr, InnerContainer &con) {
        con.reset(&hdr->bytes, &hdr->size, (char *)(hdr + 1));
    }

    std::tuple<PageHeader *, PagePtr> 
    handlePage() {
        PageHeader *hdr = nullptr;
        PagePtr pg = _db->getPageCache()->get(_id);
        if(!pg) {
            pg = _db->getPageCache()->alloc(_id, _db->getPageSize());
            //_db->getPageCache()->put(pg);
            hdr = (PageHeader *)pg->read(_db->getFileManager());
            containerReset(hdr, _container);
        }else {
            hdr = (PageHeader *)pg->data();
            if(_container.raw()) {
                containerReset(hdr, _container);
            }
        }
        return std::make_tuple(hdr, pg);
    }

    PageHeader *handleOverFlow(PagePtr pg, u32 extbytes) {
        if(pg->overFlow(extbytes)) {
            pg->extend(_db->getPageAllocator(), extbytes);
            containerReset((PageHeader*)pg->data(), _container);
        }
        return (PageHeader *)pg->data();
    }

    void split(PageHeader *hdr, PutEntry &entry) {

        u32 len = byte2page(hdr->bytes);
        auto next_pg = _db->getPageCache()->alloc(
            _db->getPageAllocator()->allocPage(len),
            _db->getPageSize(), len);
        auto next_hdr = (PageHeader *)next_pg->data();

        PageHeader::init(next_hdr, len, hdr->next); 
        hdr->next = next_pg->getId();

        InnerContainer next_container(_container);
        containerReset(next_hdr, next_container);

        entry.val = next_pg->getId();
        entry.key = _container.splitTo(next_container);
        entry.update = true;
        
        _db->getPageWriter()->write(next_pg);
    }

    bool borrow(DelEntry &entry) {

        auto [hdr, pg] = handlePage();
        auto next = _map->get(hdr->next);
        auto [next_hdr, next_pg] = next->handlePage();
        auto &next_container = next->_container;

        if(!hasmore(next_hdr)) {
            return false;
        }

        hdr = handleOverFlow(pg, _container.elemSize(
                next_container.key(0), next_container.val(0)));

        entry.key = _container.borrowFrom(next_container, entry.delim);
        entry.update = true;
        _db->getPageWriter()->write(next_pg);
        return true;
    }

    void merge(DelEntry &entry) {

        auto [hdr, pg] = handlePage();
        auto next = _map->get(hdr->next);
        auto [next_hdr, next_pg] = next->handlePage();
        auto &next_container = next->_container;

        hdr = handleOverFlow(pg, next_hdr->bytes);

        _container.mergeFrom(next_container, entry.delim);
        entry.del = true;

        auto ret = hdr->next;
        // set next
        hdr->next = next_hdr->next;
        // 1. free page buffer on memory and page id on disk
        next_pg->free(_db->getPageAllocator());
        // 2. del page at cache
        _db->getPageCache()->del(ret);
        // 3. del node on map
        _map->del(ret);
    }

    // ==================================================================

    // the mutex must locked by func get() at here.
    Status put(u32 pos, std::string &key, pgid_t &val, 
            PutEntry &entry, UnWLockGuardVec_t &lg_tlb) {

        auto [hdr, pg] = handlePage();
        hdr = handleOverFlow(pg, _container.elemSize(key, val));
        _container.putat(pos, key, val);

        if(ifsplit(hdr)) {
            DEBUGOUT("===> innernode split");
            split(hdr, entry);
        }
        _db->getPageWriter()->write(pg);
        // unlock self.
        lg_tlb.pop_back();
        return Status();
    }

    // the mutex must locked by func get() at here.
    void del(u32 pos, DelEntry &entry,
            UnWLockGuardVec_t &lg_tlb) {

        auto [hdr, pg] = handlePage();
        _container.delat(pos);

        // if legal or we are the last child of parent
        // return once.
        if(!ifmerge(hdr) || entry.last || !hdr->next) {
            goto done;
        }

        DEBUGOUT("===> innernode borrow");
        if(borrow(entry)) {
            goto done;
        }

        DEBUGOUT("===> innernode merge");
        merge(entry);
done:
        _db->getPageWriter()->write(pg);
        lg_tlb.pop_back();
    }

    // the mutex must locked by func get() at here.
    void update(u32 pos, std::string &newkey, 
            UnWLockGuardVec_t &lg_tlb) {

        auto [hdr, pg] = handlePage();
        hdr = handleOverFlow(pg, _container.elemSize(newkey, 0));
        _container.updateKeyat(pos, newkey);

        _db->getPageWriter()->write(pg);
        lg_tlb.pop_back();
    }

    // for put
    std::tuple<pgid_t, u32> 
    get(std::string &key, UnWLockGuardVec_t &lg_tlb) {

        _shmtx.lock();
        auto [hdr, pg] = handlePage();
        if(safetoput(hdr)) {
            lg_tlb.clear();
        }
        lg_tlb.emplace_back(_shmtx);

        handlePage();
        return _container.get(key);
    }

    // for del
    std::tuple<pgid_t, u32> 
    get(std::string &key, DelEntry &entry, 
        UnWLockGuardVec_t &lg_tlb) {

        _shmtx.lock();
        auto [hdr, pg] = handlePage();
        if(safetodel(hdr)) {
            lg_tlb.clear();
        }
        lg_tlb.emplace_back(_shmtx);

        handlePage();
        return _container.get(key, entry);
    }

    // for search
    std::tuple<pgid_t, u32> 
    get(std::string &key, Mutex_t &par_mtx) {

        //lock self and release parent.
        _shmtx.lock_shared();
        par_mtx.unlock_shared();

        handlePage();
        return _container.get(key);
    }

    // without lock
    std::tuple<pgid_t, u32> 
    get(std::string &key) {

        handlePage();
        return _container.get(key);
    }

    //==================================================

    static void newOnDisk(
        pgid_t id, FileManager *fm, u32 page_size, 
        std::string &key, pgid_t child1, pgid_t child2) {

        Page pg(id, page_size, 1);
        auto hdr = (PageHeader *)pg.data();
        PageHeader::init(hdr, 1, 0);

        // 初始化容器
        InnerContainer con;
        containerReset(hdr, con);
        con.init(key, child1, child2);

        pg.write(fm);
    }

    bool empty() {
        auto [hdr, pg] = handlePage();
        return hdr->size == 0;
    }

    // return the only child in node
    pgid_t tochild() {
        //std::cout << "tochild\n";
        auto [hdr, pg] = handlePage();
        assert(hdr->next == 0);
        auto ret =  _container.head();
        // free self page
        pg->free(_db->getPageAllocator());
        _db->getPageCache()->del(ret);
        return ret;
    }
private:
    InnerContainer _container;
    NodeMap<InnerNode> *_map;
};

}// namespace bptdb

#endif
