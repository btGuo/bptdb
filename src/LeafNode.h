#ifndef __LEAF_NODE_H
#define __LEAF_NODE_H

#include <memory>
#include "Node.h"
#include "PageWriter.h"
#include "LeafContainer.h"

// maintain next_container._container

namespace bptdb {

class LeafNode: public Node {
public:
    using Iter_t = LeafContainer::Iterator;
    //using IterPtr_t = std::shared_ptr<Iter_t>;
    using Mutex_t = std::shared_mutex;

    LeafNode(pgid_t id, u32 maxsize, DB *db, 
             NodeMap<LeafNode> *map, comparator_t cmp): 
        Node(id, maxsize, db), _container(cmp), _map(map) {}

    // ==================================================================

    void containerReset(PageHeader *hdr, LeafContainer &con) {
        con.reset(&hdr->bytes, &hdr->size, (char *)(hdr + 1));
    }

    std::tuple<PageHeader *, PagePtr> 
    handlePage() {
        PageHeader *hdr = nullptr;
        PagePtr pg = _db->getPageCache()->get(_id);
        if(!pg) {
            pg = _db->getPageCache()->alloc(_id, _db->getPageSize());
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
            containerReset((PageHeader *)pg->data(), _container);
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

        LeafContainer next_container(_container);
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

    std::tuple<bool, Status> 
    tryPut(std::string &key, std::string &val, Mutex_t &par_mtx) {
        std::lock_guard lg(_shmtx);
        par_mtx.unlock_shared();
        
        auto [hdr, pg] = handlePage();

        if(!safetoput(hdr)) {
            return std::make_tuple(false, Status());
        }

        hdr = handleOverFlow(pg, _container.elemSize(key, val));
        if(!_container.put(key, val)) {
            return std::make_tuple(true, Status(error::keyRepeat));
        }

        _db->getPageWriter()->write(pg);
        return std::make_tuple(true, Status());
    }

    Status put(std::string &key, std::string &val, PutEntry &entry) {

        std::lock_guard lg(_shmtx);

        auto [hdr, pg] = handlePage();
        //assert(!_container.find(key));
        hdr = handleOverFlow(pg, _container.elemSize(key, val));

        _container.put(key, val);
        // we need to judge again, because other thread may do this.
        if(ifsplit(hdr)) {
            DEBUGOUT("===> leafnode split");
            // do split
            split(hdr, entry);
        }
        _db->getPageWriter()->write(pg);
        return Status();
    }

    std::tuple<bool, Status> 
    tryDel(std::string &key, Mutex_t &par_mtx) {
        std::lock_guard lg(_shmtx);
        par_mtx.unlock_shared();

        auto [hdr, pg] = handlePage();
        // the key is not exist
        if(!_container.find(key)) {
            return std::make_tuple(true, Status(error::keyNotFind));
        }

        if(!safetodel(hdr)) {
            return std::make_tuple(false, Status());
        }

        _container.del(key);
        _db->getPageWriter()->write(pg);
        return std::make_tuple(true, Status());
    }

    Status del(std::string &key, DelEntry &entry) {

        std::lock_guard lg(_shmtx);
        auto [hdr, pg] = handlePage();
        // the key is not exist
        assert(_container.find(key));

        _container.del(key);

        //assert(ifmerge(hdr));
        // judge again 
        if(!ifmerge(hdr) || entry.last || !hdr->next) {
            goto done;
        }

        DEBUGOUT("===> leafnode borrow");
        if(borrow(entry)) {
            goto done;
        }
        DEBUGOUT("===> leafnode merge");
        merge(entry);
done:
        _db->getPageWriter()->write(pg);
        return Status(); 
    }

    Status get(std::string &key, std::string &val, 
            Mutex_t &par_mtx) {

        // shared lock guard for self and unlock parent.
        std::shared_lock lg(_shmtx);
        par_mtx.unlock_shared();

        handlePage();
        if(!_container.get(key, val)) {
            return Status(error::keyNotFind);
        }
        return Status();
    }

    Status update(std::string &key, std::string &val, 
            Mutex_t &par_mtx) {

        // lock guard for self and unlock parent.
        std::lock_guard lg(_shmtx);
        par_mtx.unlock_shared();

        auto [hdr, pg] = handlePage();
        if(!_container.find(key)) {
            return Status(error::keyNotFind);
        }

        auto extbytes = _container.elemSize(key, val);

        if(pg->overFlow(extbytes)) {
            hdr = (PageHeader *)pg->extend(_db->getPageAllocator(), extbytes);
            containerReset(hdr, _container);
        }
        _container.update(key, val);
        _db->getPageWriter()->write(pg);

        return Status();
    }

    // !!!iteration without lock
    // =====================================================

    std::tuple<Iter_t, PagePtr> begin() {
        auto [hdr, pg] = handlePage();
        return std::make_tuple(_container.begin(), pg);
    }
    std::tuple<Iter_t, PagePtr> at(std::string &key) {
        auto [hdr, pg] = handlePage();
        return std::make_tuple(_container.at(key), pg);
    }

    // ================================================= 
    static void newOnDisk(pgid_t id, FileManager *fm, 
                                   u32 page_size) {
        Page pg(id, page_size, 1);
        auto hdr = (PageHeader *)pg.data();
        PageHeader::init(hdr, 1, 0);
        pg.write(fm);
    }
    // !!!without lock
    LeafNode *next() {
        auto [hdr, pg] = handlePage();
        if(hdr->next == 0) {
            return nullptr;
        }
        return _map->get(hdr->next);
    }

private:
    LeafContainer         _container;
    NodeMap<LeafNode>    *_map;
};

}// namespace bptdb

#endif
