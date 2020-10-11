#ifndef __LEAF_NODE_H
#define __LEAF_NODE_H

#include <memory>
#include "Node.h"
#include "LeafContainer.h"
#include "PageHelper.h"

// maintain next_container._container

namespace bptdb {

class LeafNode: public Node {
public:
    using Iter_t = LeafContainer::Iterator;
    //using IterPtr_t = std::shared_ptr<Iter_t>;
    using Mutex_t = std::shared_mutex;

    LeafNode(pgid_t id, u32 maxsize, 
             NodeMap<LeafNode> *map, comparator_t cmp): 
        Node(id, maxsize), _container(cmp), _map(map) {}

    // ==================================================================

    void containerReset(PageHeader *hdr, LeafContainer &con) {
        con.reset(&hdr->bytes, &hdr->size, (char *)(hdr + 1));
    }

    std::tuple<PageHeader *, PageHelperPtr> 
    handlePage() {
        auto pg = std::make_shared<PageHelper>(_id);
        pg->read();
        PageHeader *hdr = (PageHeader *)pg->data();
        containerReset(hdr, _container);
        return std::make_tuple(hdr, pg);
    }
    PageHeader *handleOverFlow(PageHelperPtr pg, u32 extbytes) {
        if(pg->overFlow(extbytes)) {
            pg->extend(extbytes);
            containerReset((PageHeader *)pg->data(), _container);
        }
        return (PageHeader *)pg->data();
    }

    void split(PageHeader *hdr, PutEntry &entry) {

        u32 len = byte2page(hdr->bytes);
        auto next_pg = std::make_shared<PageHelper>(
                g_pa->allocPage(len), len);
        auto next_hdr = (PageHeader *)next_pg->data();

        PageHeader::init(next_hdr, len, hdr->next); 
        hdr->next = next_pg->getId();

        LeafContainer next_container(_container);
        containerReset(next_hdr, next_container);

        entry.val = next_pg->getId();
        entry.key = _container.splitTo(next_container);
        entry.update = true;
        
        next_pg->write();
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
        next_pg->write();
        
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
        
        assert(hdr->next);
        auto ret = hdr->next;
        // set next
        hdr->next = next_hdr->next;
        // 1. free page buffer on memory and page id on disk
        next_pg->free();
        // 2. del page at cache
        // _db->getPageCache()->del(ret);
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
            // write here, because the page maybe over flow and change.
            pg->write();
            return std::make_tuple(true, Status(error::keyRepeat));
        }

        pg->write();
        return std::make_tuple(true, Status());
    }

    Status put(std::string &key, std::string &val, PutEntry &entry) {

        std::lock_guard lg(_shmtx);

        auto [hdr, pg] = handlePage();
        hdr = handleOverFlow(pg, _container.elemSize(key, val));

        if(!_container.put(key, val)) {
            // same as tryPut
            pg->write();
            return Status(error::keyRepeat);
        }
        // we need to judge again, because other thread may do this.
        if(ifsplit(hdr)) {
            DEBUGOUT("===> leafnode split");
            // do split
            split(hdr, entry);
        }
        pg->write();
        return Status();
    }

    std::tuple<bool, Status> 
    tryDel(std::string &key, Mutex_t &par_mtx) {
        std::lock_guard lg(_shmtx);
        par_mtx.unlock_shared();

        auto [hdr, pg] = handlePage();
        if(!safetodel(hdr)) {
            return std::make_tuple(false, Status());
        }
        if(!_container.del(key)) {
            pg->write();
            return std::make_tuple(true, Status(error::keyNotFind));
        }
        pg->write();
        return std::make_tuple(true, Status());
    }

    Status del(std::string &key, DelEntry &entry) {

        std::lock_guard lg(_shmtx);
        auto [hdr, pg] = handlePage();

        if(!_container.del(key)) {
            return Status(error::keyNotFind);
        }
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
        pg->write();
            
        return Status(); 
    }

    Status get(std::string &key, std::string &val, 
            Mutex_t &par_mtx) {

        // shared lock guard for self and unlock parent.
        std::shared_lock lg(_shmtx);
        par_mtx.unlock_shared();

        // keep page alive.
        auto [hdr, pg] = handlePage();
        (void)hdr;
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
        hdr = handleOverFlow(pg, _container.elemSize(key, val));

        if(!_container.update(key, val)) {
            pg->write();
            return Status(error::keyNotFind);
        }
        pg->write();
        return Status();
    }

    // !!!iteration without lock
    // =====================================================

    std::tuple<Iter_t, PageHelperPtr> begin() {
        // keep page alive.
        auto [hdr, pg] = handlePage();
        (void)hdr;
        return std::make_tuple(_container.begin(), pg);
    }
    std::tuple<Iter_t, PageHelperPtr> at(std::string &key) {
        // keep page alive.
        auto [hdr, pg] = handlePage();
        (void)hdr;
        return std::make_tuple(_container.at(key), pg);
    }

    // ================================================= 
    static void newOnDisk(pgid_t id) {
        PageHelper pg(id, 1);
        auto hdr = (PageHeader *)pg.data();
        PageHeader::init(hdr, 1, 0);
        pg.write();
    }
    // !!!without lock, only used by iterator.
    LeafNode *next() {
        // keep page alive.
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
