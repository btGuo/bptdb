#ifndef __LEAF_NODE_H
#define __LEAF_NODE_H

#include <memory>
#include <list>
#include "__Node.h"
#include "LeafContainer.h"

// maintain other._container

namespace bptdb {

enum class IterMoveType;

template <typename KType, typename VType, typename Comp>
class LeafNode: public Node {
public:
    using ContainerType = LeafContainer<KType, VType, Comp>;
    using Iter_t = typename ContainerType::Iterator;
    //using IterPtr_t = std::shared_ptr<Iter_t>;
    using Mutex_t = std::shared_mutex;

    LeafNode(pgid_t id, u32 maxsize, DB *db, 
             NodeMap<LeafNode> *map): Node(id, maxsize, db) {
        _map = map;
    }

    std::tuple<bool, Status> 
    tryPut(KType &key, VType &val, Mutex_t &par_mtx) {
        std::lock_guard lg(_shmtx);
        par_mtx.unlock_shared();
        
        auto [hdr, pg] = handlePage(_container);
        // only leafcontainer have to find
        if(_container.find(key)) {
            return std::make_tuple(true, Status(error::keyRepeat));
        }

        if(!safetoput(hdr)) {
            return std::make_tuple(false, Status());
        }

        auto extbytes = _container.elemSize(key, val);
        // there enought space
        if(pg->overFlow(extbytes)) {
            hdr = (PageHeader *)pg->extend(_db->getPageAllocator(), extbytes);
            containerReset(hdr, _container);
        }
        _container.put(key, val);
        pg->write(_db->getFileManager());
        return std::make_tuple(true, Status());
    }

    Status put(KType &key, VType &val, PutEntry<KType> &entry) {

        std::lock_guard lg(_shmtx);
        auto [hdr, pg] = handlePage(_container);
        // func tryPut() have do this.
        assert(!_container.find(key));

        auto extbytes = _container.elemSize(key, val);
        // there enought space
        if(pg->overFlow(extbytes)) {
            hdr = (PageHeader *)pg->extend(_db->getPageAllocator(), extbytes);
            containerReset(hdr, _container);
        }
        _container.put(key, val);

        // must split here.
        assert(ifsplit(hdr));
        DEBUGOUT("===> leafnode split");

        split(hdr, entry, _container);
        entry.update = true;
        pg->write(_db->getFileManager());
        return Status();
    }

    std::tuple<bool, Status> 
    tryDel(KType &key, Mutex_t &par_mtx) {
        std::lock_guard lg(_shmtx);
        par_mtx.unlock_shared();

        auto [hdr, pg] = handlePage(_container);
        // the key is not exist
        if(!_container.find(key)) {
            return std::make_tuple(true, Status(error::keyNotFind));
        }

        if(!safetodel(hdr)) {
            return std::make_tuple(false, Status());
        }

        _container.del(key);
        pg->write(_db->getFileManager());
        return std::make_tuple(true, Status());
    }


    Status del(KType &key, DelEntry<KType> &entry) {

        std::lock_guard lg(_shmtx);
        auto [hdr, pg] = handlePage(_container);
        // the key is not exist
        assert(_container.find(key));

        _container.del(key);

        assert(ifmerge(hdr));
        auto &next_container = _map->get(hdr->next)->_container;
        // special 
        if(entry.last || !hdr->next) {
            goto done;
        }

        DEBUGOUT("===> leafnode borrow");
        if(borrow(hdr, pg, entry, _container, next_container)) {
            goto done;
        }

        DEBUGOUT("===> leafnode merge");
        // 3. del node on map
        _map->del(merge(hdr, pg, entry, _container, next_container));
done:
        pg->write(_db->getFileManager());
        return Status(); 
    }

    Status get(KType &key, VType &val, 
            Mutex_t &par_mtx) {

        // shared lock guard for self and unlock parent.
        std::shared_lock lg(_shmtx);
        par_mtx.unlock_shared();

        handlePage(_container);
        if(!_container.find(key)) {
            return Status(error::keyNotFind);
        }
        val = _container.get(key);
        return Status();
    }

    Status update(KType &key, VType &val, 
            Mutex_t &par_mtx) {

        // lock guard for self and unlock parent.
        std::lock_guard lg(_shmtx);
        par_mtx.unlock_shared();

        auto [hdr, pg] = handlePage(_container);
        if(!_container.find(key)) {
            return Status(error::keyNotFind);
        }

        auto extbytes = _container.elemSize(key, val);

        if(pg->overFlow(extbytes)) {
            hdr = (PageHeader *)pg->extend(_db->getPageAllocator(), extbytes);
            containerReset(hdr, _container);
        }
        _container.update(key, val);
        pg->write(_db->getFileManager());

        return Status();
    }

    // !!!iteration without lock
    // =====================================================

    std::tuple<Iter_t, PagePtr> begin() {
        auto [hdr, pg] = handlePage(_container);
        return std::make_tuple(_container.begin(), pg);
    }
    std::tuple<Iter_t, PagePtr> at(KType &key) {
        auto [hdr, pg] = handlePage(_container);
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
        auto [hdr, pg] = handlePage(_container);
        if(hdr->next == 0) {
            return nullptr;
        }
        return _map->get(hdr->next);
    }

private:
    ContainerType        _container;
    NodeMap<LeafNode>    *_map;
};

}// namespace bptdb

#endif
