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
    using IterPtr_t = std::shared_ptr<Iter_t>;
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
        _iters.clear();

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

        _iters.clear();

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
        _iters.clear();
        _container.del(key);
        pg->write(_db->getFileManager());
        return std::make_tuple(true, Status());
    }


    Status del(KType &key, DelEntry<KType> &entry) {

        std::lock_guard lg(_shmtx);
        auto [hdr, pg] = handlePage(_container);
        // the key is not exist
        assert(_container.find(key));

        _iters.clear();
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

        // if we should update iter here ???

        auto extbytes = _container.elemSize(key, val);

        if(pg->overFlow(extbytes)) {
            hdr = (PageHeader *)pg->extend(_db->getPageAllocator(), extbytes);
            containerReset(hdr, _container);
        }
        _container.update(key, val);
        pg->write(_db->getFileManager());

        return Status();
    }
    // =====================================================

    IterPtr_t begin() {
        // write lock here because we change the _iters.
        //std::lock_guard lg(_shmtx);

        handlePage(_container);
        auto it =  std::make_shared<Iter_t>(_container.begin());
        _iters.push_back(it);
        return it;
    }
    //// XXX
    //IterPtr_t upper(KType &key, Mutex_t &par_mtx) {
    //    handlePage(_container);
    //    auto it =  std::make_shared<Iter_t>(_container.upper());
    //    _iters.push_back(it);
    //    return it;
    //}
    //// XXX
    //IterPtr_t lower(KType &key, Mutex_t &par_mtx) {
    //    handlePage(_container);
    //    auto it =  std::make_shared<Iter_t>(_container.lower());
    //    _iters.push_back(it);
    //    return it;
    //}
    IterPtr_t at(KType &key, Mutex_t &par_mtx) {
        // write lock here because we change the _iters.
        //std::lock_guard lg(_shmtx);
        //par_mtx.unlock_shared();

        handlePage(_container);
        auto it =  std::make_shared<Iter_t>(_container.at(key));
        _iters.push_back(it);
        return it;
    }
    bool updateIter(IterPtr_t iter, IterMoveType type) {
        //std::shared_lock lg(_shmtx);
        handlePage(_container);
        if(iter.use_count() == 1) {
            return false;
        }
        _container.updateIter(*iter, type);    
        return true;
    }
    // ================================================= 
    static void newOnDisk(pgid_t id, FileManager *fm, 
                                   u32 page_size) {
        Page pg(id, page_size, 1);
        auto hdr = (PageHeader *)pg.data();
        PageHeader::init(hdr, 1, 0);
        pg.write(fm);
    }
    //XXX
    LeafNode *next() {
        auto [hdr, pg] = handlePage(_container);
        if(hdr->next == 0) {
            return nullptr;
        }
        return _map->get(hdr->next);
    }

private:
    ContainerType        _container;
    //TODO change to thread safe
    std::list<IterPtr_t> _iters;
    NodeMap<LeafNode>    *_map;
};

}// namespace bptdb

#endif
