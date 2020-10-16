#ifndef __LEAF_NODE_H
#define __LEAF_NODE_H

#include <memory>
#include "Node.h"
#include "Option.h"
#include "LeafNodeImpl.h"
#include "PageHelper.h"

// maintain next_impl._impl

namespace bptdb {

class LeafNode: public Node {
public:
    using Iter_t = LeafNodeImpl::Iterator;
    //using IterPtr_t = std::shared_ptr<Iter_t>;
    using Mutex_t = std::shared_mutex;

    LeafNode(pgid_t id, u32 maxsize, 
             NodeMap<LeafNode> *map, comparator_t cmp): 
        Node(id, maxsize), _cmp(cmp), _map(map) {}

    // ==================================================================

    void split(PutEntry &entry, LeafNodeImpl &impl) {

        auto new_id = g_pa->allocPage(1);
        PageHeader::newOnDisk(new_id, 1, impl.next());
        impl.setNext(new_id);

        auto next_node = LeafNodeImpl(new_id, _cmp);

        entry.val = new_id;
        entry.key = impl.splitTo(next_node);
        entry.update = true;

        next_node.write();
    }

    bool borrow(DelEntry &entry, LeafNodeImpl &impl) {

        auto next_node = LeafNodeImpl(impl.next(), _cmp);

        if(!hasmore(next_node.size())) {
            return false;
        }

        entry.key = impl.borrowFrom(next_node);
        entry.update = true;
        next_node.write();
        
        return true;
    }

    void merge(DelEntry &entry, LeafNodeImpl &impl) {

        auto next_node = LeafNodeImpl(impl.next(), _cmp);
        impl.mergeFrom(next_node);
        entry.del = true;
        
        assert(impl.next());
        auto ret = impl.next();
        // set next
        impl.setNext(next_node.next());
        // 1. free page buffer on memory and page id on disk
        next_node.free();
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
        
        auto impl = LeafNodeImpl(_id, _cmp);
        if (!safetoput(impl.size())) {
            return std::make_tuple(false, Status());
        }
        if(!impl.put(key, val)) {
            // write here, because the page maybe over flow and change.
            impl.write();
            return std::make_tuple(true, Status(error::keyRepeat));
        }
        impl.write();
        return std::make_tuple(true, Status());
    }

    Status put(std::string &key, std::string &val, PutEntry &entry) {

        std::lock_guard lg(_shmtx);

        auto impl = LeafNodeImpl(_id, _cmp);
        if(!impl.put(key, val)) {
            // same as tryPut
            impl.write();
            return Status(error::keyRepeat);
        }
        // we need to judge again, because other thread may do this.
        if(ifsplit(impl.size())) {
            DEBUGOUT("===> leafnode split");
            // do split
            split(entry, impl);
        }
        impl.write();
        return Status();
    }

    std::tuple<bool, Status> 
    tryDel(std::string &key, Mutex_t &par_mtx) {
        std::lock_guard lg(_shmtx);
        par_mtx.unlock_shared();

        auto impl = LeafNodeImpl(_id, _cmp);
        if(safetodel(impl.size())) {
            return std::make_tuple(false, Status());
        }
        if(!impl.del(key)) {
            impl.write();
            return std::make_tuple(true, Status(error::keyNotFind));
        }
        impl.write();
        return std::make_tuple(true, Status());
    }

    Status del(std::string &key, DelEntry &entry) {

        std::lock_guard lg(_shmtx);
        auto impl = LeafNodeImpl(_id, _cmp);

        if(!impl.del(key)) {
            return Status(error::keyNotFind);
        }
        // judge again 
        if(!ifmerge(impl.size()) || entry.last || !impl.next()) {
            goto done;
        }
        DEBUGOUT("===> leafnode borrow");
        if(borrow(entry, impl)) {
            goto done;
        }
        DEBUGOUT("===> leafnode merge");
        merge(entry, impl);
done:
        impl.write();
            
        return Status(); 
    }

    Status get(std::string &key, std::string &val, 
            Mutex_t &par_mtx) {

        // shared lock guard for self and unlock parent.
        std::shared_lock lg(_shmtx);
        par_mtx.unlock_shared();

        auto impl = LeafNodeImpl(_id, _cmp);
        // keep page alive.
        if(!impl.get(key, val)) {
            return Status(error::keyNotFind);
        }
        return Status();
    }

    Status update(std::string &key, std::string &val, 
            Mutex_t &par_mtx) {

        // lock guard for self and unlock parent.
        std::lock_guard lg(_shmtx);
        par_mtx.unlock_shared();

        auto impl = LeafNodeImpl(_id, _cmp);

        if(!impl.update(key, val)) {
            impl.write();
            return Status(error::keyNotFind);
        }
        impl.write();
        return Status();
    }

    static void newOnDisk(pgid_t id) {
        PageHeader::newOnDisk(id);
    }

    // !!!iteration without lock
    // =====================================================

    std::tuple<Iter_t, LeafNodeImplPtr> begin() {
        // keep page alive.
        auto impl = std::make_shared<LeafNodeImpl>(_id, _cmp);
        return std::make_tuple(impl->begin(), impl);
    }
    std::tuple<Iter_t, LeafNodeImplPtr> at(std::string &key) {
        // keep page alive.
        auto impl = std::make_shared<LeafNodeImpl>(_id, _cmp);
        return std::make_tuple(impl->at(key), impl);
    }

    // !!!without lock, only used by iterator.
    LeafNode *next() {
        // keep page alive.
        auto impl = LeafNodeImpl(_id, _cmp);
        if(impl.next() == 0) {
            return nullptr;
        }
        return _map->get(impl.next());
    }

private:
    comparator_t         _cmp;
    NodeMap<LeafNode>    *_map;
};

}// namespace bptdb

#endif
