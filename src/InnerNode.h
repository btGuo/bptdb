#ifndef __INNER_NODE_H
#define __INNER_NODE_H

#include "Node.h"
#include "InnerNodeImpl.h"
#include "LockHelper.h"
#include "PageHelper.h"

namespace bptdb {

class InnerNode: public Node {
public:
    //using UnRLockGuardVec_t = std::vector<UnReadLockGuard>;
    using UnWLockGuardVec_t = UnLockGuardArray<UnWriteLockGuard>;
    using Mutex_t = std::shared_mutex;

    InnerNode(pgid_t id, u32 maxsize, 
            NodeMap<InnerNode> *map, comparator_t cmp): 
        Node(id, maxsize), _cmp(cmp), _map(map){}

    // ==================================================================

    void split(PutEntry &entry, InnerNodeImpl &impl) {

        auto new_id = g_pa->allocPage(1);
        PageHeader::newOnDisk(new_id, 1, impl.next());
        impl.setNext(new_id);

        auto next_node = InnerNodeImpl(new_id, _cmp);
        entry.val = new_id;
        entry.key = impl.splitTo(next_node);
        entry.update = true;
        
        next_node.write();
    }

    bool borrow(DelEntry &entry, InnerNodeImpl &impl) {

        auto next_node = InnerNodeImpl(impl.next(), _cmp);
        if(!hasmore(next_node.size())) {
            return false;
        }

        // diff with leafnode. here we use entry.delim.

        entry.key = impl.borrowFrom(next_node, entry.delim);
        entry.update = true;
        next_node.write();
        return true;
    }

    void merge(DelEntry &entry, InnerNodeImpl &impl) {

        auto next_node = InnerNodeImpl(impl.next(), _cmp);

        // diff with leafnode. here we add entry.delim.

        impl.mergeFrom(next_node, entry.delim);
        entry.del = true;

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

    // the mutex must locked by func get() at here.
    Status put(u32 pos, std::string &key, pgid_t &val, 
            PutEntry &entry, UnWLockGuardVec_t &lg_tlb) {

        auto impl = InnerNodeImpl(_id, _cmp);
        impl.putat(pos, key, val);

        if(ifsplit(impl.size())) {
            DEBUGOUT("===> innernode split");
            split(entry, impl);
        }
        impl.write();
        // unlock self.
        lg_tlb.pop_back();
        return Status();
    }

    // the mutex must locked by func get() at here.
    void del(u32 pos, DelEntry &entry,
            UnWLockGuardVec_t &lg_tlb) {

        auto impl = InnerNodeImpl(_id, _cmp);
        impl.delat(pos);

        // if legal or we are the last child of parent
        // return once.
        if(!ifmerge(impl.size()) || entry.last || !impl.next()) {
            goto done;
        }

        DEBUGOUT("===> innernode borrow");
        if(borrow(entry, impl)) {
            goto done;
        }

        DEBUGOUT("===> innernode merge");
        merge(entry, impl);
done:
        impl.write();
        lg_tlb.pop_back();
    }

    // the mutex must locked by func get() at here.
    void update(u32 pos, std::string &newkey, 
            UnWLockGuardVec_t &lg_tlb) {

        auto impl = InnerNodeImpl(_id, _cmp);
        impl.updateKeyat(pos, newkey);

        impl.write();
        lg_tlb.pop_back();
    }

    // for put
    std::tuple<pgid_t, u32> 
    get(std::string &key, UnWLockGuardVec_t &lg_tlb) {

        _shmtx.lock();
        // keep page alive.
        auto impl = InnerNodeImpl(_id, _cmp);
        if(safetoput(impl.size())) {
            lg_tlb.clear();
        }
        lg_tlb.emplace_back(_shmtx);
        return impl.get(key);
    }

    // for del
    std::tuple<pgid_t, u32> 
    get(std::string &key, DelEntry &entry, 
        UnWLockGuardVec_t &lg_tlb) {

        _shmtx.lock();
        // keep page alive.
        auto impl = InnerNodeImpl(_id, _cmp);
        if(safetodel(impl.size())) {
            lg_tlb.clear();
        }
        lg_tlb.emplace_back(_shmtx);
        return impl.get(key, entry);
    }

    // for search
    std::tuple<pgid_t, u32> 
    get(std::string &key, Mutex_t &par_mtx) {

        //lock self and release parent.
        _shmtx.lock_shared();
        par_mtx.unlock_shared();

        auto impl = InnerNodeImpl(_id, _cmp);
        // keep page alive.
        return impl.get(key);
    }

    // without lock, only used by iterator.
    std::tuple<pgid_t, u32> 
    get(std::string &key) {
        // keep page alive.
        auto impl = InnerNodeImpl(_id, _cmp);
        return impl.get(key);
    }

    //==================================================

    static void newOnDisk(
        pgid_t id, std::string &key, 
        pgid_t child1, pgid_t child2, comparator_t cmp) {

        PageHeader::newOnDisk(id);

        // 初始化容器
        InnerNodeImpl impl(id, cmp);
        impl.init(key, child1, child2);
        impl.write();
    }

    bool empty() {
        // keep page alive.
        return false;
    }

    // return the only child in node
    pgid_t tochild() {
        //std::cout << "tochild\n";
        auto impl = InnerNodeImpl(_id, _cmp);
        assert(impl.next() == 0);
        auto ret =  impl.head();
        // free self page
        impl.free();
        // _db->getPageCache()->del(ret);
        
        return ret;
    }

    std::string maxkey() {
        auto impl = InnerNodeImpl(_id, _cmp);
        return std::string(impl.maxkey());
    }

    std::string minkey() {
        auto impl = InnerNodeImpl(_id, _cmp);
        return std::string(impl.minkey());
    }

    void show() {
        auto impl = InnerNodeImpl(_id, _cmp);
        std::cout << "size " << impl.size() << "\n";
        for(auto it = impl.begin(); !it.done(); it.next()) {
            std::cout << "key " << std::string(it.key()) << "\n";
        }
    }

    void debug(u32 height) {
        auto impl = InnerNodeImpl(_id, _cmp);
        if(height == 2) {
            return;
        }
        pgid_t prev = impl.head(), cur = 0;
        _map->get(prev)->debug(height - 1);
        for(auto it = impl.begin(); !it.done(); it.next()) {
            cur = it.val();
            _map->get(cur)->debug(height - 1);
            auto keylower = _map->get(prev)->maxkey();
            auto keyupper = _map->get(cur)->minkey();
            if(!(it.key() > keylower &&
                   it.key() <= keyupper)) {
                assert(0);
            }
            prev = cur;
        }
    }
private:
    comparator_t       _cmp;
    NodeMap<InnerNode> *_map;
};

}// namespace bptdb

#endif
