#ifndef __BPTREE_H
#define __BPTREE_H

#include <mutex>
#include <iostream>
#include <vector>
#include <type_traits>
#include <unordered_map>
#include "Status.h"
#include "LeafNode.h"
#include "InnerNode.h"
#include "LockHelper.h"
#include "DBImpl.h"
#include "IteratorBase.h"

namespace bptdb {

class Bptree {
public:
    using Iter_t      = LeafNode::Iter_t;
    using UnWLockGuardVec_t = UnLockGuardArray<UnWriteLockGuard>;

    // ====================================================

    class Iterator: public IteratorBase {
        friend class Bptree;
    public:    
        Iterator() = default;
        std::string_view key()    { return it.key(); }
        std::string_view val()    { return it.val(); }
        bool  done() { return _done; }
        void next() {
            if(_done) {
                throw "out of range";
            }
            it.next();
            // if we reach the last elem
            if(it.done()) {
                if(!(node = node->next())) {
                    _done = true;
                    return;
                }
                std::tie(it, pg) = node->begin();
            }
        }
    private:
        bool _done{false};
        LeafNode *node{nullptr};
        Iter_t it;
        PagePtr pg;
    };

    // ====================================================

    std::shared_ptr<IteratorBase> begin() {
        auto node = _leaf_map.get(_first);
        auto it = std::make_shared<Iterator>();
        it->node = node;
        std::tie(it->it, it->pg) = node->begin();
        return it;
    }

    std::shared_ptr<IteratorBase> at(std::string &key) {
        auto nodeid = down(_height, _root, key);
        auto node = _leaf_map.get(nodeid);
        auto it = std::make_shared<Iterator>();
        it->node = node;
        std::tie(it->it, it->pg) = node->at(key);
        return it;
    }
    // ====================================================

    Bptree(std::string name, BptreeMeta meta, DBImpl *db, comparator_t cmp):
    _leaf_map(meta.order, db, cmp), _inner_map(meta.order, db, cmp){
        _name   = name;
        _order  = meta.order;
        _height = meta.height;
        _root   = meta.root;
        _first  = meta.first;
        _db     = db;
        _cmp    = cmp;
    }

    static void newOnDisk(pgid_t id, FileManager *fm, u32 page_size) {
        LeafNode::newOnDisk(id, fm, page_size);
    }

    //====================================================================

    std::tuple<Status, std::string> get(std::string &key) {
        _root_mtx.lock_shared();
        auto [nodeid, mutex] = down(_height, _root, key, _root_mtx);
        std::string val;
        auto stat = _leaf_map.get(nodeid)->get(key, val, mutex);
        return std::make_tuple(stat, val);
    }

    Status update(std::string &key, std::string &val) {
        _root_mtx.lock_shared();
        auto [nodeid, mutex] = down(_height, _root, key, _root_mtx);
        return  _leaf_map.get(nodeid)->update(key, val, mutex);
    }

    Status put(std::string &key, std::string &val) {
        {
            //try put at first.
            _root_mtx.lock_shared();
            auto [nodeid, mutex] = down(_height, _root, key, _root_mtx);
            auto [success, stat] = _leaf_map.get(nodeid)->tryPut(key, val, mutex);
            // success! only change the leafnode.
            if(success) {
                return stat;
            }
        }
        // leafnode split.

        PutEntry entry;
        UnWLockGuardVec_t lg_tlb(_height + 1);
        lg_tlb.emplace_back(_root_mtx); 

        _root_mtx.lock();

        auto stat = _put(_height, _root, key, val, entry, lg_tlb);
        if(!stat.ok()) {
            return stat;
        }

        if(!entry.update) {
            return stat;
        }

        // must be locked here.
        auto prev = _root;
        _root = _db->getPageAllocator()->allocPage(1);
        //std::cout << "root " << prev << " change to " << _root << "\n";
        InnerNode::newOnDisk(_root,
                _db->getFileManager(), _db->getPageSize(),
                entry.key, prev, entry.val);

        _height++;
        _db->updateRoot(_name, _root, _height);
        return stat;
    }

    Status del(std::string &key) {
        {
            //try put at first.
            _root_mtx.lock_shared();
            auto [nodeid, mutex] = down(_height, _root, key, _root_mtx);
            auto [success, stat] = _leaf_map.get(nodeid)->tryDel(key, mutex);
            // success! only change the leafnode.
            if(success) {
                return stat;
            }
        }

        DelEntry entry;
        UnWLockGuardVec_t lg_tlb(_height + 1);
        lg_tlb.emplace_back(_root_mtx); 

        _root_mtx.lock();

        auto stat = _del(_height, _root, key, entry, lg_tlb);
        if(!stat.ok()) {
            lg_tlb.clear();
            return stat;
        }

        // must be locked here.
        // maybe we need to change the root
        if(_height > 1) {
            auto root = _inner_map.get(_root);
            // only one child in node
            if(root->empty()) {
                DEBUGOUT("=====> root change");
                auto old = _root;
                _root = root->tochild();
                _height--;
                _db->updateRoot(_name, _root, _height);
                // delete the prev root
                _inner_map.del(old);
            }
        }
        lg_tlb.clear();
        return stat;
    }

private:

    Status _del(u32 height, pgid_t nodeid, std::string &key,
                DelEntry &entry, 
                UnWLockGuardVec_t &lg_tlb) {

        if(height == 1) {
            auto node = _leaf_map.get(nodeid);
            return node->del(key, entry);
        }
        DelEntry selfentry;
        auto node = _inner_map.get(nodeid);
        auto [id, pos] = node->get(key, selfentry, lg_tlb);

        auto stat = _del(height - 1, id, key, selfentry, lg_tlb);
        if(!stat.ok()) {
            return stat;
        }

        if(selfentry.update) {
            node->update(pos, selfentry.key, lg_tlb);
        }else if(selfentry.del) {
            node->del(pos, entry, lg_tlb);
        }else {
            DEBUGOUT("special time");
            //lg_tlb.pop_back();
        }
        
        return stat;
    }

    Status _put(u32 height, pgid_t nodeid, std::string &key, std::string &val, 
                PutEntry &entry, UnWLockGuardVec_t &lg_tlb) {

        if(height == 1) {
            auto node = _leaf_map.get(nodeid);
            return node->put(key, val, entry);
        }
        auto node = _inner_map.get(nodeid);
        auto [id, pos] = node->get(key, lg_tlb);

        PutEntry selfentry;
        auto stat = _put(height - 1, id, key, val, selfentry, lg_tlb);
        if(!stat.ok() || !selfentry.update) 
            return stat;

        return node->put(pos, selfentry.key, selfentry.val, 
                         entry, lg_tlb);
    }

    std::tuple<pgid_t, std::shared_mutex &> down(
        u32 height, pgid_t nodeid , std::string &key, 
        std::shared_mutex &par_mtx) {

        if(height == 1) {
            return std::forward_as_tuple(nodeid, par_mtx);
        }
        auto node = _inner_map.get(nodeid);
        auto [id, pos] = node->get(key, par_mtx);
        (void)pos;
        if(height == 2) {
            return std::forward_as_tuple(id, node->getMutex());
        }
        return down(height - 1, id, key, node->getMutex()); 
    }

    pgid_t down(u32 height, pgid_t nodeid, std::string &key) {
        if(height == 1) {
            return nodeid;
        }
        auto node = _inner_map.get(nodeid);
        auto [id, pos] = node->get(key);
        (void)pos;
        if(height == 2) {
            return id;
        }
        return down(height - 1, id, key); 
    }

    std::shared_mutex &mutex() {
        return _root_mtx;
    }

    void debug() {
        if(_height > 1) {
            _inner_map.get(_root)->debug(_height);
        }
    }

    u32           _order{0};
    u32           _height{0};
    pgid_t        _root{0};
    pgid_t        _first{0};
    std::string   _name;
    comparator_t  _cmp;
    DBImpl            *_db{nullptr};
    std::shared_mutex  _root_mtx;
    NodeMap <LeafNode>  _leaf_map;
    NodeMap <InnerNode> _inner_map;
};

}// namespace bptdb

#endif
