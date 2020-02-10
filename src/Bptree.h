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
#include "DB.h"

namespace bptdb {

template <typename KType, typename VType, typename OrderType>
class Bptree {
public:
    using compare_t = std::conditional_t<
          std::is_same_v<OrderType, keyOrder::ASCE>, 
          keycompare::less<KType>, 
          keycompare::greater<KType> >;

    using LeafNode_t  = LeafNode<KType, VType, compare_t>;
    using InnerNode_t = InnerNode<KType, compare_t>;
    using Iter_t      = typename LeafNode_t::Iter_t;
    using IterPtr_t   = typename LeafNode_t::IterPtr_t;
    using UnWLockGuardVec_t = UnLockGuardArray<UnWriteLockGuard>;

    // ====================================================

    class Iterator {
        friend class Bptree;
    public:    
        Iterator() = default;
        KType key()    { return it->key(); }
        VType val()    { return it->val(); }
        bool  valid()  { return _valid; }
        bool  done() { return _done; }
        void next() {
            if(!_valid) {
                throw "unvalid iter";
            }
            if(_done) {
                throw "out of range";
            }
            // if we reach the last elem
            if(it->lastElem()) {
                node = node->next();
                if(node == nullptr) {
                    _done = true;
                    return;
                }
                it = node->begin();
                return;
            }
            _valid = node->updateIter(it, IterMoveType::Forward);
        }
    private:
        bool _done{false};
        bool _valid{true};
        LeafNode_t *node{nullptr};
        IterPtr_t it;
    };

    // ====================================================

    enum class IterHandleType {
        upper, lower, at,
    };

    Iterator handlerIter(KType &key, IterHandleType type) {
        auto [nodeid, mutex] = down(_height, _root, key, _root_mtx);
        auto node = getLeafNode(nodeid);
        Iterator it;
        it.node = node;

        if(type == IterHandleType::upper) {
            it.it = node->upper(key, mutex);
        }
        else if(type == IterHandleType::lower) {
            it.it = node->lower(key, mutex);
        }
        else if(type == IterHandleType::at) {
            it.it = node->at(key, mutex);
        }
        else {
            // should not be here
            assert(0);
        }
        return it;
    }
    Iterator begin() {
        auto node = _leaf_map.get(_first);
        Iterator it;
        it.node = node;
        it.it = node->begin();
        return it;
    }
    //Iterator upper(KType &key) {
    //    return handlerIter(key, IterHandleType::upper);
    //}
    //Iterator lower(KType &key) {
    //    return handlerIter(key, IterHandleType::lower);
    //}
    Iterator at(KType &key) {
        return handlerIter(key, IterHandleType::at);
    }
    // ====================================================

    Bptree(std::string name, BptreeMeta meta, DB *db):
    _leaf_map(meta.order, db), _inner_map(meta.order, db){
        _name   = name;
        _order  = meta.order;
        _height = meta.height;
        _root   = meta.root;
        _first  = meta.first;
        _db     = db;
    }

    static void newOnDisk(pgid_t id, FileManager *fm, u32 page_size) {
        LeafNode_t::newOnDisk(id, fm, page_size);
    }

    //====================================================================

    Status get(KType key, VType &val) {
        _root_mtx.lock_shared();
        auto [nodeid, mutex] = down(_height, _root, key, _root_mtx);
        return _leaf_map.get(nodeid)->get(key, val, mutex);
    }

    Status update(KType key, VType val) {
        _root_mtx.lock_shared();
        auto [nodeid, mutex] = down(_height, _root, key, _root_mtx);
        return  _leaf_map.get(nodeid)->update(key, val, mutex);
    }

    Status put(KType key, VType val) {
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

        PutEntry<KType> entry;
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
        InnerNode_t::newOnDisk(_root,
                _db->getFileManager(), _db->getPageSize(),
                entry.key, prev, entry.val);

        _height++;
        _db->updateRoot(_name, _root, _height);
        return stat;
    }

    Status del(KType key) {
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

        DelEntry<KType> entry;
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

    Status _del(u32 height, pgid_t nodeid, KType &key,
                DelEntry<KType> &entry, 
                UnWLockGuardVec_t &lg_tlb) {

        if(height == 1) {
            auto node = _leaf_map.get(nodeid);
            return node->del(key, entry);
        }
        DelEntry<KType> selfentry;
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

    Status _put(u32 height, pgid_t nodeid, KType &key, VType &val, 
                PutEntry<KType> &entry, UnWLockGuardVec_t &lg_tlb) {

        if(height == 1) {
            auto node = _leaf_map.get(nodeid);
            return node->put(key, val, entry);
        }
        auto node = _inner_map.get(nodeid);
        auto [id, pos] = node->get(key, lg_tlb);

        PutEntry<KType> selfentry;
        auto stat = _put(height - 1, id, key, val, selfentry, lg_tlb);
        if(!stat.ok() || !selfentry.update) 
            return stat;

        return node->put(pos, selfentry.key, selfentry.val, 
                         entry, lg_tlb);
    }

    std::tuple<pgid_t, std::shared_mutex &> down(
        u32 height, pgid_t nodeid , KType &key, 
        std::shared_mutex &par_mtx) {

        if(height == 1) {
            return std::forward_as_tuple(nodeid, par_mtx);
        }
        auto node = _inner_map.get(nodeid);
        auto [id, pos] = node->get(key, par_mtx);
        if(height == 2) {
            return std::forward_as_tuple(id, node->getMutex());
        }
        return down(height - 1, id, key, node->getMutex()); 
    }

    u32           _order{0};
    u32           _height{0};
    pgid_t        _root{0};
    pgid_t        _first{0};
    std::string   _name;
    DB            *_db{nullptr};
    std::shared_mutex  _root_mtx;
    NodeMap <LeafNode_t>  _leaf_map;
    NodeMap <InnerNode_t> _inner_map;
};

}// namespace bptdb

#endif
