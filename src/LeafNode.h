#ifndef __LEAF_NODE_H
#define __LEAF_NODE_H

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
    LeafNode(pgid_t id, u32 maxsize, DB *db, 
             NodeMap<LeafNode> *map): Node(id, maxsize, db) {
        _map = map;
    }

    Status put(KType &key, VType &val, 
            PutEntry<KType> &entry, 
            std::vector<PagePtr> &dirty,
            std::shared_mutex &par_mtx) {

        auto [hdr, pg] = handlePage(_container);
        // only leafcontainer have to find
        if(_container.find(key)) {
            return Status(error::keyRepeat);
        }

        _valid_iters.clear();
        auto extbytes = _container.elemSize(key, val);
        // there enought space
        if(pg->overFlow(extbytes)) {
            hdr = (PageHeader *)pg->extend(_db->getPageAllocator(), extbytes);
            containerReset(hdr, _container);
        }
        _container.put(key, val);
        //std::cout << "hdr->size " << hdr->size << "\n";
        //std::cout << hdr->pghdr.bytes << "\n";

        if(ifsplit(hdr)) {
            std::cout << "=====> leafnode split \n";
            split(hdr, entry, dirty, _container);
            entry.update = true;
        }
        dirty.push_back(pg);
        return Status();
    }

    Status del(KType &key, DelEntry<KType> &entry, 
            std::vector<PagePtr> &dirty, 
            std::shared_mutex &par_mtx) {

        auto [hdr, pg] = handlePage(_container);
        // the key is not exist
        if(!_container.find(key)) {
            return Status(error::keyNotFind);
        }
        _container.del(key);
        //std::cout << "hdr->size " << hdr->size << "\n";
        if(hdr->size == 0) {
            std::cout << hdr->bytes << "\n";
        }

        if(!ifmerge(hdr) || entry.last || !hdr->next) {
            dirty.push_back(pg);
            return Status();
        }

        auto &next_container = _map->get(hdr->next)->_container;

        std::cout << "=====> leafnode borrow\n";
        if(borrow(hdr, pg, entry, dirty, _container, next_container)) {
            dirty.push_back(pg);
            return Status();
        }

        std::cout << "=====> leafnode merge\n";
        // 3. del node on map
        _map->del(merge(hdr, pg, entry, dirty, _container, next_container));

        dirty.push_back(pg);
        return Status(); 
    }

    Status get(KType &key, VType &val, 
            std::shared_mutex &par_mtx) {

        handlePage(_container);
        if(!_container.find(key)) {
            return Status(error::keyNotFind);
        }
        val = _container.get(key);
        return Status();
    }
    Status update(KType &key, VType &val, 
            std::vector<PagePtr> &dirty,
            std::shared_mutex &par_mtx) {

        auto [hdr, pg] = handlePage(_container);
        if(!_container.find(key)) {
            return Status(error::keyNotFind);
        }

        _valid_iters.clear();
        auto extbytes = _container.elemSize(key, val);

        if(pg->overFlow(extbytes)) {
            hdr = (PageHeader *)pg->extend(_db->getPageAllocator(), extbytes);
            containerReset(hdr, _container);
        }
        _container.update(key, val);
        dirty.push_back(pg);
        return Status();
    }
    // =====================================================

    Iter_t begin() {
        auto it =  _container.begin();
        _valid_iters.insert(it->_id);
        return it;
    }
    Iter_t upper(KType &key, std::shared_mutex &par_mtx) {
        auto it =  _container.upper(key);
        _valid_iters.insert(it->_id);
        return it;
    }
    Iter_t lower(KType &key, std::shared_mutex &par_mtx) {
        auto it =  _container.lower(key);
        _valid_iters.insert(it->_id);
        return it;
    }
    Iter_t at(KType &key, std::shared_mutex &par_mtx) {
        auto it =  _container.at(key);
        _valid_iters.insert(it->_id);
        return it;
    }
    bool updateIter(Iter_t &iter, IterMoveType type) {
        if(_valid_iters.find(iter._id) == _valid_iters.end()) {
            return false;
        }
        _container.updateIter(iter, type);    
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
    LeafNode *next() {
        auto [hdr, pg] = handlePage(_container);
        if(hdr->next == 0) {
            return nullptr;
        }
        return _map->get(hdr->next);
    }

private:
    ContainerType     _container;
    std::set<u32>     _valid_iters;
    NodeMap<LeafNode> *_map;
};

}// namespace bptdb

#endif
