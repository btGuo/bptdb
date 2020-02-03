#ifndef __INNER_NODE_H
#define __INNER_NODE_H

#include "__Node.h"
#include "InnerContainer.h"

namespace bptdb {

template <typename KType, typename Comp>
class InnerNode: public Node {
public:
    InnerNode(pgid_t id, u32 maxsize, DB *db, 
            NodeMap<InnerNode> *map): Node(id, maxsize, db){
        _map = map;
    }

    Status put(u32 pos, KType &key, pgid_t &val, 
            PutEntry<KType> &entry, 
            std::vector<PagePtr> &dirty, 
            std::shared_mutex &par_mtx) {

        auto [hdr, pg] = handlePage(_container);
        auto extbytes = _container.elemSize(key, val);
        // there enought space
        if(pg->overFlow(extbytes)) {
            hdr = (PageHeader *)pg->extend(_db->getPageAllocator(), extbytes);
            containerReset(hdr, _container);
        }
        _container.putat(pos, key, val);
        //std::cout << "hdr->size " << hdr->size << "\n";
        //std::cout << hdr->pghdr.bytes << "\n";

        if(ifsplit(hdr)) {
            std::cout << "=====> innernode split \n";
            split(hdr, entry, dirty, _container);
            entry.update = true;
        }
        dirty.push_back(pg);
        return Status();
    }

    void del(u32 pos, DelEntry<KType> &entry,
            std::vector<PagePtr> &dirty,
            std::shared_mutex &par_mtx) {

        auto [hdr, pg] = handlePage(_container);
        // delete key at pos
        _container.delat(pos);

        // if legal or we are the last child of parent
        // return once.
        if(!ifmerge(hdr) || entry.last || !hdr->next) {
            dirty.push_back(pg);
            return;
        }

        auto &next_container = _map->get(hdr->next)->_container;

        std::cout << "=====> innernode borrow\n";
        if(borrow(hdr, pg, entry, dirty, _container, next_container)) {
            dirty.push_back(pg);
            return;
        }

        std::cout << "=====> innernode merge\n";
        _map->del(merge(hdr, pg, entry, dirty, _container, next_container));
        dirty.push_back(pg);
    }

    void update(u32 pos, KType &newkey, 
            std::vector<PagePtr> &dirty,
            std::shared_mutex &par_mtx) {

        //std::cout << "update " << newkey << "\n";
        auto [hdr, pg] = handlePage(_container);
        auto extbytes = _container.elemSize(newkey, 0);
        if(pg->overFlow(extbytes)) {
            hdr = (PageHeader *)pg->extend(_db->getPageAllocator(), extbytes);
            containerReset(hdr, _container);
        }
        _container.updateKeyat(pos, newkey);
        dirty.push_back(pg);
    }

    std::tuple<pgid_t, u32> get(
        KType &key, std::shared_mutex &par_mtx) {

        handlePage(_container);
        return _container.get(key);
    }

    std::tuple<pgid_t, u32> get(
        KType &key, DelEntry<KType> &entry, 
        std::shared_mutex &par_mtx) {

        handlePage(_container);
        return _container.get(key, entry);
    }

    static void newOnDisk(
        pgid_t id, FileManager *fm, u32 page_size, 
        KType &key, pgid_t child1, pgid_t child2) {

        Page pg(id, page_size, 1);
        auto hdr = (PageHeader *)pg.data();
        PageHeader::init(hdr, 1, 0);

        // 初始化容器
        ContainerType con;
        containerReset(hdr, con);
        con.init(key, child1, child2);

        pg.write(fm);
    }

    InnerNode *next() {
        auto [hdr, pg] = handlePage(_container);
        if(hdr->next == 0) {
            return nullptr;
        }
        return _map->get(hdr->next);
    }

    bool empty() {
        auto [hdr, pg] = handlePage(_container);
        return hdr->size == 0;
    }

    // return the only child in node
    pgid_t tochild() {
        //std::cout << "tochild\n";
        auto [hdr, pg] = handlePage(_container);
        assert(hdr->next == 0);
        auto ret =  _container.head();
        // free self page
        pg->free(_db->getPageAllocator());
        _db->getPageCache()->del(ret);
        return ret;
    }
private:
    using ContainerType = InnerContainer<KType, Comp>;
    ContainerType _container;
    NodeMap<InnerNode> *_map;
};

}// namespace bptdb

#endif
