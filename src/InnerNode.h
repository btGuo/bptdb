#ifndef __INNER_NODE_H
#define __INNER_NODE_H

#include "Node.h"
#include "InnerContainer.h"
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
        Node(id, maxsize), _container(cmp), _map(map){}

    // ==================================================================

    static void containerReset(PageHeader *hdr, InnerContainer &con) {
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
            containerReset((PageHeader*)pg->data(), _container);
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

        InnerContainer next_container(_container);
        containerReset(next_hdr, next_container);

        entry.val = next_pg->getId();
        entry.key = _container.splitTo(next_container);
        entry.update = true;
        
        next_pg->write();
    }

    bool borrow(PageHeader *hdr, PageHelperPtr pg, DelEntry &entry) {

        auto next = _map->get(hdr->next);
        auto [next_hdr, next_pg] = next->handlePage();
        auto &next_container = next->_container;

        if(!hasmore(next_hdr)) {
            return false;
        }

        // diff with leafnode. here we use entry.delim.
        hdr = handleOverFlow(pg, _container.elemSize(
                entry.delim, next_container.val(0)));

        entry.key = _container.borrowFrom(next_container, entry.delim);
        entry.update = true;
        next_pg->write();
        return true;
    }

    void merge(PageHeader *hdr, PageHelperPtr pg, DelEntry &entry) {

        auto next = _map->get(hdr->next);
        auto [next_hdr, next_pg] = next->handlePage();
        auto &next_container = next->_container;

        // diff with leafnode. here we add entry.delim.
        hdr = handleOverFlow(pg, next_hdr->bytes + entry.delim.size());

        _container.mergeFrom(next_container, entry.delim);
        entry.del = true;

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

    // the mutex must locked by func get() at here.
    Status put(u32 pos, std::string &key, pgid_t &val, 
            PutEntry &entry, UnWLockGuardVec_t &lg_tlb) {

        auto [hdr, pg] = handlePage();
        hdr = handleOverFlow(pg, _container.elemSize(key, val));
        _container.putat(pos, key, val);

        if(ifsplit(hdr)) {
            DEBUGOUT("===> innernode split");
            split(hdr, entry);
        }
        pg->write();
        // unlock self.
        lg_tlb.pop_back();
        return Status();
    }

    // the mutex must locked by func get() at here.
    void del(u32 pos, DelEntry &entry,
            UnWLockGuardVec_t &lg_tlb) {

        auto [hdr, pg] = handlePage();
        _container.delat(pos);

        // if legal or we are the last child of parent
        // return once.
        if(!ifmerge(hdr) || entry.last || !hdr->next) {
            goto done;
        }

        DEBUGOUT("===> innernode borrow");
        if(borrow(hdr, pg, entry)) {
            goto done;
        }

        DEBUGOUT("===> innernode merge");
        merge(hdr, pg, entry);
done:
        pg->write();
        lg_tlb.pop_back();
    }

    // the mutex must locked by func get() at here.
    void update(u32 pos, std::string &newkey, 
            UnWLockGuardVec_t &lg_tlb) {

        auto [hdr, pg] = handlePage();
        hdr = handleOverFlow(pg, _container.elemSize(newkey, 0));
        _container.updateKeyat(pos, newkey);

        pg->write();
        lg_tlb.pop_back();
    }

    // for put
    std::tuple<pgid_t, u32> 
    get(std::string &key, UnWLockGuardVec_t &lg_tlb) {

        _shmtx.lock();
        // keep page alive.
        auto [hdr, pg] = handlePage();
        (void)hdr;
        if(safetoput(hdr)) {
            lg_tlb.clear();
        }
        lg_tlb.emplace_back(_shmtx);
        return _container.get(key);
    }

    // for del
    std::tuple<pgid_t, u32> 
    get(std::string &key, DelEntry &entry, 
        UnWLockGuardVec_t &lg_tlb) {

        _shmtx.lock();
        // keep page alive.
        auto [hdr, pg] = handlePage();
        if(safetodel(hdr)) {
            lg_tlb.clear();
        }
        lg_tlb.emplace_back(_shmtx);
        return _container.get(key, entry);
    }

    // for search
    std::tuple<pgid_t, u32> 
    get(std::string &key, Mutex_t &par_mtx) {

        //lock self and release parent.
        _shmtx.lock_shared();
        par_mtx.unlock_shared();

        // keep page alive.
        auto [hdr, pg] = handlePage();
        (void)hdr; 
        return _container.get(key);
    }

    // without lock, only used by iterator.
    std::tuple<pgid_t, u32> 
    get(std::string &key) {
        // keep page alive.
        auto [hdr, pg] = handlePage();
        (void)hdr; 
        return _container.get(key);
    }

    //==================================================

    static void newOnDisk(
        pgid_t id, std::string &key, 
        pgid_t child1, pgid_t child2) {

        PageHelper pg(id, 1);
        auto hdr = (PageHeader *)pg.data();
        PageHeader::init(hdr, 1, 0);

        // 初始化容器
        InnerContainer con;
        containerReset(hdr, con);
        con.init(key, child1, child2);

        pg.write();
    }

    bool empty() {
        // keep page alive.
        auto [hdr, pg] = handlePage();
        return hdr->size == 0;
    }

    // return the only child in node
    pgid_t tochild() {
        //std::cout << "tochild\n";
        auto [hdr, pg] = handlePage();
        assert(hdr->next == 0);
        auto ret =  _container.head();
        // free self page
        pg->free();
        // _db->getPageCache()->del(ret);
        
        return ret;
    }

    std::string maxkey() {
        auto [hdr, pg] = handlePage();
        return std::string(_container.maxkey());
    }

    std::string minkey() {
        auto [hdr, pg] = handlePage();
        return std::string(_container.minkey());
    }

    void show() {
        auto [hdr, pg] = handlePage();
        std::cout << "size " << hdr->size << "\n";
        for(auto it = _container.begin(); !it.done(); it.next()) {
            std::cout << "key " << std::string(it.key()) << "\n";
        }
    }

    void debug(u32 height) {
        auto [hdr, pg] = handlePage();
        if(height == 2) {
            return;
        }
        pgid_t prev = _container.head(), cur = 0;
        _map->get(prev)->debug(height - 1);
        for(auto it = _container.begin(); !it.done(); it.next()) {
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
    InnerContainer _container;
    NodeMap<InnerNode> *_map;
};

}// namespace bptdb

#endif
