#ifndef __PAGE_Cache_H
#define __PAGE_Cache_H

#include <cstdlib>
#include <atomic>
#include <cassert>
#include <functional>
#include <mutex>
#include <map>
#include <memory>
#include <list>
#include <shared_mutex>
#include <utility>
#include <vector>

#include "FileManager.h"
#include "common.h"
#include "List.h"
#include "Page.h"

namespace bptdb {

class PageCache {
public:
    PageCache(u32 max_page);
    void read(pgid_t id, void *dest);
    void write(pgid_t id, void *src);
    std::vector<PagePtr> collectDirty();
    void start();
    void stop();
    bool alive();
private:
    // PagePtr readWrite(pgid_t id);
    PagePtr tryGet(pgid_t id);
    PagePtr insertNew(pgid_t id);
    static void run();
    u32 _max_page{0};
    std::atomic<u32> _page_count{0};
    std::map<pgid_t, PagePtr> _cache;
    std::shared_mutex _shmtx;
    std::atomic_bool _stop{false};
    List<Page> _lru; // 侵入式链表，并不拥有Page所有权
};

extern std::shared_ptr<PageCache> g_pc;

}// namespace bptdb
#endif
