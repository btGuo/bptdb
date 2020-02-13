#ifndef __PAGE_Cache_H
#define __PAGE_Cache_H

#include <cstdlib>
#include <unordered_map>
#include <atomic>
#include <cassert>
#include <mutex>
#include <memory>

#include "common.h"
#include "List.h"
#include "Page.h"

namespace bptdb {

class PageCache {
public:
    PageCache(u32 max_page): _lru(Page::lru_tag()){
        _max_page = max_page;
    }

    template <typename ... Args>
    PagePtr alloc(Args && ... args) {
        auto pg = std::make_shared<Page>(std::forward<Args>(args)...);
        _page_count++;
        std::lock_guard lg(_mtx);
        if(_page_count > _max_page) {
            DEBUGOUT("page drop");
            auto pg = _lru.pop_back();    
            _cache.erase(pg->getId());
        }
        _cache.insert({pg->getId(), pg});
        _lru.push_front(pg.get());
        return pg;
    }
    PagePtr get(pgid_t key);
    void del(pgid_t key);
private:
    u32 _max_page{0};
    std::atomic<u32> _page_count{0};
    std::unordered_map<pgid_t, PagePtr> _cache;
    std::mutex _mtx;
    List<Page> _lru; // 侵入式链表，并不拥有Page所有权
};

}// namespace bptdb
#endif
