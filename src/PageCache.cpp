#include "Page.h"
#include "PageCache.h"

namespace bptdb {

PagePtr PageCache::get(pgid_t key) {
    assert(key > 1);
    std::lock_guard lg(_mtx);

    auto it = _cache.find(key);
    if(it == _cache.end())
        return std::shared_ptr<Page>();
    auto pg = it->second;
    _lru.erase(pg.get());
    _lru.push_front(pg.get());
    return pg;
}

void PageCache::del(pgid_t key) {
    assert(key > 1);
    std::lock_guard lg(_mtx);

    _page_count--;
    auto it = _cache.find(key);
    assert(it != _cache.end());
    _lru.erase(it->second.get());
    _cache.erase(key);
}

}// namespace bptdb
