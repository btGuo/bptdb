#include "Page.h"
#include "PageCache.h"

namespace bptdb {

PagePtr PageCache::get(pgid_t key) {
    assert(key > 1);
    std::unique_lock<std::mutex> clock(_cache_mtx);
    auto it = _cache.find(key);
    if(it == _cache.end())
        return std::shared_ptr<Page>();
    auto pg = it->second;
    clock.unlock();
    std::lock_guard<std::mutex> llock(_lru_mtx);
    _lru.erase(pg.get());
    _lru.push_front(pg.get());
    return pg;
}

void PageCache::del(pgid_t key) {
    assert(key > 1);
    _page_count--;
    std::lock_guard<std::mutex> clock(_cache_mtx);
    std::lock_guard<std::mutex> llock(_lru_mtx);
    auto it = _cache.find(key);
    assert(it != _cache.end());
    _lru.erase(it->second.get());
    _cache.erase(key);
}

}// namespace bptdb
