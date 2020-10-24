#include <algorithm>
#include <functional>
#include <future>
#include <list>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <utility>
#include <vector>
#include <thread>
#include <chrono>
#include "Page.h"
#include "PageCache.h"

namespace bptdb {

std::shared_ptr<PageCache> g_pc;

PageCache::PageCache(u32 max_page): _lru(Page::lru_tag()) {
    _max_page = max_page;
}

void PageCache::start() {
    _f = std::async(std::launch::async, PageCache::run);
}

void PageCache::stop() {
    _stop.store(true);
    _f.get();
}

PagePtr PageCache::tryGet(pgid_t id) {
    std::shared_lock lg(_shmtx);
    auto it = _cache.find(id);
    if (it == _cache.end()) {
        return std::shared_ptr<Page>();
    }
    auto pg = it->second;
    _lru.move_to_front(pg.get());
    return pg;
}

PagePtr PageCache::insertNew(pgid_t id) {
    std::unique_lock lg(_shmtx);
    auto pg = std::make_shared<Page>(id);
    _page_count++;
    if (_page_count > _max_page) {
        auto raw = _lru.pop_back();
        auto it = _cache.find(raw->getId());
        it->second->flush();
        _cache.erase(it);
        _page_count--;
    }
    _cache.insert({pg->getId(), pg});
    _lru.push_front(pg.get());
    return pg;
}

void PageCache::read(pgid_t id, void *dest) {
    auto pg = tryGet(id);
    if (!pg) {
        pg = insertNew(id);
    }
    pg->read(dest);
}

void PageCache::write(pgid_t id, void *src) {
    auto pg = tryGet(id);
    if (pg) {
        pg->write(src);
        return;
    }
    g_fm->write((char *)src, g_option.page_size, id * g_option.page_size);
}

bool PageCache::alive() {
    return !_stop.load();
}

void PageCache::run() {
    DEBUGOUT("PageCache start...");
    auto pc = g_pc;
    while (pc->alive()) {
        auto dirty_pgs = pc->collectDirty();
        std::for_each(dirty_pgs.begin(), 
                dirty_pgs.end(), [](PagePtr pg) { pg->flush(); });
        if (!pc->alive()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::seconds(10));
    }
    auto dirty_pgs = pc->collectDirty();
    std::for_each(dirty_pgs.begin(), 
            dirty_pgs.end(), [](PagePtr pg) { pg->flush(); });
    DEBUGOUT("PageCache stop...");
}

std::vector<PagePtr> PageCache::collectDirty() {
    std::vector<PagePtr> dirty_pgs;
    std::shared_lock lg(_shmtx);
    for (auto it = _cache.begin(); it != _cache.end(); ++it) {
        if (it->second->dirty()) {
            dirty_pgs.push_back(it->second);
        }
    }
    return dirty_pgs;
}

}// namespace bptdb
