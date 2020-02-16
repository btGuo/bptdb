#include <cstring>
#include <iostream>
#include <memory>
#include "PageAllocator.h"
#include "DB.h"
#include "Page.h"
#include "PageCache.h"

namespace bptdb {

void PageAllocator::newOnDisk(pgid_t root, FileManager *fm, 
        u32 page_size, u32 start_pos) {
    Page pg(root, page_size, 1);
    auto hdr = (PageHeader *)pg.data();
    PageHeader::init(hdr, 1, start_pos);
    pg.write(fm);
}

PageAllocator::PageAllocator(pgid_t root, FileManager *fm, u32 page_size) {
    _page_size = page_size;
    _root = root;
    _fm = fm;
    _pg = std::make_unique<Page>(_root, page_size);
    _pg->read(fm);
}

pgid_t PageAllocator::allocPage(u32 len) {

    std::lock_guard lg(_mtx);

    auto hdr = (PageHeader *)_pg->data();
    auto begin = (Elem *)(hdr + 1);
    auto end = begin + hdr->size;
    auto it = std::find_if(begin, end, 
            [len](Elem &x){ return x.len >= len; });
    if(it == end) {
        auto ret = hdr->next;
        hdr->next += len;
        _pg->write(_fm);
        return ret;
    }
    auto ret = it->pos;
    if(it->len == len) {
        std::memmove(it, it + 1, (end - it - 1) * sizeof(Elem));
        hdr->size -= 1;
        hdr->bytes -= sizeof(Elem);
    }else {
        it->len -= len;
        it->pos += len;
    }
    _pg->write(_fm);
    return ret;
}

void PageAllocator::freePage(pgid_t pos, u32 len) {

    std::lock_guard lg(_mtx);

    assert(len);
    Elem cur{pos, len};
    auto hdr = (PageHeader *)_pg->data();
    auto begin = (Elem *)(hdr + 1);
    auto end = begin + hdr->size;
    
    auto it = std::lower_bound(
        begin, end, cur, 
        [](const Elem &e1, const Elem &e2){
            return e1.pos < e2.pos; 
        });
    if(it != end) {
        assert(it->pos != pos);
    }
    auto next = it;
    auto prev = it - 1;
    if(next == end)   next = nullptr;
    if(prev <  begin) prev = nullptr;

    if(prev && adjacent(prev, &cur) &&
            next && adjacent(&cur, next)){
            
        // merge
        prev->len += cur.len + next->len;
        std::memmove(next, next + 1, (end - next - 1) * sizeof(Elem));
        hdr->size--;
        hdr->bytes -= sizeof(Elem);
    }
    else if(prev && adjacent(prev, &cur)) {
        prev->len += cur.len;
    }
    else if(next && adjacent(&cur, next)) {
        next->len += cur.len;
        next->pos -= cur.len;
    }
    else {
        std::memmove(it + 1, it, (end - it) * sizeof(Elem));
        *it = cur;
        hdr->size++;
        hdr->bytes += sizeof(Elem);
    }

    // last
    if(_pg->overFlow(sizeof(Elem))) {
        DEBUGOUT("overflow");
        hdr = (PageHeader *)extendPage(sizeof(Elem));
        hdr->bytes += sizeof(Elem);
        // 第一次extend不会realloc
        if(_tmp.len) {
            freePage(_tmp.pos, _tmp.len);
        }
    }
    _pg->write(_fm);
}

pgid_t PageAllocator::reallocPage(pgid_t pos, u32 len, u32 newlen) {

    std::lock_guard lg(_mtx);

    assert(len);
    assert(newlen > len);

    freePage(pos, len);
    return allocPage(newlen);
}

// do this by self other call pg->extend;
void *PageAllocator::extendPage(u32 extbytes) {
    assert(_pg->_data);
    auto hdr = (PageHeader *)_pg->_data;
    u32 extpages = _pg->byte2page(hdr->bytes + extbytes) - _pg->_data_pgs;
    _pg->_data_pgs += extpages;

    // we have not enought space on disk, realloc on disk.
    if(_pg->_data_pgs > hdr->realpages) {
        assert(hdr->realpages >= hdr->hdrpages);
        u32 reslen = hdr->realpages - hdr->hdrpages;
        hdr->realpages += extpages;

        if(hdr->res == 0) {
            hdr->res = hdr->next;
            hdr->next += extpages;
        }else {
            assert(reslen > 0);
            _tmp.pos = hdr->res;
            _tmp.len = reslen;
            hdr->res = hdr->next;
            hdr->next += (reslen + extpages);
        }
    }
    // we have not enought space on memory, realloc on memory.
    _pg->_data = (char *)std::realloc(_pg->_data, 
                                      _pg->_page_size * _pg->_data_pgs);
    return _pg->_data;
}

void PageAllocator::show() {
    auto hdr = (PageHeader *)_pg->data();
    auto begin = (Elem *)(hdr + 1);
    auto end = begin + hdr->size;
    std::cout << "pos  ";
    for(auto it = begin; it != end; it++) {
        std::cout << it->pos << " ";
    }
    std::cout << "\nsize ";
    for(auto it = begin; it != end; it++) {
        std::cout << it->len << " ";
    }
    std::cout << "\n";
}

}// namespace bptdb
