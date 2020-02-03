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
        std::memmove(it, it + 1, end - it - 1);
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
        std::memmove(next, next + 1, end - next - 1);
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
        if(_pg->overFlow(sizeof(Elem))) {
            hdr = (PageHeader *)_pg->extend(this, sizeof(Elem));
            freePage(_tmp.pos, _tmp.len);
        }
        std::memmove(it + 1, it, (end - it) * sizeof(Elem));
        *it = cur;
        hdr->size++;
        hdr->bytes += sizeof(Elem);
    }
    _pg->write(_fm);
}

pgid_t PageAllocator::reallocPage(pgid_t pos, u32 len, u32 newlen) {
    auto hdr = (PageHeader *)_pg->data();
    //self
    if(pos == hdr->res) {
        //std::cout << "self\n";
        _tmp = {pos, len};
        return allocPage(newlen);
    }
    freePage(pos, len);
    return allocPage(newlen);
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
