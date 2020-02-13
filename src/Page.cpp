#include <iostream>
#include <cstring>
#include <cassert>
#include "FileManager.h"
#include "PageAllocator.h"
#include "Page.h"

namespace bptdb {

Page::Page(pgid_t id, u32 page_size) {
    _id = id;
    _page_size = page_size;
}

Page::Page(pgid_t id, u32 page_size, u32 data_pgs) {
    _id = id;
    _page_size = page_size;
    _data_pgs = data_pgs;
    _data = (char *)std::malloc(_page_size * _data_pgs);
    //memset(_data, 0, _page_size * _data_pgs);
}

Page::~Page() {
    if(_data) std::free(_data);
}

void *Page::read(FileManager *fm) {
    assert(_data == nullptr); 
    _data = (char *)std::malloc(_page_size);
    _readPage(fm, _data, 1, _id);

    auto hdr = (PageHeader *)_data;
    _data_pgs = byte2page(hdr->bytes);
    u32 datapages = _data_pgs;
    std::cout << "datapages " << datapages << "\n";

    _data = (char *)std::realloc(_data, datapages * _page_size);
    // 更新header
    hdr = (PageHeader *)_data;

    datapages--;

    // read res content from disk
    if(datapages > 0) {
        u32 toread = std::min(datapages, hdr->hdrpages - 1);
        _readPage(fm, _data + _page_size, toread, _id + 1);
        datapages -= toread;
    }
    if(datapages > 0) {
        _readPage(fm, _data + _page_size * hdr->hdrpages,
                datapages, hdr->res);
    }
    return _data;
}

void *Page::extend(PageAllocator *pa, u32 extbytes) {
    assert(_data);
    auto hdr = (PageHeader *)_data;
    u32 extpages = byte2page(hdr->bytes + extbytes) - _data_pgs;
    //std::cout << "extpages " << extpages << "\n";
    _data_pgs += extpages;

    // we have not enought space on disk, realloc on disk.
    if(_data_pgs > hdr->realpages) {
        u32 reslen = hdr->realpages - hdr->hdrpages;
        hdr->realpages += extpages;
        if(hdr->res == 0)
            hdr->res = pa->allocPage(extpages);
        else 
            hdr->res = pa->reallocPage(hdr->res, reslen, reslen + extpages);
    }
    // we have not enought space on memory, realloc on memory.
    _data = (char *)std::realloc(_data, _page_size * _data_pgs);
    return _data;
}

void Page::writeBatch(FileManager *fm, 
        std::vector<std::shared_ptr<Page>> &pages) {
    for(auto &pg: pages) {
        pg->_write(fm);
    }
    fm->flush();
}

void Page::_write(FileManager *fm) {
    assert(_data);
    auto hdr = (PageHeader *)_data;
    u32 total = _data_pgs;
    u32 towrite = std::min(hdr->hdrpages, total);
    //std::cout << _page_size << " to write " << towrite << " id " << _id << "\n";
    _writePage(fm, _data, towrite, _id);
    total -= towrite;
    if(total > 0) {
        _writePage(fm, _data + _page_size * towrite, total, hdr->res);
    }
}

void Page::write(FileManager *fm) {
    _write(fm);
    fm->flush();
}

void Page::_readPage(FileManager *fm, char *buf, u32 cnt, u32 pos) {
    fm->read(buf, cnt * _page_size, pos * _page_size);
}

void Page::_writePage(FileManager *fm, char *buf, u32 cnt, u32 pos) {
    fm->writebuffer(buf, cnt * _page_size, pos * _page_size);
}

u32 Page::byte2page(u32 bytes) {
    return (bytes + _page_size - 1) / _page_size;
}

bool Page::overFlow(u32 extbytes) {
    auto hdr = (PageHeader *)_data;
    return (hdr->bytes + extbytes > _data_pgs * _page_size);
}

// free on disk and memory
void Page::free(PageAllocator *pa) {
    assert(_data);
    auto hdr = (PageHeader *)_data;
    pa->freePage(_id, hdr->hdrpages);
    if(hdr->res) {
        pa->freePage(hdr->res, hdr->realpages - hdr->hdrpages);
    }
    std::free(_data);
    _data = nullptr;
}

}
