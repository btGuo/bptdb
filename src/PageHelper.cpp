#include <iostream>
#include <cstring>
#include <cassert>
#include "FileManager.h"
#include "PageAllocator.h"
#include "PageHelper.h"
#include "Option.h"
#include "PageCache.h"
#include "PageHeader.h"

namespace bptdb {

PageHelper::PageHelper(pgid_t id) {
    assert(id > 0);
    _id = id;
}

PageHelper::PageHelper(pgid_t id, u32 data_pgs) {
    assert(id > 0);
    assert(data_pgs > 0);
    _id = id;
    _data_pgs = data_pgs;
    _data = (char *)std::malloc(g_option.page_size * _data_pgs);
}

PageHelper::~PageHelper() {
    if(_data) std::free(_data);
}

void *PageHelper::read() {
    assert(_data == nullptr); 
    _data = (char *)std::malloc(g_option.page_size);
    _readPage(_data, 1, _id);

    auto hdr = (PageHeader *)_data;
    _data_pgs = byte2page(hdr->bytes);
    u32 datapages = _data_pgs;

    //std::cout << "datapages " << datapages << "\n";
    assert(datapages > 0);

    _data = (char *)std::realloc(_data, datapages * g_option.page_size);
    // 更新header
    hdr = (PageHeader *)_data;

    datapages--;

    // read res content from disk
    if(datapages > 0) {
        u32 toread = std::min(datapages, hdr->hdrpages - 1);
        _readPage(_data + g_option.page_size, toread, _id + 1);
        datapages -= toread;
    }
    if(datapages > 0) {
        _readPage(_data + g_option.page_size * hdr->hdrpages,
                datapages, hdr->res);
    }
    return _data;
}

void *PageHelper::extend(u32 extbytes) {
    assert(_data);
    auto hdr = (PageHeader *)_data;
    u32 extpages = byte2page(hdr->bytes + extbytes) - _data_pgs;
    _data_pgs += extpages;

    // we have not enought space on disk, realloc on disk.
    if(_data_pgs > hdr->realpages) {
        assert(hdr->realpages >= hdr->hdrpages);
        u32 reslen = hdr->realpages - hdr->hdrpages;
        hdr->realpages += extpages;
        if(hdr->res == 0)
            hdr->res = g_pa->allocPage(extpages);
        else {
            assert(reslen > 0);
            hdr->res = g_pa->reallocPage(hdr->res, reslen, reslen + extpages);
        }
    }
    // we have not enought space on memory, realloc on memory.
    _data = (char *)std::realloc(_data, g_option.page_size * _data_pgs);
    return _data;
}

void PageHelper::write() {
    assert(_data);
    auto hdr = (PageHeader *)_data;
    u32 total = _data_pgs;
    u32 towrite = std::min(hdr->hdrpages, total);
    //std::cout << g_option.page_size << " to write " << towrite << " id " << _id << "\n";
    _writePage(_data, towrite, _id);
    total -= towrite;
    if(total > 0) {
        _writePage(_data + g_option.page_size * towrite, total, hdr->res);
    }
}

void PageHelper::_readPage(char *buf, u32 cnt, u32 pos) {
    // g_fm->read(buf, cnt * g_option.page_size, pos * g_option.page_size);
    for (u32 i = 0; i < cnt; i++) {
        g_pc->read(pos, buf);
        pos++;
        buf += g_option.page_size;
    }
}

void PageHelper::_writePage(char *buf, u32 cnt, u32 pos) {
    // g_fm->write(buf, cnt * g_option.page_size, pos * g_option.page_size);
    for (u32 i = 0; i < cnt; i++) {
        g_pc->write(pos, buf);
        pos++;
        buf += g_option.page_size;
    }
}

bool PageHelper::overFlow(u32 extbytes) {
    auto hdr = (PageHeader *)_data;
    return (hdr->bytes + extbytes > _data_pgs * g_option.page_size);
}

// free on disk and memory
void PageHelper::free() {
    assert(_data);
    auto hdr = (PageHeader *)_data;
    g_pa->freePage(_id, hdr->hdrpages);
    if(hdr->res) {
        g_pa->freePage(hdr->res, hdr->realpages - hdr->hdrpages);
    }
    std::free(_data);
    _data = nullptr;
}

}

