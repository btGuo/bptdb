#include <iostream>
#include <string_view>
#include <algorithm>
#include <cstdlib>
#include <thread>
#include <cstring>
#include "DB.h"
#include "PageAllocator.h"
#include "PageCache.h"
#include "FileManager.h"
#include "Bucket.h"
#include "common.h"
#include "Bptree.h"

namespace bptdb {

Status DB::open(std::string path, bool creat, Option option) {
    // test file
    std::fstream file(path, std::ios::in);
    // file is not exist
    if(!file.is_open()) {
        // if create the file
        if(creat)
            return create(path, option); 
        // return error
        return Status(error::DbOpenFailed);
    }
    file.close();
    // end

    // database exist
    // init member data
    _path = path;
    _fm = std::make_shared<FileManager>(_path, option.sync);
    _write_que = std::make_shared<WriteQue_t>();
    // read meta
    _fm->read((char *)&_meta, sizeof(Meta), 0);
    _pc = std::make_unique<PageCache>(_meta.max_buffer_pages);
    _pa = std::make_unique<PageAllocator>(_meta.freelist_id, 
                                          _fm.get(), _meta.page_size, _write_que.get());
    _buckets = std::make_shared<Bptree>(
        "__BUCKET_TREE__", _meta.bucket_tree_meta, this, std::less<std::string_view>());

    startWriteThread();
    return Status();
}
Status DB::create(std::string path, Option option) {

    // create file 
    std::fstream file(path, std::ios::out);
    if(!file.is_open()) {
        return Status(error::DbCreatFailed);
    }
    file.close();
    // end

    _path = path;
    // format the disk
    init(option);
    return Status();
}

void DB::init(Option option) {
    // init meta
    _meta.page_size = option.page_size;
    _meta.max_buffer_pages = option.max_buffer_pages;
    _meta.freelist_id = 1;
    auto tree_meta = &_meta.bucket_tree_meta;
    tree_meta->root = 2;
    tree_meta->first = 2;
    tree_meta->height = 1;
    tree_meta->order = 96;

    // create filemanager firstly
    _fm = std::make_shared<FileManager>(_path, option.sync);
    _write_que = std::make_shared<WriteQue_t>();

    // write meta
    _fm->write((char *)&_meta, sizeof(_meta), 0);
    // init pageAllocator on disk
    PageAllocator::newOnDisk(_meta.freelist_id, _fm.get(), 
                             _meta.page_size, _meta.freelist_id + 2);

    // create basic utils
    _pc = std::make_unique<PageCache>(_meta.max_buffer_pages);
    _pa = std::make_unique<PageAllocator>(_meta.freelist_id, _fm.get(), 
                                          _meta.page_size, _write_que.get());

    auto meta = _meta.bucket_tree_meta;
    // the id of bucket must be 2
    // init bucket on disk
    Bptree::newOnDisk(meta.root, _fm.get(), _meta.page_size);
    // create bucket tree
    _buckets = std::make_shared<Bptree>(
        "__BUCKET_TREE__", meta, this, std::less<std::string_view>());

    startWriteThread();
}

std::tuple<Status, Bucket> 
DB::createBucket(std::string name, comparator_t cmp) {

    BptreeMeta meta;
    auto id = _pa->allocPage(1);
    meta.root = id;
    meta.first = id;
    meta.height = 1;
    // TODO set right order
    meta.order = 128;

    std::string val((char *)&meta, sizeof(BptreeMeta));

    auto stat =  _buckets->put(name, val);
    if(!stat.ok()) {
        // rollback
        _pa->freePage(id, 1);
        return std::forward_as_tuple(stat, Bucket());
    }
    Bptree::newOnDisk(meta.root, _fm.get(), _meta.page_size);
    return std::forward_as_tuple(stat, Bucket(name, meta, this, cmp));
}

std::tuple<Status, Bucket> 
DB::getBucket(std::string name, comparator_t cmp) {

    BptreeMeta meta;
    std::string val;
    auto stat =  _buckets->get(name, val);
    if(!stat.ok()) {
        return std::forward_as_tuple(stat, Bucket());
    }
    std::memcpy(&meta, val.data(), sizeof(BptreeMeta));
    return std::forward_as_tuple(stat, Bucket(name, meta, this, cmp));
}

void DB::updateRoot(std::string &name, pgid_t newroot, u32 height) {
    if(name == "__BUCKET_TREE__") {
        _meta.bucket_tree_meta.root = newroot;
        _meta.bucket_tree_meta.height = height;
        return;    
    }
    std::string val;
    auto stat = _buckets->get(name, val);
    assert(stat.ok());
    BptreeMeta *meta = (BptreeMeta *)val.data();
    meta->root = newroot;
    meta->height = height;
    stat = _buckets->update(name, val);
    assert(stat.ok());
}

void DB::show() {
    _pa->show();
}

void DB::startWriteThread() {
    //return;
    _th = std::thread([](std::shared_ptr<FileManager> fm,
                      std::shared_ptr<WriteQue_t>  wq){
        for(;;) {
            PagePtr pg;
            wq->waitAndPop(pg);
            if(!pg) {
                DEBUGOUT("write thread exit");
                break;
            }
            pg->write(fm.get());
        }
    }, _fm, _write_que);
}

}// namespace bptdb
