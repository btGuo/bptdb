#include <iostream>
#include "DB.h"
#include "Bptree.h"
#include "PageAllocator.h"
#include "PageCache.h"
#include "FileManager.h"

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
    _fm = std::make_unique<FileManager>(_path);
    // read meta
    _fm->read((char *)&_meta, sizeof(Meta), 0);
    _pc = std::make_unique<PageCache>(_meta.max_buffer_pages);
    _pa = std::make_unique<PageAllocator>(_meta.freelist_id, 
                                          _fm.get(), _meta.page_size);
    _buckets = std::make_unique<Bucket_t>(
        "__BUCKET_TREE__", _meta.bucket_tree_meta, this);
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
    tree_meta->order = 128;

    // create filemanager firstly
    _fm = std::make_unique<FileManager>(_path);

    // write meta
    _fm->write((char *)&_meta, sizeof(_meta), 0);
    // init pageAllocator on disk
    PageAllocator::newOnDisk(_meta.freelist_id, _fm.get(), 
                             _meta.page_size, _meta.freelist_id + 2);

    // create basic utils
    _pc = std::make_unique<PageCache>(_meta.max_buffer_pages);
    _pa = std::make_unique<PageAllocator>(_meta.freelist_id, _fm.get(), 
                                          _meta.page_size);

    auto meta = _meta.bucket_tree_meta;
    // the id of bucket must be 2
    // init bucket on disk
    Bucket_t::newOnDisk(meta.root, _fm.get(), _meta.page_size);
    // create bucket tree
    _buckets = std::make_unique<Bucket_t>(
        "__BUCKET_TREE__", meta, this);
}

Status DB::createBucket(std::string name, BptreeMeta &meta) {
    auto id = _pa->allocPage(1);
    meta.height = 1;
    meta.root = id;
    meta.first = id;
    // TODO set right order
    meta.order = 128;

    auto stat =  _buckets->put(name, meta);
    if(!stat.ok()) {
        // rollback
        _pa->freePage(id, 1);
        return stat;
    }
    Bucket_t::newOnDisk(meta.root, _fm.get(), _meta.page_size);
    return stat;
}

Status DB::getBucket(std::string name, BptreeMeta &meta) {
    BptreeMeta ori_meta;
    auto stat =  _buckets->get(name, ori_meta);
    if(!stat.ok()) {
        return stat;
    }
    if(meta.keytype != ori_meta.keytype ||
            meta.valtype != ori_meta.valtype ||
            meta.cmptype != ori_meta.cmptype) {
        return Status(error::bucketTypeErr);
    }
    meta = ori_meta;
    return stat;
}
void DB::updateRoot(std::string &name, pgid_t newroot, u32 height) {
    if(name == "__BUCKET_TREE__") {
        _meta.bucket_tree_meta.root = newroot;
        _meta.bucket_tree_meta.height = height;
        return;    
    }
    BptreeMeta meta;
    auto stat = _buckets->get(name, meta);
    assert(stat.ok());
    meta.root = newroot;
    meta.height = height;
    stat = _buckets->update(name, meta);
    assert(stat.ok());
}

}// namespace bptdb
