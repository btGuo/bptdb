#include <iostream>
#include <string_view>
#include <algorithm>
#include <cstdlib>
#include <thread>
#include <cstring>
#include "DB.h"
#include "DBImpl.h"
#include "PageAllocator.h"
#include "PageCache.h"
#include "FileManager.h"
#include "Bucket.h"
#include "common.h"
#include "Bptree.h"

namespace bptdb {

DB::DB() {
    _impl = std::make_shared<DBImpl>();
}

Status DB::open(std::string path, bool creat, Option option) {
    return _impl->open(path, creat, option);
}

Status DB::create(std::string path, Option option) {
    return _impl->create(path, option);
}

std::tuple<Status, Bucket>
DB::createBucket(std::string name, comparator_t cmp) {
    return _impl->createBucket(name, cmp);
}

std::tuple<Status, Bucket>
DB::getBucket(std::string name, comparator_t cmp) {
    return _impl->getBucket(name, cmp);
}

Status DBImpl::open(std::string path, bool creat, Option option) {
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
    // read meta
    _fm->read((char *)&_meta, sizeof(Meta), 0);
    _pc = std::make_unique<PageCache>(_meta.max_buffer_pages);
    _pa = std::make_unique<PageAllocator>(_meta.freelist_id, 
                                          _fm.get(), _meta.page_size);
    _buckets = std::make_shared<Bptree>(
        "__BUCKET_TREE__", _meta.bucket_tree_meta, this, std::less<std::string_view>());

    return Status();
}
Status DBImpl::create(std::string path, Option option) {

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

void DBImpl::init(Option option) {
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
    Bptree::newOnDisk(meta.root, _fm.get(), _meta.page_size);
    // create bucket tree
    _buckets = std::make_shared<Bptree>(
        "__BUCKET_TREE__", meta, this, std::less<std::string_view>());
}

std::tuple<Status, Bucket> 
DBImpl::createBucket(std::string name, comparator_t cmp) {

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
    return std::forward_as_tuple(
        stat, Bucket(std::make_shared<Bptree>(name, meta, this, cmp)));
}

std::tuple<Status, Bucket> 
DBImpl::getBucket(std::string name, comparator_t cmp) {

    BptreeMeta meta;
    auto [stat, val] =  _buckets->get(name);
    if(!stat.ok()) {
        return std::forward_as_tuple(stat, Bucket());
    }
    std::memcpy(&meta, val.data(), sizeof(BptreeMeta));
    return std::forward_as_tuple(
        stat, Bucket(std::make_shared<Bptree>(name, meta, this, cmp)));
}

void DBImpl::updateRoot(std::string &name, pgid_t newroot, u32 height) {
    if(name == "__BUCKET_TREE__") {
        _meta.bucket_tree_meta.root = newroot;
        _meta.bucket_tree_meta.height = height;
        return;    
    }
    auto [stat, val] = _buckets->get(name);
    assert(stat.ok());
    BptreeMeta *meta = (BptreeMeta *)val.data();
    meta->root = newroot;
    meta->height = height;
    stat = _buckets->update(name, val);
    assert(stat.ok());
}

void DBImpl::show() {
    _pa->show();
}

}// namespace bptdb
