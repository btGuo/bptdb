#ifndef __DB_H
#define __DB_H

#include <string>
#include <memory>
#include <string_view>
#include <functional>
#include <algorithm>
#include "Status.h"
#include "PageAllocator.h"
#include "PageCache.h"
#include "FileManager.h"
#include "Bucket.h"
#include "common.h"

namespace bptdb {
 
struct Option {
    u32 page_size{4096};
    u32 max_buffer_pages{8192};
};

class Bptree;

constexpr bool DB_CREATE = true;

class DB {
public:
    struct Meta {
        u32 page_size;
        u32 max_buffer_pages;
        pgid_t freelist_id;
        BptreeMeta bucket_tree_meta;
        u32 checksum;
    };
    Status open(std::string path, bool creat = false, Option option = Option());
    Status create(std::string path, Option option = Option());

    std::tuple<Status, Bucket>
    createBucket(std::string name, comparator_t cmp = std::less<std::string_view>());

    std::tuple<Status, Bucket>
    getBucket(std::string name, comparator_t cmp = std::less<std::string_view>());

    void updateRoot(std::string &name, pgid_t newroot, u32 height);
    void show();

    FileManager *getFileManager() {
        return _fm.get();
    }
    PageAllocator *getPageAllocator() {
        return _pa.get();
    }
    PageCache *getPageCache() {
        return _pc.get();
    }
    u32 getPageSize() {
        return _meta.page_size;
    }
private:
    void init(Option option);
    std::shared_ptr<Bptree>        _buckets;
    std::string                    _path;
    std::unique_ptr<FileManager>   _fm;
    std::unique_ptr<PageAllocator> _pa;
    std::unique_ptr<PageCache>     _pc;
    Meta                           _meta;
};

}// namespace bptdb

#endif
