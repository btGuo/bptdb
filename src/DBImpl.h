#ifndef __DB_IMPL_H
#define __DB_IMPL_H

#include <string>
#include <memory>
#include <string_view>
#include <thread>
#include <functional>

#include "DB.h"
#include "Status.h"
#include "Option.h"
#include "PageAllocator.h"
#include "PageCache.h"
#include "FileManager.h"
#include "Bucket.h"
#include "common.h"

namespace bptdb {
 
class Bptree;

class DBImpl {
public:
    struct Meta {
        u32 page_size;
        u32 max_buffer_pages;
        pgid_t freelist_id;
        BptreeMeta bucket_tree_meta;
        u32 checksum;
    };
    ~DBImpl() {
        show();
    }
    Status open(std::string path, bool creat, Option option);
    Status create(std::string path, Option option);

    std::tuple<Status, Bucket>
    createBucket(std::string name, comparator_t cmp);

    std::tuple<Status, Bucket>
    getBucket(std::string name, comparator_t cmp);

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
    void startWriteThread();

    std::shared_ptr<Bptree>        _buckets;
    std::shared_ptr<FileManager>   _fm;
    std::unique_ptr<PageAllocator> _pa;
    std::unique_ptr<PageCache>     _pc;
    std::string                    _path;
    Meta                           _meta;
};

}// namespace bptdb

#endif
