#ifndef __DB_H
#define __DB_H

#include <string>
#include <memory>
#include <string_view>
#include <functional>
#include "Status.h"
#include "common.h"

namespace bptdb {
 
struct Option {
    u32 page_size{4096};
    u32 max_buffer_pages{8192};
};

template <typename KType, typename VType, typename OrderType>
class Bptree;

class FileManager;
class PageCache;
class PageAllocator;

constexpr bool DB_CREATE = true;

struct BptreeMeta {
    pgid_t root;
    pgid_t first;
    u32 height;
    u32 order;
    u64 keytype;
    u64 valtype;
    u64 cmptype;
};

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

    Status createBucket(std::string name, BptreeMeta &meta);
    Status getBucket(std::string name, BptreeMeta &meta);

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
    using Bucket_t = Bptree<std::string, BptreeMeta, keyOrder::ASCE>;
    void init(Option option);
    std::unique_ptr<Bucket_t> _buckets;
    std::string                    _path;
    std::unique_ptr<FileManager>   _fm;
    std::unique_ptr<PageAllocator> _pa;
    std::unique_ptr<PageCache>     _pc;
    Meta                           _meta;
};

}// namespace bptdb

#endif
