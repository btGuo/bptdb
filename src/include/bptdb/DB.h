#ifndef __DB_H
#define __DB_H

#include <string>
#include <memory>
#include <algorithm>

#include "bptdb/Option.h"
#include "bptdb/Status.h"
#include "bptdb/Bucket.h"

namespace bptdb {

class DBImpl;

constexpr bool DB_CREATE = true;

class DB {
public:
    DB();
    Status open(std::string path, bool creat = false, Option option = Option());

    Status create(std::string path, Option option = Option());

    std::tuple<Status, Bucket>
    createBucket(std::string name, comparator_t cmp = std::less<std::string_view>());

    std::tuple<Status, Bucket>
    getBucket(std::string name, comparator_t cmp = std::less<std::string_view>());
private:
    std::shared_ptr<DBImpl> _impl;
};

}
#endif
