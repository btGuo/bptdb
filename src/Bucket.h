#ifndef __BUCKET_H
#define __BUCKET_H

#include <memory>
#include <string>
#include "Status.h"
#include "common.h"
#include "FileManager.h"
#include "IteratorBase.h"

namespace bptdb{

class DB;
class Bptree;

class Bucket {
public:
    Bucket();
    ~Bucket();
    Bucket(std::string &name, BptreeMeta &meta, DB *db, comparator_t cmp);
    Status get(std::string key, std::string &val);
    Status update(std::string key, std::string val);
    Status put(std::string key, std::string val);
    Status del(std::string key);
    std::shared_ptr<IteratorBase> begin();
    std::shared_ptr<IteratorBase> at(std::string &key);
private:
    std::shared_ptr<Bptree> _impl;
};

}// namespace bptdb

#endif
