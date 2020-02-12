#include <cassert>
#include "Bucket.h"
#include "Bptree.h"

namespace bptdb {

Status Bucket::get(std::string key, std::string &val) {
    return _impl->get(key, val);
}

Status Bucket::update(std::string key, std::string val) {
    return _impl->update(key, val);
}

Status Bucket::put(std::string key, std::string val) {
    return _impl->put(key, val);
}

Status Bucket::del(std::string key) {
    return _impl->del(key);
}

Bucket::Bucket(std::string &name, BptreeMeta &meta, DB *db, comparator_t cmp) {
    _impl = std::make_shared<Bptree>(name, meta, db, cmp);
}

std::shared_ptr<IteratorBase> Bucket::begin() {
    return _impl->begin();
}

std::shared_ptr<IteratorBase> Bucket::at(std::string &key) {
    return _impl->at(key);
}

Bucket::~Bucket() = default;
Bucket::Bucket()  = default;

 
}// namespace bptdb
