#include <cassert>
#include "Bucket.h"
#include "Bptree.h"

namespace bptdb {

std::tuple<Status, std::string> Bucket::get(std::string &key) {
    return _impl->get(key);
}

Status Bucket::update(std::string &key, std::string &val) {
    return _impl->update(key, val);
}

Status Bucket::put(std::string &key, std::string &val) {
    return _impl->put(key, val);
}

Status Bucket::del(std::string &key) {
    return _impl->del(key);
}

std::shared_ptr<IteratorBase> Bucket::begin() {
    return _impl->begin();
}

std::shared_ptr<IteratorBase> Bucket::at(std::string &key) {
    return _impl->at(key);
}

Bucket::Bucket(std::shared_ptr<Bptree> impl) {
    _impl = impl;
}

Bucket::~Bucket() = default;
Bucket::Bucket()  = default;

 
}// namespace bptdb
