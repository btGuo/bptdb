#ifndef __BUCKET_H
#define __BUCKET_H

#include <memory>
#include <string>

#include "Status.h"
#include "IteratorBase.h"

namespace bptdb{

class Bptree;

class Bucket {
public:
    Bucket();
    Bucket(std::shared_ptr<Bptree> impl);
    ~Bucket();
    std::tuple<Status, std::string> get(std::string &key);
    Status update(std::string &key, std::string &val);
    Status put(std::string &key, std::string &val);
    Status del(std::string &key);
    std::shared_ptr<IteratorBase> begin();
    std::shared_ptr<IteratorBase> at(std::string &key);
private:
    std::shared_ptr<Bptree> _impl;
};

}// namespace bptdb

#endif
