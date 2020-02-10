#ifndef __BPTDB_H
#define __BPTDB_H

#include <typeinfo>
#include "Status.h"
#include "DB.h"
#include "Bptree.h"
#include "common.h"

namespace bptdb{

// template alias 
template <typename KType, typename VType, 
         typename OrderType = keyOrder::ASCE>
using Bucket = Bptree<KType, VType, OrderType>;

template <typename KType, 
         typename VType, 
         typename OrderType>
static inline Status
createBucket(std::string name, DB *db, 
             std::shared_ptr<Bptree<KType, VType, OrderType>> &ptr) {
    return handlerBucket(name, db, ptr, false);
}

template <typename KType, 
         typename VType, 
         typename OrderType>
static inline Status
getBucket(std::string name, DB *db,
             std::shared_ptr<Bptree<KType, VType, OrderType>> &ptr) {
    return handlerBucket(name, db, ptr, true);
}

template <typename KType, 
         typename VType, 
         typename OrderType>
static inline Status
handlerBucket(std::string name, DB *db,
             std::shared_ptr<Bptree<KType, VType, OrderType>> &ptr, bool type) {
    BptreeMeta meta;
    meta.keytype = typeid(KType).hash_code();
    meta.valtype = typeid(VType).hash_code();
    meta.cmptype = typeid(OrderType).hash_code();
    Status stat;
    if(type) {
        stat = db->getBucket(name, meta);
    }else {
        stat = db->createBucket(name, meta);
    }
    if(!stat.ok()) {
        return stat;
    }
    ptr = std::make_shared<Bptree<KType, VType, OrderType>>(
        name, meta, db);
    return Status();
}

}// namespace bptdb

#endif
