#ifndef __INNERNODE_CONTAINER_H
#define __INNERNODE_CONTAINER_H

#include <string>
#include <cstring>
#include <string_view>
#include <type_traits>
#include "common.h"

namespace bptdb {

template <typename KType, typename Comp>
class InnerContainer {
    // pod 通用模板
    static_assert(std::is_pod<KType>::value);
public:
    struct Elem {
        KType key;
        pgid_t blknr;
    };
    
    // =======================================
    class Iterator{};
    // =======================================
    InnerContainer() = default;
    void init(KType &key, pgid_t child1, pgid_t child2) {
    }
    void put(KType &key, pgid_t val) {

    }
    void update(KType &key, pgid_t &val) {
    }
    pgid_t get(KType &key) {
        return 0;
    }
    KType splitTo(InnerContainer &other) {
        return KType();
    }
    void reset(u32 *bytes, 
            u32 *size, char *data) {
        _size    = size;
        _bytes   = bytes;
        _head    = (pgid_t *)data;
        _data    = (char *)(_head + 1);
    }
    bool raw() { return !_data; }
    u32 elemSize(KType &key, pgid_t &val) { return sizeof(Elem); }
private:
    char   *_data{nullptr};
    Comp   _cmp;
    pgid_t *_head{nullptr};
    u32    *_size{nullptr};
    u32    *_bytes{nullptr};
};

}// namespace bptdb

#include "__InnerContainer_1.h"
#endif
