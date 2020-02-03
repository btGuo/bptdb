#ifndef __LEAF_CONTAINER_H
#define __LEAF_CONTAINER_H

#include <cstring>
#include <type_traits>
#include <algorithm>
#include "common.h"

namespace bptdb {

enum class IterMoveType {
    Forward, BackWard,
};

template <typename KType, typename VType, typename Comp>
class LeafContainer {

    static_assert(std::is_pod<KType>::value && 
                  std::is_pod<VType>::value);
public:
    struct Elem {
        KType key;
        VType val;
    };
    class Iterator {
        friend class LeafContainer;
    public:
        Iterator(){}
        Iterator(int pos, LeafContainer *con) {
            _pos = pos;
            _con = con;
            _key = _con->key(_pos);
            _val = _con->val(_pos);
            _id  = ids++;
        }
        KType key() { return _key; }
        VType val() { return _val; }
        bool done() {
            return _pos == *(_con->_size);    
        }
        bool valid() {
            return _con->valid;
        }
    private:
        KType _key;
        VType _val;
        LeafContainer *_con{nullptr};
        int _pos{0};
        u32 _id;
        static u32 ids;
    };
    LeafContainer() = default;

    KType key(u32 pos) {
        return KType();
    }
    VType val(u32 pos) {
        return VType();
    }

    //TODO
    // ==============================================
    Iterator begin() {
        return Iterator(0, this);
    }
    Iterator upper(KType &key) {
        return Iterator();
    }
    Iterator lower(KType &key) {
        return Iterator();
    }
    Iterator at(KType &key) {
        return Iterator();
    }
    void updateIter(Iterator &iter, IterMoveType type) {
    }
    // =============================================
    bool find(KType &key) {
        Elem *begin = (Elem *)_data;
        Elem *end = begin + _size;
        return std::binary_search(
            begin, end, key, [this](const Elem &a, const Elem &b){
                return _cmp(a.key, b.key);
            });
    }
    VType get(KType &key) {
        return VType();
    }
    
    void put(KType &key, VType &val) {

    }

    void update(KType &key, VType &val) {

    }

    void del(KType &key) {

    }

    KType splitTo(LeafContainer &other) {
        return KType();
    }

    void reset(u32 *bytes, 
            u32 *size, char *data) {
        _size = size;
        _bytes = bytes;
        _data = data;
    }
    u32 elemSize(KType &key, VType &val) { return sizeof(Elem); }
private:
    char *_data{nullptr};
    Comp _cmp;
    u32 *_size{nullptr};
    u32 *_bytes{nullptr};
};


template <typename KType, typename VType, typename Comp>
u32 LeafContainer<KType, VType, Comp>::Iterator::ids{0};

}// namespace bptdb

#include "__LeafContainer_1.h"

#endif
