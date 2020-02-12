#ifndef __INNER_CONTAINER_1_H
#define __INNER_CONTAINER_1_H

#include <iostream>
#include <string>
#include <string_view>
#include <vector>
#include <cassert>
#include <cstring>
#include <type_traits>
#include <algorithm>
#include <tuple>
#include "common.h"

namespace bptdb {

class DelEntry;

static inline u32 roundup(u32 size) {
    return size - size / 2;
}

class InnerContainer {
public:
    struct Elem {
        u32 keylen;
        pgid_t val;
    };
    // =======================================
    class Iterator{
        friend class InnerContainer;
    public:
        Iterator() = default;
        Iterator(int pos, InnerContainer *con) {
            _pos = pos;
            _con = con;
        }
        void next() {
            _pos++;
        }
        std::string key() { return _con->key(_pos); }
        pgid_t val() { return _con->val(_pos); }
        bool done() {
            return _pos == *(_con->_size);    
        }
        bool lastElem() {
            return _pos + 1 == *(_con->_size);
        }
    private:
        int           _pos{0};
        InnerContainer *_con{nullptr};
    };

    Iterator begin() {
        return Iterator(0, this);
    }
    pgid_t head() {
        return *_head;
    }

    // =======================================
    //
    std::string key(u32 pos) {
        auto key = _keys[pos];
        return std::string(key.data(), key.size());
    }
    pgid_t val(u32 pos) {
        return key2val(_keys[pos]);
    }
    pgid_t key2val(std::string_view key) {
        auto elem = (Elem *)(key.data() - sizeof(Elem));
        return elem->val;
    }

    // =======================================
    InnerContainer() = default;
    InnerContainer(comparator_t cmp): _cmp(cmp){}
    InnerContainer(InnerContainer &other): _cmp(other._cmp){}

    // init an InnerContainer we must have a key and two child.
    void init(std::string &key, pgid_t child1, pgid_t child2) {
        (*_bytes) += sizeof(pgid_t);
        (*_head) = child1;
        _put(_data, key, child2);
    }

    // put key and val at pos
    void putat(u32 pos, std::string &key, pgid_t val) {
        assert(pos <= *_size);
        if(pos == *_size) {
            push_back(key, val);
            return;
        }
        _put(const_cast<char *>(_keys[pos].data() - sizeof(Elem)), key, val);
    }

    // delete elem at pos.
    void delat(u32 pos) {
        assert(pos < *_size);
        _del(const_cast<char *>(_keys[pos].data() - sizeof(Elem)));
    }

    // update key at pos
    void updateKeyat(u32 pos, std::string &newkey) {
        assert(pos < *_size);
        _updateKey(const_cast<char *>(_keys[pos].data() - sizeof(Elem)), newkey);
    }

    std::tuple<pgid_t, u32> get(std::string &key) {
        // 这里upper_bound
        auto ret = std::upper_bound(
            _keys.begin(), _keys.end(), key, _cmp);
        u32 pos = ret - _keys.begin();
        if(ret == _keys.begin()) 
            return std::make_tuple(*_head, pos);
        ret--;
        Elem *elem = (Elem *)(ret->data() - sizeof(Elem));
        return std::make_tuple(elem->val, pos);
    }
    std::tuple<pgid_t, u32> get(std::string &key, 
                                DelEntry &entry) {

        auto ret = std::upper_bound(
            _keys.begin(), _keys.end(), key, _cmp);
        u32 pos = ret - _keys.begin();

        if(ret == _keys.end()) {
            entry.last = true;
        }else {
            entry.delim = std::string(ret->data(), ret->size());
        }

        if(ret == _keys.begin()) 
            return std::make_tuple(*_head, pos);

        ret--;
        Elem *elem = (Elem *)(ret->data() - sizeof(Elem));
        return std::make_tuple(elem->val, pos);
    }
    std::string splitTo(InnerContainer &other) {
        u32 pos = roundup(*_size);
        auto str = _keys[pos];
        char *it = const_cast<char *>(str.data() - sizeof(Elem));

        auto strprev = _keys[pos - 1];
        auto ret = std::string(strprev.data(), strprev.size());
        Elem *elem = (Elem *)(strprev.data() - sizeof(Elem));

        u32 rentbytes = (_end - it);
        u32 rentsize = (*_size - pos);

        *other._head = elem->val;

        *other._size += rentsize;
        *other._bytes += rentbytes;
        other._end = other._data + rentbytes;
        std::memcpy(other._data, it, rentbytes);
        other.updateVec();

        *_size -= (rentsize + 1);
        *_bytes -= (rentbytes + elemSize(elem));
        _end -= (rentbytes + elemSize(elem));
        updateVec();
        return ret;
    }
    void mergeFrom(InnerContainer &other, std::string &str) {
        push_back(str, *other._head);
        *_size += *other._size;
        u32 bytes = other._end - other._data;
        *_bytes += bytes;
        std::memcpy(_end, other._data, bytes);
        _end += bytes;
        updateVec();
    }
    std::string borrowFrom(InnerContainer &other, std::string delim) {
        auto ret = std::string(other._keys[0]);
        push_back(delim, *other._head);
        *other._head = other.val(0);
        other.pop_front();
        return ret;
    }
    void reset(u32 *bytes, 
            u32 *size, char *ptr) {
        _size  = size;
        _bytes = bytes;
        _head  = (pgid_t *)ptr;
        _data  = (char *)(_head + 1);
        updateVec();
    }
    u32 elemSize(std::string &key, pgid_t val) { 
        return sizeof(Elem) + key.size(); 
    }
    u32 elemSize(std::string &&key, pgid_t val) {
        return sizeof(Elem) + key.size(); 
    }
    bool raw() { return !_data; }
    u32 size() { return *_size; }

private:

    // ===================================================
    // put at pos it.
    void _put(char *it, std::string &key, pgid_t val) {
        u32 size = sizeof(Elem) + key.size();
        std::memmove(it + size, it, _end - it);
        auto elem = (Elem *)it;
        elem->keylen = key.size();
        elem->val = val;
        char *data = (char *)(elem + 1);
        std::memcpy(data, key.data(), elem->keylen);

        (*_size)++;
        (*_bytes) += size;
        _end += size;
        updateVec();
    }

    void push_back(std::string &key, pgid_t val) {
        _put(_end, key, val);
    }

    //===================================================
    //
    void _del(char *it) {
        auto elem = (Elem *)it;
        auto size = elemSize(elem);
        auto next = it + size;
        std::memmove(it, next, _end - next);

        (*_size)--;
        (*_bytes) -= size;
        _end -= size;
        updateVec();
    }

    void pop_front() {
        _del(_data);
    }

    //==================================================

    void _updateKey(char *it, std::string &newkey) {
        auto elem = (Elem *)it;
        auto next = it + elemSize(elem);
        int delta = (int)newkey.size() - (int)elem->keylen;
        std::memmove(next + delta, next, _end - next);
        elem->keylen = newkey.size();
        char *data = (char *)(elem + 1);
        std::memcpy(data, newkey.data(), elem->keylen);
        
        (*_bytes) += delta;
        _end += delta;
        updateVec();
    }

    //==================================================

    void updateVec() {

        _keys.resize(*_size);
        Elem *elem = (Elem *)_data;
        char *data = _data;
        for(u32 i = 0; i < *_size; i++) {
            _keys[i] = std::string_view(data + sizeof(Elem), elem->keylen);
            data += elemSize(elem);
            elem = (Elem *)data;
        }
        _end = data;
    }
    u32 elemSize(Elem *elem) {
        return elem->keylen + sizeof(Elem);
    }

    std::vector<std::string_view> _keys;
    char   *_data{nullptr};
    char   *_end{nullptr};
    comparator_t   _cmp;
    pgid_t *_head{nullptr};
    u32    *_size{nullptr};
    u32    *_bytes{nullptr};
};

}// namespace bptdb
#endif
