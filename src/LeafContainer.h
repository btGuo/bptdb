#ifndef __LEAF_CONTAINER_H
#define __LEAF_CONTAINER_H

#include <string>
#include <string_view>
#include <vector>
#include <cassert>
#include <cstring>
#include <type_traits>
#include <algorithm>
#include <tuple>
#include "common.h"
#include "Option.h"

namespace bptdb {

class LeafContainer {
public:
    struct Elem {
        u32 keylen;
        u32 vallen;
    };
    class Iterator {
        friend class LeafContainer;
    public:
        Iterator() = default;
        Iterator(u32 pos, LeafContainer *con) {
            _pos = pos;
            _con = con;
        }
        void next() {
            _pos++;
        }
        std::string_view key() { return _con->_keys[_pos]; }
        std::string_view val() { return key2val(key());    }
        bool done() {
            return _pos == *(_con->_size);    
        }
    private:
        std::string_view key2val(std::string_view key) {
            auto elem = (Elem *)(key.data() - sizeof(Elem));
            return std::string_view(
                key.data() + elem->keylen, elem->vallen);
        }
        u32           _pos{0};
        LeafContainer *_con{nullptr};
    };

    std::string_view minkey() {
        return _keys[0];
    }
    std::string_view maxkey() {
        return _keys[_keys.size() - 1];
    }

    // ===============================================

    // get key and val at pos
    std::string key(u32 pos) {
        auto key = _keys[pos];
        return std::string(key.data(), key.size());
    }
    std::string val(u32 pos) {
        return key2val(_keys[pos]);
    }
    std::string key2val(std::string_view key) {
        auto elem = (Elem *)(key.data() - sizeof(Elem));
        return std::string(
            key.data() + elem->keylen, elem->vallen);
    }

    // ===============================================
    
    // get iteration
    Iterator begin() {
        return Iterator(0, this);
    }
    Iterator at(std::string &key) {
        auto it = std::lower_bound(
            _keys.begin(), _keys.end(), key, _cmp);
        if((it == _keys.end()) || _cmp(key, *it)) {
            return Iterator();
        }
        return Iterator(it - _keys.begin(), this);
    }
    //================================================
    LeafContainer(comparator_t cmp): _cmp(cmp) {}
    LeafContainer(LeafContainer &other): _cmp(other._cmp) {}
    ~LeafContainer(){ _keys.clear(); }

    void verify() {
        return;
        for(u32 i = 1; i < _keys.size(); i++) {
            assert(_keys[i] > _keys[i - 1]);
        }
    }

    void push_back(std::string &&key, std::string &&val) {
        _put(_end, key, val);
    }
    bool put(std::string &key, std::string &val) {
        auto ret = std::lower_bound(
            _keys.begin(), _keys.end(), key, _cmp);

        if(ret != _keys.end() && !_cmp(key, *ret)) {
            return false;
        }
        char *it = nullptr;
        if(ret == _keys.end()) {
            it = _end;
        }else {
            it = const_cast<char *>(ret->data()) - sizeof(Elem);
        }
        _put(it, key, val);
        return true;
    }
    bool find(std::string &key) {
        //updateVec();
        return std::binary_search(
            _keys.begin(), _keys.end(), key, _cmp);
    }
    bool get(std::string &key, std::string &val) {
        auto ret = std::lower_bound(
            _keys.begin(), _keys.end(), key, _cmp);
        if(ret == _keys.end() || _cmp(key, *ret)) {
            return false;
        }
        val = key2val(*ret);
        return true;
    }

    // del at pos it
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

    bool del(std::string &key) {
        verify();
        assert(*_size == _keys.size());
        auto ret = std::lower_bound(
            _keys.begin(), _keys.end(), key, _cmp);
        if(ret == _keys.end() || _cmp(key, *ret)) {
            return false;
        }
        auto it = const_cast<char *>(ret->data()) - sizeof(Elem);
        _del(it);
        return true;
    }
    bool update(std::string &key, std::string &val) {
        auto ret = std::lower_bound(
            _keys.begin(), _keys.end(), key, _cmp);

        if(ret == _keys.end() || _cmp(key, *ret)) {
            return false;
        }

        auto it = const_cast<char *>(ret->data()) - sizeof(Elem);
        auto elem = (Elem *)it;
        auto next = it + elemSize(elem);
        int delta = (int)val.size() - (int)elem->vallen;
        std::memmove(next + delta, next, _end - next);
        elem->vallen = val.size();
        char *data = (char *)(elem + 1) + elem->keylen;
        std::memcpy(data, val.data(), elem->vallen);
        
        (*_bytes) += delta;
        _end += delta;
        updateVec();
        return true;
    }
    void pop_front() {
        _del(_data);
    }

    std::string splitTo(LeafContainer &other) {
        auto pos = *_size / 2;
        auto str = _keys[pos];
        auto it = str.data() - sizeof(Elem);
        //XXX
        auto ret = std::string(str.data(), str.size());
        u32 rentbytes = (_end - it);
        u32 rentsize = (*_size - pos);

        *other._size += rentsize;
        *other._bytes += rentbytes;
        other._end = other._data + rentbytes;
        std::memcpy(other._data, it, rentbytes);
        other.updateVec();
        
        *_size -= rentsize;
        *_bytes -= rentbytes;
        _end -= rentbytes;
        updateVec();
        return ret;
    }

    // the parameter ignore is for compatibility with InnerContainer 
    std::string borrowFrom(LeafContainer &other, std::string &ignore) {
        (void)ignore;
        // convert string_view to string at once. other._keys[1] will 
        // be unavailable after other.pop_front().
        auto str = other._keys[1];
        auto ret = std::string(str.data(), str.size());
        push_back(other.key(0), other.val(0));
        other.pop_front();
        return ret;
    }

    // the parameter ignore is for compatibility with InnerContainer 
    void mergeFrom(LeafContainer &other, std::string &ignore) {
        (void)ignore;
        *_size += *other._size;
        // we should not use other._bytes which include the 
        // node header bytes
        u32 bytes = other._end - other._data;
        *_bytes += bytes;
        std::memcpy(_end, other._data, bytes);
        _end += bytes;
        updateVec();
    }
    u32 elemSize(std::string &key, std::string &val) { 
        return sizeof(Elem) + key.size() + val.size(); 
    }
    u32 elemSize(std::string &&key, std::string &&val) { 
        return sizeof(Elem) + key.size() + val.size(); 
    }
    void reset(u32 *bytes, 
            u32 *size, char *data) {
        _size = size;
        _bytes = bytes;
        _data = data;
        // notice! we don't set the _end here.
        updateVec();
    }
    u32 size() { return *_size; }
    bool raw() { return !_data; }
private:
    //put key and val at pos it
    void _put(char *it, std::string &key, std::string &val) {
        auto size = elemSize(key, val);
        std::memmove(it + size, it, _end - it);
        auto elem = (Elem *)it;
        elem->keylen = key.size();
        elem->vallen = val.size();
        char *data = (char *)(elem + 1);
        std::memcpy(data, key.data(), elem->keylen);
        std::memcpy(data + elem->keylen, val.data(), elem->vallen);

        // update header
        (*_size)++;
        (*_bytes) += size;
        _end += size;
        updateVec();
    }
    u32 elemSize(Elem *elem) {
        return sizeof(Elem) + elem->keylen + elem->vallen;
    }
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
    comparator_t _cmp;
    // memory
    std::vector<std::string_view> _keys;
    char *_end{nullptr};
    // disk
    u32 *_bytes{nullptr};
    char *_data{nullptr};
    u32 *_size{nullptr};
};

}// namespace bptdb
#endif

