#ifndef __INNER_CONTAINER_1_H
#define __INNER_CONTAINER_1_H

#include <ios>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <vector>
#include <cassert>
#include <cstring>
#include <type_traits>
#include <algorithm>
#include <tuple>
#include "common.h"
#include "Node.h"
#include "Option.h"
#include "PageHeader.h"
#include "PageHelper.h"

namespace bptdb {


static inline u32 roundup(u32 size) {
    return size - size / 2;
}

class InnerNodeImpl {
public:
    struct Elem {
        u32 keylen;
        pgid_t val;
    };

    pgid_t head() {
        return *_head;
    }

    class Iterator {
        friend class InnerNodeImpl;
    public:
        Iterator() = default;
        Iterator(u32 pos, InnerNodeImpl *con) {
            _pos = pos;
            _con = con;
        }
        void next() {
            _pos++;
        }
        std::string_view key() { return _con->_keys[_pos]; }
        pgid_t val() { return _con->key2val(key());  }
        bool done() {
            return _pos == *(_con->_size);    
        }
    private:
        u32           _pos{0};
        InnerNodeImpl *_con{nullptr};
    };

    Iterator begin() {
        return Iterator(0, this);
    }

    std::string_view minkey() {
        return _keys[0];
    }
    std::string_view maxkey() {
        return _keys[_keys.size() - 1];
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

    void verify() {
        // for(u32 i = 1; i < _keys.size(); i++) {
        //     assert(_keys[i] > _keys[i - 1]);
        // }
    }

    // =======================================
    InnerNodeImpl(pgid_t id, comparator_t cmp) {
        _pg = std::make_shared<PageHelper>(id);
        _cmp = cmp;
        _pg->read();
        reset();
    }

    void reset() {
        _hdr = (PageHeader *)_pg->data();
        _size  = &_hdr->size;
        _bytes = &_hdr->bytes;
        _head  = (pgid_t *)(_hdr + 1);
        _data  = (char *)(_head + 1);
        updateVec();
    }
    // =================================================

    void handleOverFlow(u32 extbytes) {
        if (_pg->overFlow(extbytes)) {
            _pg->extend(extbytes);
            reset();
        } 
    }

    // init an InnerNodeImpl we must have a key and two child.
    void init(std::string &key, pgid_t child1, pgid_t child2) {
        handleOverFlow(elemSize(key, child1) + sizeof(child2));
        (*_bytes) += sizeof(pgid_t);
        (*_head) = child1;
        _put(_data, key, child2);
    }

    // put key and val at pos
    void putat(u32 pos, std::string &key, pgid_t val) {
        verify();
        handleOverFlow(elemSize(key, val));
        assert(pos <= *_size);
        if(pos == *_size) {
            push_back(key, val);
            return;
        }
        _put(const_cast<char *>(_keys[pos].data() - sizeof(Elem)), key, val);
    }

    // delete elem at pos.
    void delat(u32 pos) {
        verify();
        assert(pos < *_size);
        _del(const_cast<char *>(_keys[pos].data() - sizeof(Elem)));
    }

    // update key at pos
    void updateKeyat(u32 pos, std::string &newkey) {
        verify();
        handleOverFlow(newkey.size());
        assert(pos < *_size);
        _updateKey(const_cast<char *>(_keys[pos].data() - sizeof(Elem)), newkey);
    }

    std::tuple<pgid_t, u32> get(std::string &key) {
        verify();
        /* 
        for(u32 i = 1; i < _keys.size(); i++) {
            assert(_keys[i] > _keys[i - 1]);
        }
        */
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

    std::tuple<pgid_t, u32> get(
            std::string &key, DelEntry &entry) {

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

    std::string splitTo(InnerNodeImpl &other) {
        u32 pos = roundup(*_size);
        auto str = _keys[pos];
        char *it = const_cast<char *>(str.data() - sizeof(Elem));

        auto strprev = _keys[pos - 1];
        auto ret = std::string(strprev.data(), strprev.size());
        Elem *elem = (Elem *)(strprev.data() - sizeof(Elem));

        u32 rentbytes = (_end - it);
        u32 rentsize = (*_size - pos);

        other.handleOverFlow(rentbytes + sizeof(pgid_t));

        *other._head = elem->val;
        *other._size += rentsize;
        // add head size !!!
        *other._bytes += (rentbytes + sizeof(pgid_t));
        other._end = other._data + rentbytes;
        std::memcpy(other._data, it, rentbytes);
        other.updateVec();

        *_size -= (rentsize + 1);
        *_bytes -= (rentbytes + elemSize(elem));
        _end -= (rentbytes + elemSize(elem));
        updateVec();
        return ret;
    }

    void mergeFrom(InnerNodeImpl &other, std::string &str) {
        push_back(str, *other._head);
        u32 bytes = other._end - other._data;
        handleOverFlow(bytes);
        *_size += *other._size;
        *_bytes += bytes;
        std::memcpy(_end, other._data, bytes);
        _end += bytes;
        updateVec();
    }

    std::string borrowFrom(InnerNodeImpl &other, std::string delim) {
        auto ret = std::string(other._keys[0]);
        //assert(ret >= delim);
        push_back(delim, *other._head);
        *other._head = other.val(0);
        other.pop_front();
        return ret;
    }

    u32 elemSize(std::string &key, pgid_t val) { 
        (void)val;
        return sizeof(Elem) + key.size(); 
    }
    bool raw() { return !_data; }
    u32 size() { return *_size; }
    void write(){ _pg->write(); }
    u32 next(){ return _hdr->next;}
    void setNext(u32 next) { _hdr->next = next; }
    void free() { _pg->free(); }

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
        handleOverFlow(elemSize(key, val));
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

    comparator_t   _cmp;
    std::vector<std::string_view> _keys;
    char   *_data{nullptr};
    char   *_end{nullptr};
    pgid_t *_head{nullptr};
    u32    *_size{nullptr};
    u32    *_bytes{nullptr};
    PageHeader *_hdr{nullptr};
    PageHelperPtr _pg;
};

}// namespace bptdb
#endif
