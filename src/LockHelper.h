#ifndef __LOCK_HELPER_H
#define __LOCK_HELPER_H

#include <cstdint>
#include <new>
#include <shared_mutex>
#include <cassert>

namespace bptdb {

template <typename T>
class UnLockGuardArray {
public:
    UnLockGuardArray(u32 cap) {
        _cap = cap;
        _arr = (T *)std::malloc(sizeof(T) * _cap);
    }
    ~UnLockGuardArray() {
        clear();
        if(_arr) std::free(_arr);
    }
    void emplace_back(std::shared_mutex &mtx) {
        assert(_size < _cap);
        new(_arr + _size++)T(mtx);
    }
    void pop_back() {
        assert(_size > 0);
        (_arr + --_size)->~T();
    }
    void clear() {
        for(u32 i = 0; i < _size; i++) {
            (_arr + i)->~T();
        }
        _size = 0;
    }
    u32 size() {
        return _size;
    }
private:
    T *_arr{nullptr};
    u32 _size{0};
    u32 _cap{0};
};

class UnReadLockGuard {
public:
    UnReadLockGuard(std::shared_mutex &mtx): _mtx(mtx){}
    ~UnReadLockGuard() { _mtx.unlock_shared(); }
private:
    std::shared_mutex &_mtx;
};

class UnWriteLockGuard {
public:
    UnWriteLockGuard(std::shared_mutex &mtx): _mtx(mtx) {}
    ~UnWriteLockGuard() { _mtx.unlock(); }
private:
    std::shared_mutex &_mtx;
};

}// namespace bptdb
#endif
