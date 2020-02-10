#ifndef __COMMON_H
#define __COMMON_H


#include <string>
#include <cstdint>
#include <string_view>
#include <shared_mutex>
#include <cstdio>
#include <cassert>
#include <new>

namespace bptdb {

#define DEBUG 1

#ifdef DEBUG
#define DEBUGOUT(...) do{ \
    std::printf("[%s: %d] ", __FILE__, __LINE__); \
    std::printf(__VA_ARGS__); \
    std::printf("\n");\
}while(0)
#else 
#define DEBUGOUT(...) (void(0))
#endif

using u8  = std::uint8_t;
using u32 = std::uint32_t;
using u16 = std::uint16_t;
using u64 = std::uint64_t;
using pgid_t = std::uint32_t;

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
        for(int i = 0; i < _size; i++) {
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

namespace keyOrder {
    struct ASCE {};
    struct DESC {};
}// namespace keyOrder

static inline u32 roundup(u32 size) {
    return size - size / 2;
}

namespace keycompare {

template <typename T>
struct greater {
    bool operator() (const T& lhs, const T& rhs) {
        return lhs > rhs;
    }
};

template <>
struct greater<std::string> {
    bool operator() (std::string_view lhs, std::string_view rhs) {
        return lhs > rhs;
    }
};

template <typename T>
struct less {
    bool operator() (const T& lhs, const T& rhs) {
        return lhs < rhs;
    }
};

template <>
struct less<std::string> {
    bool operator() (std::string_view lhs, std::string_view rhs) {
        return lhs < rhs;
    }
};

}// namespace keycompare

namespace error {
    constexpr const char *keyRepeat = "key already exist";
    constexpr const char *DbOpenFailed = "DataBase open failed";
    constexpr const char *DbCreatFailed = "DataBase create failed";
    constexpr const char *keyNotFind = "Key not find";
    constexpr const char *bucketTypeErr = "bucket keytype or valuetype error";
}// namespace error

}// namespace bptdb
#endif
