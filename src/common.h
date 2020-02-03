#ifndef __COMMON_H
#define __COMMON_H


#include <string>
#include <cstdint>
#include <string_view>


namespace bptdb {

using u8  = std::uint8_t;
using u32 = std::uint32_t;
using u16 = std::uint16_t;
using u64 = std::uint64_t;
using pgid_t = std::uint32_t;

namespace keyOrder {
    struct ASCE {};
    struct DESC {};
}// namespace keyOrder

static inline u32 roundup(u32 size) {
    return size - size / 2;
}

namespace typeCoding {

template<typename T>
constexpr inline u8 code(T *p = nullptr) { return -1; }

template<>
constexpr inline u8 code(int *p) { return 1; }

template<>
constexpr inline u8 code(short *p) { return 2; }

template<>
constexpr inline u8 code(double *p) { return 3; }

template<>
constexpr inline u8 code(float *p) { return 4; }

template<>
constexpr inline u8 code(std::string *p) { return 5; }

template<>
constexpr inline u8 code(keyOrder::ASCE *p) { return 0; }

template<>
constexpr inline u8 code(keyOrder::DESC *p) { return 1; }

}// namespace typeCoding

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
