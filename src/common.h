#ifndef __COMMON_H
#define __COMMON_H

#include <string>
#include <cstdint>
#include <string_view>
#include <shared_mutex>
#include <cstdio>
#include <functional>

namespace bptdb {

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

namespace error {
    constexpr const char *keyRepeat = "key already exist";
    constexpr const char *DbOpenFailed = "DataBase open failed";
    constexpr const char *DbCreatFailed = "DataBase create failed";
    constexpr const char *keyNotFind = "Key not find";
    constexpr const char *bucketTypeErr = "bucket keytype or valuetype error";
}// namespace error

struct BptreeMeta {
    pgid_t root;
    pgid_t first;
    u32 height;
    u32 order;
};

}// namespace bptdb
#endif
