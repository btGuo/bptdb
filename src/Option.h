#ifndef __OPTION_H
#define __OPTION_H

#include <functional>
#include <string_view>
#include <cstdint>

namespace bptdb {

using comparator_t = std::function<bool(std::string_view, std::string_view)>;

struct Option {
    std::uint32_t page_size{4096};
    std::uint32_t max_buffer_pages{8192};
    bool sync{false};
};

extern Option g_option;

}// namespace bptdb


#endif
