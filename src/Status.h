#ifndef __STATUS_H
#define __STATUS_H

#include <string>
#include <string_view>

namespace bptdb {
 
class Status {
public:
    Status() = default;
    Status(const char *err): _err(err){ _ok = false;}
    bool ok() { return _ok; }
    std::string_view getErrmsg() { return _err; }
private:
    std::string_view _err;
    bool        _ok{true};
};

}// namespace bptdb

#endif
