#ifndef __ITERATOR_BASE_H
#define __ITERATOR_BASE_H

#include <string_view>

namespace bptdb {

class IteratorBase {
public:    
    virtual std::string_view key() = 0;
    virtual std::string_view val() = 0;
    virtual bool done() = 0;
    virtual void next() = 0;
};

}// namespace bptdb

#endif
