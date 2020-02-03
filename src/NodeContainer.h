#ifndef __NODE_CONTAINER_H
#define __NODE_CONTAINER_H

namespace bptdb {

template <typename KType, typename VType, typename Comp>
class NodeContainer {
public:
    virtual void put(KType &key, VType &val) = 0;
    virtual KType splitTo(NodeContainer &other);
    virtual VType get(KType &key) = 0;
    virtual void reset(void *ptr) = 0;
};

}// namespace bptdb
#endif
