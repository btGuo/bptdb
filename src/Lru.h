#ifndef __LRU_H
#define __LRU_H

#include <cstdlib>
#include <unordered_map>
#include <atomic>
#include <cassert>
#include <mutex>
#include <map>
#include <memory>

#include "List.h"
#include "common.h"

namespace bptdb {
template <typename KType, typename VType>
class Lru {
public:
    Lru(u32 max_counts): _lru(VType::lru_tag()), 
    _max_counts(max_counts){}
    using ValuePtr = std::shared_ptr<VType>;

    template <typename ... Args>
    ValuePtr alloc(Args && ... args) {
        auto val = std::make_shared<VType>(std::forward<Args>(args)...);
        std::lock_guard lg(_mtx);
        _count++;
        if(_count > _max_counts) {
            auto val = _lru.pop_back();    
            _cache.erase(val->key());
            --_count;
        }
        assert(_cache.find(val->key()) == _cache.end());
        _cache.insert({val->key(), val});
        _lru.push_front(val.get());
        return val;
    }

    ValuePtr get(KType key) {
        std::lock_guard lg(_mtx);
        auto it = _cache.find(key);
        if(it == _cache.end())
            return ValuePtr();
        auto  val = it->second;
        _lru.erase(val.get());
        _lru.push_front(val.get());
        return val;
    }

    void del(KType key) {
        std::lock_guard lg(_mtx);
        _count--;
        auto it = _cache.find(key);
        assert(it != _cache.end());
        auto val = it->second;
        _lru.erase(val.get());
        _cache.erase(key);
    }
    
private:
    u32 _max_counts{0};
    u32 _count{0};
    std::unordered_map<KType, ValuePtr> _cache;
    List<VType> _lru;
    std::mutex _mtx;
};

}// namespace bptdb

#endif
