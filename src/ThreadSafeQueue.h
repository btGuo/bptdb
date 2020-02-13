#ifndef __MUTEX_QUEUE_H
#define __MUTEX_QUEUE_H

#include <list>
#include <mutex>
#include <condition_variable>
#include <type_traits>

namespace bptdb {
/**
 * 线程安全队列
 * @tparam T 元素类型
 * @tparam ContainerType 底层容器类型，默认为stl的list
 * @tparam LockType 锁类型，默认为mutex
 */
template <typename T, 
         typename ContainerType = std::list<T>, 
         typename LockType = std::mutex>
class ThreadSafeQueue {
public:
    template <typename U, 
             std::enable_if_t<std::is_same<T, std::remove_reference_t<U> >::value, int> = 0>
    void push(U &&elem) {
        std::lock_guard<LockType> lg(_lock);
        _queue.push_back(std::forward<U>(elem));
        _cv.notify_one();
    }
    void waitAndPop(T &elem) {
        std::unique_lock<LockType> lg(_lock);
        _cv.wait(lg, [this]{ return !_queue.empty(); });
        elem = std::move(_queue.front());
        _queue.pop_front();
    }
    bool pop(T &elem) {
        std::lock_guard<LockType> lg(_lock);
        if(_queue.empty())
            return false;
        elem = std::move(_queue.front());
        _queue.pop_front();
        return true;
    }
    std::size_t size() {
        std::lock_guard<LockType> lg(_lock);
        return _queue.size();
    }
private:
    ContainerType           _queue;
    LockType                _lock;
    std::condition_variable _cv;
};

}// namespace bptdb

#endif
