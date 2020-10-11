#ifndef __THREADPOOL_H
#define __THREADPOOL_H

#include <thread>
#include <vector>
#include <mutex>
#include <queue>
#include <atomic>
#include <condition_variable>
#include <functional>

class ThreadPool {
public:
    ThreadPool(int nums = std::thread::hardware_concurrency()) {
        for (int i = 0; i < nums; i++) {
            _workers.emplace_back([this]{
                while(!this->_stop.load()) {
                    std::unique_lock<std::mutex> lg(this->_mtx);
                    this->_cv.wait(lg, [this] {
                        return this->_stop.load() || !this->_jobs.empty();
                    });
                    if (this->_jobs.empty()) {
                        continue;
                    }
                    auto task = std::move(this->_jobs.front());
                    this->_jobs.pop();
                    task();
                }
            });
        }
    }
    template<typename Func, typename... Args>
    void commit(Func && f, Args &&... args) {
        std::unique_lock<std::mutex> lg(_mtx);
        _jobs.push([&]{
            f(args...);
        });
        _cv.notify_one();
    }
    void stop() {
        _stop.store(true);
        _cv.notify_all();
    }
private:
    using task_t = std::function<void()>;
    std::vector<std::thread> _workers;
    std::queue<task_t> _jobs;
    std::mutex _mtx;
    std::condition_variable _cv;
    std::atomic_bool _stop{false};
};

#endif
