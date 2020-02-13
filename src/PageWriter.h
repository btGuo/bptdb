#ifndef __PAGE_WRITER_H
#define __PAGE_WRITER_H

#include <thread>
#include "Page.h"
#include "FileManager.h"
#include "ThreadSafeQueue.h"

namespace bptdb {

class PageWriter {
public:
    PageWriter(FileManager *fm): _fm(fm) {}
    ~PageWriter() {
        _write_que.push(PagePtr());
    }
    void write(PagePtr pg) {
        _write_que.push(pg);
    }
    void start() {
        std::thread th([this]{
            for(;;) {
                PagePtr pg;
                _write_que.waitAndPop(pg);
                if(!pg) break;
                pg->write(_fm);
            }
        });
        th.detach();
    }
private:
    FileManager              *_fm{nullptr};
    ThreadSafeQueue<PagePtr> _write_que;
};

}// namespace bptdb

#endif
