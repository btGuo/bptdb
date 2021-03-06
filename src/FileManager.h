#ifndef __FILEMANAGER_H
#define __FILEMANAGER_H

#include <string>
#include <iostream>
#include <fstream>
#include <mutex>
#include <cassert>
#include "common.h"

namespace bptdb {

class FileManager {
public:
    FileManager(std::string path, bool sync): _file(path, 
        std::ios::binary | std::ios::out | std::ios::in) {

        assert(_file.is_open());
        _path = path;
        _sync = sync;
    }

    ~FileManager() { 
        _file.close(); 
    }
    void read(char *p, u32 cnt, u32 pos) {
        std::lock_guard lg(_mtx);
        _file.seekg(pos);
        _file.read(p, cnt);
        _file.clear();
    }
    void write(char *p, u32 cnt, u32 pos) {
        std::lock_guard lg(_mtx);
        _file.seekp(pos);
        _file.write(p, cnt);
        if(_sync) _file.flush();
    }
    u32 fileSize() {
        std::lock_guard lg(_mtx);
        _file.seekg(0, std::ios_base::end);
        std::streampos sp = _file.tellg();
        return sp;
    }
private:
    std::string  _path;
    std::fstream _file;
    std::mutex   _mtx;
    bool         _sync;
};

extern std::unique_ptr<FileManager> g_fm;

}// namespace bptdb

#endif
