#ifndef __FILEMANAGER_H
#define __FILEMANAGER_H

#include <string>
#include <iostream>
#include <fstream>
#include <cassert>
#include "common.h"

namespace bptdb {

class FileManager {
public:
    FileManager(std::string path): _file(path, 
        std::ios::binary | std::ios::out | std::ios::in) {

        assert(_file.is_open());
        _path = path;
    }

    ~FileManager() { 
        _file.close(); 
    }
    void read(char *p, u32 cnt, u32 pos) {
        _file.seekg(pos);
        _file.read(p, cnt);
        _file.clear();
    }
    void write(char *p, u32 cnt, u32 pos) {
        _file.seekp(pos);
        _file.write(p, cnt);
        _file.flush();
    }
    // without flush
    void writebuffer(char *p, u32 cnt, u32 pos) {
        _file.seekp(pos);
        _file.write(p, cnt);
    }
    u32 fileSize() {
        _file.seekg(0, std::ios_base::end);
        std::streampos sp = _file.tellg();
        return sp;
    }
    void flush() {
        _file.flush();
    }
private:
    std::string _path;
    std::fstream _file;
};

}// namespace bptdb

#endif
