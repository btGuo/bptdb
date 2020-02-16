#include <leveldb/db.h>
#include <chrono>
#include <iostream>
#include <fstream>
#include <cstdlib>
#include <vector>

using namespace std;

vector<pair<string, string>> getdata(string path, int size) {
    ifstream in(path);
    assert(in.is_open());
    vector<pair<string, string>> vec(size);
    while(size-- && !in.eof()) {
        in >> vec[size].first >> vec[size].second;
    }
    return vec;
}

int main(int argc, char **argv) {

    if(argc < 3) {
        cout << "usage: " << argv[0] << " [option] [size]\n";
        cout << "\toption: [1]put, [2]delete, [3]get, [4]update\n";
        return 0;
    }
    int option = std::atoi(argv[1]);
    int size   = std::atoi(argv[2]);

    leveldb::DB *db;
    leveldb::Options options;
    options.create_if_missing = true;
    if(auto stat = 
       leveldb::DB::Open(options, "./level.db", &db); !stat.ok()) {
        return 0;
    }
    auto kvs = getdata("data.txt", size);

    auto start = chrono::system_clock::now();
    for(int i = 0; i < size; i++) {
        if(option == 1) {
            assert(db->Put(leveldb::WriteOptions(), kvs[i].first, kvs[i].second).ok());
        }else if(option == 3) {
            string val;
            assert(db->Get(leveldb::ReadOptions(), kvs[i].first, &val).ok());
            assert(val == kvs[i].second);
        }else {
            cout << "option error\n";
            break;
        }
    }
    auto stop = chrono::system_clock::now();
    cout << "rate: " << 
        chrono::duration_cast<chrono::microseconds>(stop - start).count() / (double)size <<
        " micros/op" << endl;

    return 0;
}
