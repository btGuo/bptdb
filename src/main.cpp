#include <iostream>
#include <cstdlib>
#include <vector>
#include "FileManager.h"
#include "bptdb.h"

using namespace std;

#define handler_error(stat) do{ cout << stat.getErrmsg() << endl; return 0;}while(0)

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

    bptdb::DB db;
    auto stat = db.open("my.db", bptdb::DB_CREATE);
    if(!stat.ok()) {
        cout << stat.getErrmsg() << endl;
        return 0;
    }
    std::shared_ptr<bptdb::Bucket<string, string>> bucket;
    stat = bptdb::getBucket("mybucket", &db, bucket);
    if(!stat.ok()) {
        stat = bptdb::createBucket("mybucket", &db, bucket);
        if(!stat.ok()) {
            handler_error(stat);
        }
    }

    auto kvs = getdata("data.txt", size);

    while(size--) {

        if(option == 1) {
            stat = bucket->put(kvs[size].first, kvs[size].second);
            assert(stat.ok());
        }else if(option == 2) {
            stat = bucket->del(kvs[size].first);
            if(!stat.ok()) {
                cout << stat.getErrmsg() << endl;
            }
            //assert(stat.ok());
        }else if(option == 3) {
            string val;
            stat = bucket->get(kvs[size].first, val);
            assert(stat.ok() && val == kvs[size].second);
        }else {
            cout << "option error\n";
            break;
        }
    }
}
