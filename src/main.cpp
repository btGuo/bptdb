#include <iostream>
#include <cstdlib>
#include <vector>
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

    //int size = 1024;
    auto kvs = getdata("data.txt", size);

    //for(int i = 0; i < size; i++) {
    //    assert(bucket->put(kvs[i].first, kvs[i].second).ok());
    //}

    //sort(kvs.begin(), kvs.end(), 
    //     [](const auto &p1, const auto &p2) {
    //         return p1.first < p2.first;
    //     });

    //int i = 0;
    //for(auto it = bucket->begin(); 
    //    it.valid() && !it.done(); it.next()) {
    //    assert(it.key() == kvs[i].first && 
    //           it.val() == kvs[i].second);
    //    i++;
    //}

    while(size--) {
        //cout << size << "\n";
        if(option == 1) {
            stat = bucket->put(kvs[size].first, kvs[size].second);
            assert(stat.ok());
        }else if(option == 2) {
            stat = bucket->del(kvs[size].first);
            assert(stat.ok());
        }else if(option == 3) {
            string val;
            stat = bucket->get(kvs[size].first, val);
            assert(stat.ok() && val == kvs[size].second);
        }else {
            cout << "option error\n";
            break;
        }
    }

    for(auto it = bucket->begin(); !it.done(); it.next()) {
    }
    //db.show();
}
