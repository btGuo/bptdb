#include <iostream>
#include <fstream>
#include <cstdlib>
#include <vector>
#include <thread>
#include <chrono>
#include <cassert>
#include <bptdb/DB.h>

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

void put(vector<pair<string, string>> &kvs, bptdb::Bucket bucket) {
    int size = kvs.size();
    for(int i = 0; i < size; i++) {
        assert(bucket.put(kvs[i].first, kvs[i].second).ok());
    }
}

void multi_put(vector<pair<string, string>> &kvs, bptdb::Bucket bucket) {
    int size = kvs.size();

    thread p1([bucket, &kvs, size]()mutable{
        for(int i = 0; i < size / 2; i++) {
            assert(bucket.put(kvs[i].first, kvs[i].second).ok());
        }
    });

    thread p2([bucket, &kvs, size]()mutable{
        for(int i = size / 2; i < size; i++) {
            assert(bucket.put(kvs[i].first, kvs[i].second).ok());
        }
    });

    p1.join();
    p2.join();
}

void del(vector<pair<string, string>> &kvs, bptdb::Bucket bucket) {
    int size = kvs.size();
    for(int i = 0; i < size; i++) {
        assert(bucket.del(kvs[i].first).ok());
    }
}

void multi_del(vector<pair<string, string>> &kvs, bptdb::Bucket bucket) {
    int size = kvs.size();

    thread p1([bucket, &kvs, size]()mutable{
        for(int i = 0; i < size / 2; i++) {
            //std::cout << i << endl;
            assert(bucket.del(kvs[i].first).ok());
        }
    });

    thread p2([bucket, &kvs, size]()mutable{
        for(int i = size / 2; i < size; i++) {
            //std::cout << i << endl;
            assert(bucket.del(kvs[i].first).ok());
        }
    });

    p1.join();
    p2.join();
}

void get(vector<pair<string, string>> &kvs, bptdb::Bucket bucket) {
    int size = kvs.size();
    for(int i = 0; i < size; i++) {
        auto [stat, val] = bucket.get(kvs[i].first);
        assert(stat.ok() && val == kvs[i].second);
    }
}

void iter(vector<pair<string, string>> &kvs, bptdb::Bucket bucket) {
    for(auto it = bucket.begin(); !it->done(); it->next()) {
        it->key();
        it->val();
    }
}

int main(int argc, char **argv) {

    if(argc < 3) {
        cout << "usage: " << argv[0] << " [option] [size]\n";
        cout << "\toption: [1]put, [2]delete, [3]get, [4]getbyiter\n";
        return 0;
    }
    int option = std::atoi(argv[1]);
    int size   = std::atoi(argv[2]);
    bool multi = false;

    // open database and get bucket.
    bptdb::DB db;
    if(auto stat = db.open("my.db", bptdb::DB_CREATE); !stat.ok()) {
        cout << stat.getErrmsg() << endl;
        return 0;
    }
    auto [stat, bucket] = db.getBucket("mybucket");
    if(!stat.ok()) {
        tie(stat, bucket) = db.createBucket("mybucket");
        if(!stat.ok()) {
            cout << stat.getErrmsg() << endl;
            return 0;
        }
    }

    // prepare data.
    auto kvs = getdata("data.txt", size);

    auto start = chrono::system_clock::now();

    if(option == 1) {
        if(multi) multi_put(kvs, bucket);
        else put(kvs, bucket);
    }else if(option == 2) {
        if(multi) multi_del(kvs, bucket);
        else del(kvs, bucket);
    }else if(option == 3) {
        get(kvs, bucket);
    }else if(option == 4) {
        iter(kvs, bucket);
    }else {
        cout << "option error\n";
    }

    auto stop = chrono::system_clock::now();
    cout << "rate: " << 
        chrono::duration_cast<chrono::microseconds>(stop - start).count() / (double)size <<
        " micros/op" << endl;

    return 0;
}
