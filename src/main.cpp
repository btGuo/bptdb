#include <iostream>
#include <fstream>
#include <cstdlib>
#include <vector>
#include <thread>
#include <chrono>
#include "DB.h"

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
        cout << i << endl;
        assert(bucket.del(kvs[i].first).ok());
    }
}

void get(vector<pair<string, string>> &kvs, bptdb::Bucket bucket) {
    int size = kvs.size();
    string val;
    for(int i = 0; i < size; i++) {
        auto stat = bucket.get(kvs[i].first, val);
        assert(stat.ok() && val == kvs[i].second);
    }
}

void iter(vector<pair<string, string>> &kvs, bptdb::Bucket bucket) {
    sort(kvs.begin(), kvs.end(), 
         [](const auto &p1, const auto &p2) {
             return p1.first < p2.first;
         });
    int i = 0;
    for(auto it = bucket.begin(); !it->done(); it->next()) {
        cout << i << endl;
        assert(it->key() == kvs[i].first && 
               it->val() == kvs[i].second);
        i++;
    }
}

int main(int argc, char **argv) {

    if(argc < 4) {
        cout << "usage: " << argv[0] << " [option] [size] [multithread](y/n)\n";
        cout << "\toption: [1]put, [2]delete, [3]get\n";
        return 0;
    }
    int option = std::atoi(argv[1]);
    int size   = std::atoi(argv[2]);
    bool multi = argv[3][0] == 'y' ? true : false;

    chrono::system_clock::time_point start, stop;

    {
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

        start = chrono::system_clock::now();

        if(option == 1) {
            if(multi) multi_put(kvs, bucket);
            else put(kvs, bucket);
        }else if(option == 2) {
            del(kvs, bucket);
        }else if(option == 3) {
            get(kvs, bucket);
        }else {
            cout << "option error\n";
        }
    }

    stop = chrono::system_clock::now();
    cout << "rate: " << 
        chrono::duration_cast<chrono::microseconds>(stop - start).count() / size <<
        " micros/op" << endl;

    return 0;
}
