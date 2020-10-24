#include <bptdb/Bucket.h>
#include <bptdb/Option.h>
#include <functional>
#include <iostream>
#include <fstream>
#include <cstdlib>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <cassert>
#include <algorithm>
#include <bptdb/DB.h>
#include <unistd.h>
#include <cstdio>

using Op_t = void(std::vector<std::pair<std::string, std::string>>&, bptdb::Bucket, int, int);

std::vector<std::pair<std::string, std::string>> getdata(std::string path, int size) {
    std::ifstream in(path);
    assert(in.is_open());
    std::vector<std::pair<std::string, std::string>> vec(size);
    while(size-- && !in.eof()) {
        in >> vec[size].first >> vec[size].second;
    }
    std::random_shuffle(vec.begin(), vec.end());
    return vec;
}

void multi_op(std::vector<std::pair<std::string, std::string>> &kvs, 
        bptdb::Bucket bucket, int nums, Op_t op) {

    std::vector<std::thread> vec;
    int batch_size = kvs.size() / nums;
    for (int i = 0, j = 0; i < nums; i++, j+=batch_size) {
        vec.emplace_back(op, std::ref(kvs), bucket, j, j+batch_size);
    }
    for (int i = 0; i < nums; i++) {
        vec[i].join();
    }
}

void put(std::vector<std::pair<std::string, std::string>> &kvs, 
        bptdb::Bucket bucket, int begin, int end) {

    for (int k = begin; k < end; k++) {
        auto stat = bucket.put(kvs[k].first, kvs[k].second);
        if (!stat.ok()) {
            std::cout << stat.getErrmsg() << std::endl;
            assert(0);
        }
    }
}

void del(std::vector<std::pair<std::string, std::string>> &kvs, 
        bptdb::Bucket bucket, int begin, int end) {

    for (int k = begin; k < end; k++) {
        auto stat = bucket.del(kvs[k].first);
        if (!stat.ok()) {
            std::cout << stat.getErrmsg() << std::endl;
            assert(0);
        }
    }
}

void get(std::vector<std::pair<std::string, std::string>> &kvs, 
        bptdb::Bucket bucket, int begin, int end) {

    for(int i = begin; i < end; i++) {
        auto [stat, val] = bucket.get(kvs[i].first);
        if (!stat.ok()) {
            std::cout << stat.getErrmsg() << std::endl;
        }
        if (val != kvs[i].second) {
            std::cout << "get key " << kvs[i].first << " error\n";
        }
    }
}

int main(int argc, char **argv) {

    std::string operation;
    int size;
    int thread_nums = 1;
    int gopt;

    auto show_help = [](const char *name) {
        std::fprintf(stderr, "Usage: %s [-c thread_nums] [-n size] [-o get|put|del]\n", name); 
    };
    
    while ((gopt = getopt(argc, argv, "n:c:o:")) != -1) {
        switch (gopt) {
        case 'n':
            size = std::atoi(optarg);
            break;
        case 'c':
            thread_nums = std::atoi(optarg);
            break;
        case 'o':
            operation = std::string(optarg);
            if (operation != "get" &&
                    operation != "put" &&
                    operation != "del") 
            {
                show_help(argv[0]);
                exit(EXIT_FAILURE);
            }
            break;
        default:
            show_help(argv[0]);
            exit(EXIT_FAILURE);
        }
    }

    // open database and get bucket.
    bptdb::Option opt;
    opt.max_buffer_pages = 1024*256;
    bptdb::DB db;
    if(auto stat = db.open("my.db", bptdb::DB_CREATE, opt); !stat.ok()) {
        std::cout << stat.getErrmsg() << std::endl;
        return 0;
    }
    auto [stat, bucket] = db.getBucket("mybucket");
    if(!stat.ok()) {
        std::tie(stat, bucket) = db.createBucket("mybucket");
        if(!stat.ok()) {
            std::cout << stat.getErrmsg() << std::endl;
            return 0;
        }
    }

    // prepare data.
    auto kvs = getdata("data.txt", size);

    auto start = std::chrono::system_clock::now();

    if(operation == "put") {
        multi_op(kvs, bucket, thread_nums, put);
    }else if(operation == "del") {
        multi_op(kvs, bucket, thread_nums, del);
    }else if(operation == "get") {
        multi_op(kvs, bucket, thread_nums, get);
    }else {
        std::cout << "option error\n";
    }

    auto stop = std::chrono::system_clock::now();
    std::cout << "rate: " << 
        std::chrono::duration_cast<std::chrono::microseconds>(stop - start).count() / (double)size <<
        " micros/op" << std::endl;

    return 0;
}
