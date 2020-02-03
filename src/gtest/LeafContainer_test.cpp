#include <gtest/gtest.h>
#include <string>
#include <string_view>
#include <iostream>
#include <algorithm>
#include <vector>
#include <map>
#include "../LeafContainer.h"

using namespace std;

TEST(LeafContainerTest, TestCURD) {
    char data[1024];
    bptdb::u32 bytes = 0;
    bptdb::u32 size = 0;
    bptdb::LeafContainer<string, string, less<string_view>> con;
    con.reset(&bytes, &size, data);
    string key("key");
    string val("val");
    string val2("val2");

    con.elemSize(key, val);
    con.put(key, val);
    ASSERT_EQ(con.get(key), val);
    ASSERT_EQ(size, 1);

    con.update(key, val2);
    ASSERT_EQ(con.get(key), val2);

    con.del(key);
    ASSERT_EQ(con.find(key), false);
    ASSERT_EQ(size, 0);
    ASSERT_EQ(bytes, 0);
}

vector<pair<string, string>> vec {
    {"cpp", "13"},
    {"java", "213"},
    {"c#", "24"},
    {"aaa", "3"},
    {"go", "20"},
    {"rust", "99"},
    {"ruby", "88"},
    {"javascript", "952"},
    {"vb", "898"},
};
map<string, string> m {
    {"cpp", "13"},
    {"c#", "24"},
    {"aaa", "3"},
    {"go", "20"},

    {"java", "213"},
    {"javascript", "952"},
    {"ruby", "88"},
    {"rust", "99"},
    {"vb", "898"},
};

TEST(LeafContainerTest, TestDel) {
    char data[1024];
    bptdb::u32 bytes = 0;
    bptdb::u32 size = 0;
    bptdb::LeafContainer<string, string, less<string_view>> con;
    con.reset(&bytes, &size, data);

    for(auto elem: vec) {
        con.put(elem.first, elem.second);
    }
    for(auto elem: vec) {
        con.del(elem.first);
    }
    ASSERT_EQ(size, 0);
    ASSERT_EQ(bytes, 0);
}

TEST(LeafContainerTest, TestIter) {
    char data[1024];
    bptdb::u32 bytes = 0;
    bptdb::u32 size = 0;
    bptdb::LeafContainer<string, string, less<string_view>> con;
    con.reset(&bytes, &size, data);

    for(auto elem: vec) {
        con.put(elem.first, elem.second);
    }
    ASSERT_EQ(con.size(), vec.size());
    ASSERT_EQ(con.find(vec[0].first), true);
    auto mit = m.begin();
    for(auto it = con.begin(); !it.done(); it.next()) {
        ASSERT_EQ(mit++->first, it.key());
    }
    ASSERT_EQ(mit, m.end());
}

TEST(LeafContainerTest, TestSplit) {
    char data[1024];
    bptdb::u32 bytes = 0;
    bptdb::u32 size = 0;
    bptdb::LeafContainer<string, string, less<string_view>> con;
    con.reset(&bytes, &size, data);

    for(auto elem: vec) {
        con.put(elem.first, elem.second);
    }
    {
        char data[1024];
        bptdb::u32 bytes = 0;
        bptdb::u32 size = 0;
        bptdb::LeafContainer<string, string, less<string_view>> other;
        other.reset(&bytes, &size, data);
        con.splitTo(other);
        auto mit = m.begin();
        ASSERT_EQ(other.size() + con.size(), vec.size());
        for(auto it = con.begin(); !it.done(); it.next()) {
            ASSERT_EQ(mit++->first, it.key());
        }
        for(auto it = other.begin(); !it.done(); it.next()) {
            ASSERT_EQ(mit++->first, it.key());
        }
    }
}

TEST(LeafContainerTest, TestMerge) {
    char data[1024];
    bptdb::u32 bytes = 0;
    bptdb::u32 size = 0;
    bptdb::LeafContainer<string, string, less<string_view>> con;
    con.reset(&bytes, &size, data);
    string ignore;

    auto it = m.begin();
    {
        int size = m.size() / 2;
        while(size--) {
            con.put((string &)it->first, (string &)it->second);
            it++;
        }
    }
    {
        char data[1024];
        bptdb::u32 bytes = 0;
        bptdb::u32 size = 0;
        bptdb::LeafContainer<string, string, less<string_view>> other;
        other.reset(&bytes, &size, data);

        while(it != m.end()) {
            other.put((string &)it->first, (string &)it->second);
            it++;
        }
        ASSERT_EQ(other.size() + con.size(), vec.size());
        con.mergeFrom(other, ignore);
        auto mit = m.begin();
        for(auto it = con.begin(); !it.done(); it.next()) {
            ASSERT_EQ(mit++->first, it.key());
        }
    }
}

TEST(LeafContainerTest, TestBorrow) {
    string ignore;
    char data[1024];
    bptdb::u32 bytes = 0;
    bptdb::u32 size = 0;
    bptdb::LeafContainer<string, string, less<string_view>> con;
    con.reset(&bytes, &size, data);

    auto it = m.begin();
    {
        int size = m.size() / 2;
        while(size--) {
            con.put((string &)it->first, (string &)it->second);
            it++;
        }
    }
    {
        char data[1024];
        bptdb::u32 bytes = 0;
        bptdb::u32 size = 0;
        bptdb::LeafContainer<string, string, less<string_view>> other;
        other.reset(&bytes, &size, data);

        while(it != m.end()) {
            other.put((string &)it->first, (string &)it->second);
            it++;
        }

        auto sz1 = con.size();
        auto sz2 = other.size();
        ASSERT_EQ(other.size() + con.size(), vec.size());
        con.borrowFrom(other, ignore);
        ASSERT_EQ(con.size() - sz1, 1);
        ASSERT_EQ(sz2 - other.size(), 1);
        auto mit = m.begin();
        for(auto it = con.begin(); !it.done(); it.next()) {
            ASSERT_EQ(mit++->first, it.key());
        }
        for(auto it = other.begin(); !it.done(); it.next()) {
            ASSERT_EQ(mit++->first, it.key());
        }
        mit = m.begin();
        {
            int size = m.size() / 2;
            while(size--) {
                con.del((string &)mit++->first);
            }
        }
    }
}
