#include <string>
#include <fstream>
#include <cassert>
#include <iostream>
#include <cstdlib>
#include <random>
using namespace std;

void init_dict(vector<char> &dict) {
    for(int i = 0; i < 10; i++) 
        dict.push_back('0' + i);
    for(int i = 0; i < 26; i++)
        dict.push_back('a' + i);
    for(int i = 0; i < 26; i++)
        dict.push_back('A' + i);
}

string randstr(int minsz, int maxsz, vector<char> &dict) {
    random_device rd;
    uniform_int_distribution<int> dist(minsz, maxsz);
    int size = dist(rd);
    string ret;
    {
        uniform_int_distribution<int> dist(0, dict.size());
        for(int i = 0; i < size; i++) {
            ret.push_back(dict[dist(rd)]);    
        }
    }
    return ret;
}

void general_data(ofstream &out, int size, vector<char> &dict) {
    assert(out.is_open());
    for(int i = 0; i < size; i++) {
        string key = randstr(10, 40, dict);
        string val = randstr(50, 100, dict);
        out << key << " " << val << " ";
    }
}

int main(int argc, char **argv) {
    if(argc < 2) {
        cout << "usage: " << argv[0] << " " << "[size]\n";
        return 0;
    }
    int size = std::atoi(argv[1]);
    vector<char> dict;
    ofstream out("data.txt");
    init_dict(dict);
    general_data(out, size, dict);
}
