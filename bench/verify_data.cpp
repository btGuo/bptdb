#include <string>
#include <fstream>
#include <cassert>
#include <iostream>
#include <cstdlib>
#include <set>

int main() {
    std::ifstream in("./data.txt");
    assert(in.is_open());
    std::string key;
    std::set<std::string> s;
    while(!in.eof()) {
        in >> key;
        s.insert(key);
        in >> key;
    }
    std::cout << s.size() << std::endl;
}

