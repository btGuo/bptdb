#include <fstream>
#include <cassert>
#include <iostream>
#include <set>

using namespace std;

int main() {
    ifstream in("data.txt");
    assert(in.is_open());
    string str;
    set<string> s;
    int size = 655360;
    while(size-- && !in.eof()) {
        in >> str;
        s.insert(str);
        in >> str;
    }
    cout << s.size() << endl;
}

    
