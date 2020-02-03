#ifndef __TYPE_CODING_H
#define __TYPE_CODING_H

#include <string>

namespace bptdb {
namespace typeCoding {

template<typename T>
char code(T *p = nullptr) { return -1; }

template<>
char code(int *p) { return 1; }

template<>
char code(short *p) { return 2; }

template<>
char code(double *p) { return 3; }

template<>
char code(float *p) { return 4; }

template<>
char code(std::string *p) { return 5; }

}// typeCoding
}// namespace bptdb

#endif
