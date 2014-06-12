// hashfn.h

#ifndef __HASHFN_H
#define __HASHFN_H

#include "hash.h"

hash_t generate_hash_str(const char *str, const int length);
hash_t generate_hash_long(const long long key);


#endif