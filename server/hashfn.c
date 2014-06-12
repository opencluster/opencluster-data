// hashfn.c

#include <assert.h>
#include <endian.h>

#include "hashfn.h"


#define FNV_BASE_LONG   14695981039346656037llu
#define FNV_PRIME_LONG  1099511628211llu                      


// FNV hash.  Public domain function converted from public domain Java version.
// TODO: modify this to a macro to improve performance a little.
hash_t generate_hash_str(const char *str, const int length)
{
	register int i;
	register hash_t hash = FNV_BASE_LONG;

	for (i=0; i<length; i++)  {
		hash ^= (unsigned int)str[i];
		hash *= FNV_PRIME_LONG;
	}

	return(hash);
}




hash_t generate_hash_long(const long long key)
{
	register int i;
	register hash_t hash = FNV_BASE_LONG;
	union {
		long long nkey;
		char str[sizeof(long long)];
	} match;

	assert(sizeof(key) == 8);
	assert(sizeof(match) == sizeof(key));
	
	match.nkey = htobe64(key);

	for (i=0; i<sizeof(key); i++)  {
		hash ^= (unsigned int)match.str[i];
		hash *= FNV_PRIME_LONG;
	}

	return(hash);
}

