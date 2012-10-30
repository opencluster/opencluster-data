#include <stdio.h>
#include <string.h>

typedef unsigned long long hash_t;

#define FNV_BASE_LONG   14695981039346656037ll
#define FNV_PRIME_LONG  1099511628211ll                      


// FNV1a(64bit) hash.  Public domain function converted from public domain Java version.
// TODO: modify this to a macro to improve performance a little.
hash_t generate_hash_str(const char *str, const int length)
{
	register int i;
	register hash_t hash = FNV_BASE_LONG;

	for (i=0; i<length; i++)  {
		hash ^= (int)str[i];
		hash *= FNV_PRIME_LONG;
	}
	
	return(hash);
}


void main(void) {

	hash_t hash;

	char *str;

	str = "charlie";

	hash = generate_hash_str(str, strlen(str));
	printf("%#llx\n", hash);


}



