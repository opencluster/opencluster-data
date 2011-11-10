#include <stdio.h>
#include <string.h>
#include <assert.h>

//#define LIMIT 3371500
#define LIMIT 50000000

// 	const int kFNVOffset = 2166136261l;
// 	const int kFNVPrime = 16777619;

#define kFNVOffset (2166136261l)
#define kFNVPrime (16777619)


// FNV hash.  Public domain function converted from public domain Java version.
inline unsigned int generate_hash(char *str, int length)
{
	register int i = length;
	register char *ptr = str;

	register unsigned int hash = kFNVOffset;

	for (i=0; i<length; i++) {
//		hash ^= (0x0000ffff & (int)ptr[i]);
 		hash ^= (int)ptr[i];
//		hash += (hash << 1) + (hash << 4) +  (hash << 7) + (hash << 8) + (hash << 24);
		hash *= kFNVPrime;
// 		ptr ++;
	}

	return  hash;
}



#include "stdint.h" /* Replace with <stdint.h> if appropriate */
#undef get16bits
#if (defined(__GNUC__) && defined(__i386__)) || defined(__WATCOMC__) \
  || defined(_MSC_VER) || defined (__BORLANDC__) || defined (__TURBOC__)
#define get16bits(d) (*((const uint16_t *) (d)))
#endif

#if !defined (get16bits)
#define get16bits(d) ((((uint32_t)(((const uint8_t *)(d))[1])) << 8)\
                       +(uint32_t)(((const uint8_t *)(d))[0]) )
#endif

uint32_t SuperFastHash (const char * data, int len) {
uint32_t hash = len, tmp;
int rem;

    if (len <= 0 || data == NULL) return 0;

    rem = len & 3;
    len >>= 2;

    /* Main loop */
    for (;len > 0; len--) {
        hash  += get16bits (data);
        tmp    = (get16bits (data+2) << 11) ^ hash;
        hash   = (hash << 16) ^ tmp;
        data  += 2*sizeof (uint16_t);
        hash  += hash >> 11;
    }

    /* Handle end cases */
    switch (rem) {
        case 3: hash += get16bits (data);
                hash ^= hash << 16;
                hash ^= data[sizeof (uint16_t)] << 18;
                hash += hash >> 11;
                break;
        case 2: hash += get16bits (data);
                hash ^= hash << 11;
                hash += hash >> 17;
                break;
        case 1: hash += *data;
                hash ^= hash << 10;
                hash += hash >> 1;
    }

    /* Force "avalanching" of final 127 bits */
    hash ^= hash << 3;
    hash += hash >> 5;
    hash ^= hash << 4;
    hash += hash >> 17;
    hash ^= hash << 25;
    hash += hash >> 6;

    return hash;
}




int main(void)
{

	char *teststr =  "when something is wrong, with my baby... something is wrong, with me.   ";
	char *teststr2 = "twinkle twinkle little star, how i wonder what you are. Up above the world so high, like a diamond in the sky.";
	int length, length2;
	unsigned int hash;
	int limit = LIMIT;


	printf("Number of hashes: %d\n", limit);
	
	assert(sizeof(int) == 4);
	
	length = strlen(teststr);
	length2 = strlen(teststr2);

	for (limit=0; limit < LIMIT; limit += 2)  {
		hash = generate_hash(teststr, length);
		hash = generate_hash(teststr2, length2);
		
// 		hash = SuperFastHash(teststr, length);
// 		hash = SuperFastHash(teststr2, length2);
		
		
	}

	printf("hash is: %u\n", hash);


	return 0;
}
