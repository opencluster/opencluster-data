#include <stdio.h>
#include <endian.h>


int main(void)
{
	long long t1 = 0x12345678;
	long long t2 = 0x123456789ABCDEF;
	unsigned long long t3 = 0xFFEEFFEEFFEEFFEE;
	unsigned int t4 = 0x12345;
	unsigned int t5 = 0x12345678;

	printf("sizeof(char) == %d\n", (int) sizeof(char));
	printf("sizeof(short) == %d\n", (int) sizeof(short));
	printf("sizeof(int) == %d\n", (int) sizeof(int));
	printf("sizeof(long) == %d\n", (int) sizeof(long));
	printf("sizeof(long long) == %d\n", (int) sizeof(long long));
	
	printf("t1(%d) = %#llx\n", (int) sizeof(t1), t1);
	printf("t2(%d) = %#llx\n", (int) sizeof(t2), t2);
	printf("t3(%d) = %#llx\n", (int) sizeof(t3), t3);
	printf("t4(%d) = %#llx\n", (int) sizeof(t4), t4);
	printf("t5(%d) = %#llx\n", (int) sizeof(t5), t5);

	return 0;
}


