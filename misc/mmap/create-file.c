/* when using mmap, you cannot just simply append to an empty file.  You instead must create a file of a particular size, and then use mmap to map it into memory.  this would not be a good option for, say, logfiles, but would be for binary chunks */


#include <unistd.h>
#include <stdio.h>
#include <sys/types.h>


#define TEST_FILE "datafile"


int main(void)
{
	long page_size = 0;
	off_t block_size;
	int fd = 0;


	page_size = sysconf(_SC_PAGE_SIZE);
	printf("Page size: %ld\n", page_size);


	block_size = 100 * page_size;
	printf("Block size: %ld\n", block_size);

	// open the file.  If it doesn't exist, should create it.


	// get the size of the file.


	// Add 100 blocks to that file size.


	if (truncate(TEST_FILE, block_size) != 0) {
		printf("Unable to create file\n");
		return 1;
	}
	else {
		// file was created.

		

	}
	


	return 0;
}



