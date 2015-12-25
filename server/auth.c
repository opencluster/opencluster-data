
/* 
 * Authentication details for authentication.
 */

#include "auth.h"

#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>


#include <assert.h>
#include <dirent.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>



char ** _sync_list = NULL;
char ** _query_list = NULL;


static void free_list(char **list)
{
	assert(list);
	int i = 0;
	while (list[i]) {
		free(list[i]);
		list[i] = NULL;
	}
	free(list);	
}


void auth_free(void)
{
	if (_sync_list) { 
		free_list(_sync_list);
		_sync_list = NULL;
	}
	
	if (_query_list) {
		free_list(_query_list);
		_query_list = NULL;
	}
	
	assert(_sync_list == NULL);
	assert(_query_list == NULL);
}


static char * load_file(const char *directory, const char *filename)
{
	char *contents = NULL;
	
	char path[4096];
	
	assert(directory);
	assert(directory[0] != 0);
	assert(filename);
	assert(filename[0] != 0);
	
	strcpy(path, directory);
	strcat(path, "/");
	strcat(path, filename);
	
	// open the file
	int fd = open(path, 0);
	if (fd < 0) {
		assert(contents == NULL);
	}
	else {
		// find out the length of the file.
		struct stat sb;
		fstat(fd, &sb);
		
		off_t file_length = sb.st_size;
		
		// read the entire file into a buffer.
		char *file_buffer = mmap(NULL, file_length, PROT_READ, MAP_PRIVATE, fd, 0);
		if (file_buffer != MAP_FAILED) {
			
			// copy the data from the file to a new string, because it will get modified while we are parsing it.
			assert(file_buffer);
			contents = strndup(file_buffer, file_length);
			assert(contents);
			
			// release the mmap, because we dont need it anymore, we have copied the data.
			munmap(file_buffer, file_length);  
			file_buffer = NULL;
		}

		// close the file.  We are finished with it.
		close(fd);
	}
	
	return(contents);
}

// will return an array of char pointers.  The end of the list will be indicated by an entry pointing to NULL.
static char ** load_dir(const char *directory)
{
	int keys = 0;
	char ** list = NULL;
	
	DIR *dir = opendir(directory);
	if (dir) {
		
		struct dirent *entry;
		
		while ((entry = readdir(dir))) {
			if (entry->d_name[0] != '.') {
				if (entry->d_type != DT_DIR) {

					// allocate enough for 2 entries.  This one, and the ending one that will be set at the end of the loop.
					list = realloc(list, sizeof(char *) * (keys+2));
					assert(list);
					
					list[keys] = load_file(directory, entry->d_name);
					keys ++;
				}
			}
		}
		
		assert(keys >= 0);
		list[keys] = NULL;
		
		closedir(dir);
	}
	
	return(list);
}



// load all the keys from the directory.   These should all be private keys.  Returns a 0 if no keys 
// were found.  Returnes 1 if keys were found.
int auth_sync_load(const char *sync_dir)
{
	assert(sync_dir);
	assert(sync_dir[0] != 0);
	
	assert(_sync_list == NULL);
	
	_sync_list = load_dir(sync_dir);
	
	// if we found keys, return 1, otherwise, return 0.
	if (_sync_list) { return(1); }
	else { return(0); }
}


// load all the keys from the directory.   These should all be private keys.  Returns a 0 if no keys 
// were found.  Returnes 1 if keys were found.
int auth_query_load(const char *query_dir)
{
	assert(query_dir);
	assert(query_dir[0] != 0);
	
	assert(_query_list == NULL);
	
	_query_list = load_dir(query_dir);
	
	// if we found keys, return 1, otherwise, return 0.
	if (_query_list) { return(1); }
	else { return(0); }
}





