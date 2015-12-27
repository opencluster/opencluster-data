// config.c

#include "config.h"

#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>



typedef struct {
	char *key;
	char *value;
} config_pair_t;


typedef struct {

	int items;
	config_pair_t *pairs;
	const char *path;
	
} config_t;


config_t * _current = NULL;


// free the resources for the current config that is loaded.
void config_free(void)
{
	assert(_current);
	if (_current->pairs) {
		assert(_current->items >= 0);
		
		while(_current->items > 0) {
			_current->items --;
			assert(_current->pairs[_current->items].key);
			free(_current->pairs[_current->items].key);
			_current->pairs[_current->items].key = NULL;
			
			assert(_current->pairs[_current->items].value);
			free(_current->pairs[_current->items].value);
			_current->pairs[_current->items].value = NULL;
		}
		
		free(_current->pairs);
	}
	
	if (_current->path) { free(_current->path); }
	
	free(_current);
	_current = NULL;
}


// load a config file into an array.   Return 0 on success.
int config_load(const char *path)
{
	assert(path);
	int result = 0;
	
	// if there is a config already loaded, we need to free it.
	if (_current) {
		config_free();
		assert(_current == NULL);
	}
	
	_current = calloc(1, sizeof(config_t));
	assert(_current->items == 0);
	assert(_current->pairs == NULL);
	
	// store the path to the config, so that we can know which one it is if we switch configs.
	_current->path = strdup(path);
	
	// open the file
	int fd = open(path, 0);
	if (fd < 0) {
		#ifndef NDEBUG
			printf("an error occurred trying to open the file: %s\n", path);
		#endif
		result = -1;
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
			char *buffer = strndup(file_buffer, file_length);
			assert(buffer);
			
			// release the mmap, because we dont need it anymore, we have copied the data.
			munmap(file_buffer, file_length);  
			file_buffer = NULL;

			// first parse, remove all comments and blank lines.
			char *next_line = buffer;
			while (next_line) {
				char *line = strsep(&next_line, "\n");
				int line_length = strlen(line);
				
				if (line_length > 0) {
					// trim all whitespace from the beginning of the line.
// 					printf("raw line: [%s]\n", line);
					while(line_length > 0 && (line[0] == ' ' || line[0] == '\t' || line[0] == '\r')) { 
						line ++; 
						line_length --;
					}
					
					if (line_length > 0) {
					
// 						printf("after trim front: [%s]\n", line);
						
						if (line[0] != '#' ) {
						
							// trim all whitespace at the end of the line
							assert(line_length > 0);
							line_length--;
							while(line_length > 0 && (line[line_length] == ' ' || line[line_length] == '\t' || line[0] == '\r')) {
								line[line_length] = 0;
								line_length --;
							}
// 							printf("after trim end: [%s]\n", line);
							
							// we now have a line that is trimmed.  
							// second parse, split on '='
							char *value=index(line, '=');
							if (value) {
								value[0] = 0;
								value++;
								while(value[0] == ' ' || value[0] == '\t' || value[0] == '\r') {
									value ++;
								}
								
								int len = strlen(value);
								len --;
								while(len > 0 && (value[len] == ' ' || value[len] == '\t')) {
									value[len] = 0;
									len --;
								}
								
								// trim leading and trailing spaces from keys and values.
// 								printf("%s='%s'\n", line, value);
								assert(_current);
								assert(_current->items >= 0);
								_current->pairs = realloc(_current->pairs, sizeof(config_pair_t) * (_current->items+1));
								assert(_current->pairs);
								
								_current->pairs[_current->items].key = strdup(line);
								_current->pairs[_current->items].value = strdup(value);
								_current->items++;
							}
						}
					}
				}
			}
			
			
			free(buffer);
			buffer =  NULL;
		}
		
		// close the file
		close(fd);
		fd = -1;
	}	
	
	return(result);
}



const char * config_get(const char *key)
{
	assert(key);
	const char *value = NULL;

	assert(_current);
	
	int i;
	for (i=0; i<_current->items; i++) {
		assert(_current->pairs[i].key);
		assert(_current->pairs[i].value);
		
// 		printf("checking: %s\n", _current->pairs[i].key);
		
		if(strcasecmp(key, _current->pairs[i].key) == 0) {
			value = _current->pairs[i].value;
			i = _current->items;
		}
	}
	
	return(value);
}


// get the config value and convert to a long.  If the value does not exist, or does not convert, 
// then a 0 is returned.
long long config_get_long(const char *key)
{
	long long result = 0;
	
	assert(key);
	const char *value = config_get(key);
	if (value) {
		result = atoll(value);
	}
	return(result);
}


// This will search for the key, and if it is 'yes' or 'true' or 1, then it will return a non-zero 
// number (most probably 1).  Otherwise, it will return 0.
int config_get_bool(const char *key)
{
	int result = 0;
	
	assert(key);
	const char *value = config_get(key);
	if (value) {
		char *ptr = value;
		
		// skip any wrapping chars that are likely to be used.
		while(*ptr == '"' || *ptr == '\'' || *ptr == '(' || *ptr == '[') {
			ptr ++;
		}
		
		if (*ptr == 't' || *ptr == 'T' || *ptr == 'y' || *ptr == 'Y' || *ptr == '1') {
			result = 1;
		}
	}
	
	return(result);
}


