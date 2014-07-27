// conninfo.c

#include "conninfo.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>


char const *KEY_NAME = "name";
char const *KEY_IP = "ip";
char const *KEY_PORT = "port";


static conninfo_t * conninfo_new(void)
{
	conninfo_t *conninfo = calloc(1, sizeof(conninfo_t));
	assert(conninfo);
	assert(conninfo->conninfo_str == NULL);
	assert(conninfo->valid == 0);
	return(conninfo);
}

// release the resources allocated to this object.
void conninfo_free(conninfo_t *conninfo)
{
	assert(conninfo);

	if (conninfo->name) { free(conninfo->name); conninfo->name = NULL; }
	if (conninfo->original) { free(conninfo->original); conninfo->original = NULL; }
	if (conninfo->remote_addr) { free(conninfo->remote_addr); conninfo->remote_addr = NULL; }
	if (conninfo->conninfo_str) { free(conninfo->conninfo_str); conninfo->conninfo_str = NULL; }
	
	if (conninfo->root) {
		// decrease the reference count to the root json object, which should free it since nothing else should be using it.
		json_decref(conninfo->root);
		conninfo->root = NULL;
	}

	assert(0);
}


// return a string (that needs to be freed by the caller), that represents a raw lookup of the json 
// data.  Only goes one level deep, anything trickier will need to be parsed from the json data 
// itself externally.
char * conninfo_value_str(const conninfo_t *conninfo, const char *key)
{
	char *value = NULL;
	
	assert(conninfo);
	assert(key);
	assert(conninfo->root);
	
	json_t *js = json_object_get(conninfo->root, key);
	if (js) {
		
		if (json_is_string(js)) {
			const char *data = json_string_value(js);
			if (data) { 
				value = strdup(data); 
				assert(value);
			}
		}
		else if (json_is_integer(js)) {
			// since we are after a string, we need to convert the integer to a string value.
			int data = json_integer_value(js);
			value = malloc(32);
			int length = sprintf(value, "%d", data);
			assert(length < (32-1));
			value = realloc(value, length + 1);
		}
		else {
			assert(value == NULL);
		}
	}
	
	return(value);
}




// return a string (that needs to be freed by the caller), that represents a raw lookup of the json 
// data.  Only goes one level deep, anything trickier will need to be parsed from the json data 
// itself externally.
int conninfo_value_int(const conninfo_t *conninfo, const char *key)
{
	int value = 0;
	
	assert(conninfo);
	assert(key);
	assert(conninfo->root);
	
	json_t *js = json_object_get(conninfo->root, key);
	if (js) {
		
		if (json_is_string(js)) {
			const char *data = json_string_value(js);
			if (data) { 
				value = atoi(data);
			}
		}
		else if (json_is_integer(js)) {
			// since we are after a string, we need to convert the integer to a string value.
			value = json_integer_value(js);
		}
		else {
			assert(value == 0);
		}
	}
	
	return(value);
}




static int extract_name(conninfo_t *conninfo)
{
	int valid = 0;
	assert(conninfo);
	assert(conninfo->name == NULL);

	conninfo->name = conninfo_value_str(conninfo, KEY_NAME);
	if (conninfo->name) { valid = 1; }
	return(valid);
}


static int extract_remoteaddr(conninfo_t *conninfo)
{
	int valid = 0;
	assert(conninfo);
	
	assert(conninfo->remote_addr == NULL);
	
	char *ip = conninfo_value_str(conninfo, KEY_IP);
	int port = conninfo_value_int(conninfo, KEY_PORT);
	if (port <= 0) { port = DEFAULT_OPENCLUSTER_PORT; }
	
	if (ip) {

		assert(conninfo->remote_addr == NULL);
		int expected = strlen(ip) + 32;
		conninfo->remote_addr = malloc(expected);
		int size = sprintf(conninfo->remote_addr, "%s:%d", ip, port);
		assert(size > 0);
		assert(size < expected);
		conninfo->remote_addr = realloc(conninfo->remote_addr, size + 1);
		valid = 1;
		
		free(ip);
	}
	else {
		assert(conninfo->remote_addr == NULL);
		assert(valid == 0);
	}
	
	return(valid);
}

static int extract_data(conninfo_t *conninfo)
{
	int valid = 0;
	assert(conninfo);

	// export the json object into a new string which will be used as the conninfo_str.
	assert(conninfo->root);
	if (conninfo->conninfo_str) { free(conninfo->conninfo_str); conninfo->conninfo_str = NULL; }
	conninfo->conninfo_str = json_dumps(conninfo->root, JSON_COMPACT | JSON_SORT_KEYS);
	if (conninfo->conninfo_str == NULL) {
		// parsing the conninfo failed.  
		assert(0);
	}
	else {
		// need to parse the string to get all the meaningful information out.
// 		printf("Compacted block:\n%s\n", conninfo->conninfo_str);
	
		// the root object needs to be a json object, and not an array or any other type.
		if (json_is_object(conninfo->root)) {
			assert(valid == 0);			// valid should be 0 to begin with
			
			// name and remoteaddr are mandatory fields, so it is considered invalid if they dont exist.
			valid = extract_remoteaddr(conninfo);
			if (valid) { extract_name(conninfo); }
			
			assert(valid);
		}
		else {
			// the root object was not an object.  We cannot parse it.
			json_decref(conninfo->root);
			conninfo->root = NULL;
			assert(valid == 0);
		}
	}
	
	return(valid);
}


conninfo_t * conninfo_parse(const char *conninfo_str)
{
	conninfo_t *conninfo = NULL;
	
	assert(conninfo_str);
	
	// load the string into a root json object.
	assert(0);
	
	if (extract_data(conninfo) == 0) {
		assert(0);
		// the extraction failed.
		conninfo_free(conninfo);
		conninfo = NULL;
	}
	
	return(conninfo);
}

conninfo_t * conninfo_load(const char *connfile)
{
	conninfo_t *conninfo = NULL;
	json_error_t jerror;

	assert(connfile);
	
	conninfo = conninfo_new();
	assert(conninfo);

	assert(conninfo->root == NULL);
	if (strcmp("-", connfile) == 0) {
		conninfo->root = json_loadf(stdin, 0, &jerror);
	}
	else {
		conninfo->root = json_load_file(connfile, 0, &jerror);
	}
	
	if (conninfo->root == NULL) {
		// something happened loading the file.
		assert(0);
		
		conninfo_free(conninfo);
		conninfo = NULL;
	}
	else {
		// now we should have a loaded root file.
		if (extract_data(conninfo) == 0) {
			
			assert(0);
			// failed to extract the required data.
			conninfo_free(conninfo);
			conninfo = NULL;
		}
	}
	
	return(conninfo);
}

// load from a file handle, rather than giving a file path.
conninfo_t * conninfo_loadf(FILE *fp)
{
	conninfo_t *conninfo = NULL;
	json_error_t jerror;

	assert(fp);
	
	conninfo = conninfo_new();
	assert(conninfo);
	
	assert(conninfo->root == NULL);
	conninfo->root = json_loadf(fp, 0, &jerror);
	if (conninfo->root == NULL) {
		// something happened loading the file.
		assert(0);
		
		conninfo_free(conninfo);
		conninfo = NULL;
	}
	else {
		// now we should have a loaded root file.
		if (extract_data(conninfo) == 0) {
			
			assert(0);
			// failed to extract the required data.
			conninfo_free(conninfo);
			conninfo = NULL;
		}
	}
	
	return(conninfo);
}




// return the name of the node.   If no name was specified when the connect info was loaded, then 
// it should have generated a hash of the details and used that.
const char * conninfo_name(const conninfo_t *info)
{
	assert(info);
	assert(info->name);
	return(info->name);
}



// act like it is safely duplicating the conninfo object, but what it is really doing is simply increasing the refcount.
conninfo_t * conninfo_dup(conninfo_t *info)
{
	assert(info);
	assert(info->refcount >= 0);
	info->refcount ++;
	return(info);
}


const char * conninfo_str(const conninfo_t *conninfo)
{
	assert(conninfo);
	assert(conninfo->original);
	return((const char *) conninfo->original);
}


// when making a remote connection, we have a simple remote_addr field to send to the libevent 
// socket handling tools.   We may have to modify this to handle more complicated scenarios at some 
// point.
const char * conninfo_remoteaddr(conninfo_t *conninfo)
{
	assert(conninfo);
	return(conninfo->remote_addr);
}


// compare the string version of conninfo with the string supplied to see if they are the same.
int conninfo_compare_str(conninfo_t *conninfo, const char *str)
{
	assert(conninfo);
	assert(str);
	assert(conninfo->original);
	
	return(strcmp(conninfo->original, str));
}


// return 0 if the conninfo details have not parsed completely or correctly
// return 1 (non zero) if the conninfo parsed correctly.
int conninfo_isvalid(conninfo_t *conninfo) 
{
	assert(conninfo);
	return(conninfo->valid);
}




