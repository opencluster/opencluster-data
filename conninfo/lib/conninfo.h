// conninfo.h

#ifndef __CONNINFO_H
#define __CONNINFO_H

#include <jansson.h>



typedef struct {
	char *name;
	char *original;
	char *conninfo_str;
	char *remote_addr;
	int refcount;
	json_t *root;
	int valid;
} conninfo_t;


#define DEFAULT_OPENCLUSTER_PORT  31336


void conninfo_free(conninfo_t *conninfo);

conninfo_t * conninfo_parse(const char *conninfo_str);
conninfo_t * conninfo_load(const char *connfile);
conninfo_t * conninfo_loadf(FILE *fp);

const char * conninfo_name(const conninfo_t *info);
conninfo_t * conninfo_dup(conninfo_t *info);
const char * conninfo_str(const conninfo_t *conninfo);
const char * conninfo_remoteaddr(conninfo_t *conninfo);
int conninfo_compare_str(conninfo_t *conninfo, const char *str);

char * conninfo_value_str(const conninfo_t *conninfo, const char *key); 

// return 0 if the conninfo details have not parsed completely or correctly
// return 1 (non zero) if the conninfo parsed correctly.
int conninfo_isvalid(conninfo_t *conninfo);

#endif