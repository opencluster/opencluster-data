// config.h

// this is a generic config file loader.   No application specific code should be here.
// since this code can be used to load multiple config files, we need an easy way of keeping them seperate.

// actually, since this initial implementation will only use a single config file, it will be done from that perspective, with the capacity to switch configs 


#ifndef __CONFIG_H
#define __CONFIG_H


// load the configfile
int config_load(const char *path);

const char * config_get(const char *key);
int config_get_bool(const char *key);
long long config_get_long(const char *key);

void config_free(void);



#endif