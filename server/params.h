// params.h

#ifndef __PARAMS_H
#define __PARAMS_H


void params_parse_args(int argc, char **argv);
void params_free(void);


int params_get_daemonize(void);
const char * params_get_username(void);
const char * params_get_pidfile(void);
const char * params_get_logfile(void);
int params_get_logfile_max(void);
const char * params_get_conninfo_file(void);


#endif