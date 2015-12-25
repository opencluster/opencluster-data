// params.h

#ifndef __PARAMS_H
#define __PARAMS_H

void params_parse_args(int argc, char **argv);
void params_free(void);

const char * params_configfile(void);
int params_usage(void);

#endif