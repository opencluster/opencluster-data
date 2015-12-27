// params.c

#include "params.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>


static const char *_configfile = NULL;
static int _usage = 0;

// parses the command-line parameters, and returns a params structure.  Do not necessarily process the information provided, other than applying defaults for params not supplied.
void params_parse_args(int argc, char **argv)
{
	int c;
	
	assert(argc >= 0);
	assert(argv);
	
	// process arguments
	/// Need to check the options in here, there're possibly ones that we dont need.
	while ((c = getopt(argc, argv, 
		"h"     /* help */
		"k:"    /* config file */
		)) != -1) {
		switch (c) {
			case 'h':
				_usage = 1;
				break;
				
			case 'k':
				_configfile = strdup(optarg);
				assert(_configfile);
				break;
				
			default:
				fprintf(stderr, "Illegal argument \"%c\"\n", c);
				return;
		}
	}
}

// free resources used.
void params_free(void)
{
	if (_configfile) {
		free(_configfile);
		_configfile = NULL;
	}
}


const char * params_configfile(void)
{
	return(_configfile);
}

int params_usage(void)
{
	return(_usage);
}
