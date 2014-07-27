// params.c

#include "params.h"

#include "logging.h"
#include "usage.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>



static int         _daemonize = 0;
static int         _max_connections = 1024;
static const char *_username = NULL;
static const char *_pid_file = NULL; 
static const char *_logfile = NULL;
static int         _logfile_max = 50;
static const char *_conninfo_file = NULL;
static const char *_node_file = NULL;



// parses the command-line parameters, and returns a params structure.  Do not necessarily process the information provided, other than applying defaults for params not supplied.
void params_parse_args(int argc, char **argv)
{
	int c;
	
	assert(argc >= 0);
	assert(argv);
	
	// process arguments
	/// Need to check the options in here, there're possibly ones that we dont need.
	while ((c = getopt(argc, argv, 
		"c:"    /* max connections. */
		"h"     /* help */
		"v"     /* verbosity */
		"d"     /* daemon */
		"u:"    /* user to run as */
		"P:"    /* PID file */
		"l:"    /* connect-info filename */
		"n:"    /* other node to connect to */
		"g:"	/* logfile */
		"m:"	/* logfile maximum size */
		)) != -1) {
		switch (c) {
			case 'c':
				_max_connections = atoi(optarg);
				assert(_max_connections > 0);
				break;
			case 'h':
				usage();
				exit(EXIT_SUCCESS);
			case 'v':
				log_inclevel();
				break;
			case 'd':
				assert(_daemonize == 0);
				_daemonize = 1;
				break;
			case 'u':
				assert(_username == NULL);
				_username = strdup(optarg);
				assert(_username != NULL);
				assert(_username[0] != 0);
				assert(_username[0] != '-');  // this is an assert because I believe getopt should generate an error before-hand.
				break;
			case 'P':
				assert(_pid_file == NULL);
				_pid_file = strdup(optarg);
				assert(_pid_file != NULL);
				assert(_pid_file[0] != 0);
				assert(_pid_file[0] != '-');
				break;
			case 'l':
				_conninfo_file = strdup(optarg);
				assert(_conninfo_file != NULL);
				assert(_conninfo_file[0] != 0);
				assert(_conninfo_file[0] != '-');
				break;
			case 'n':
				// disabled for now, because only single-node is working,
				assert(0);
				
				// and we should support receiving more than one node file in the params
				assert(0);
				
				assert(_node_file == NULL);
				_node_file = strdup(optarg);
				assert(_node_file);
				break;
				
			case 'g':
				assert(_logfile == NULL);
				_logfile = strdup(optarg);
				assert(_logfile);
				break;
				
			case 'm':
				assert(_logfile_max == 0);
				_logfile_max = atoi(optarg);
				assert(_logfile_max >= 0);
				break;
				
			default:
				fprintf(stderr, "Illegal argument \"%c\"\n", c);
				return;
				
		}	
	}
}




int params_get_daemonize(void)
{ 
	return(_daemonize);
}

const char * params_get_username(void)
{
	return(_username);
}


const char * params_get_pidfile(void)
{
	return(_pid_file);
}


const char * params_get_logfile(void)
{
	assert(_logfile);
	return(_logfile);
}

int params_get_logfile_max(void)
{
	assert(_logfile_max > 0);
	return(_logfile_max);
}


// free the resources used by the parameters object.
void params_free(void)
{
	if (_username) { free((void*)_username); _username = NULL; }
	if (_pid_file) { free((void*)_pid_file); _pid_file = NULL; }
	if (_conninfo_file) { free((void*)_conninfo_file); _conninfo_file = NULL; }
}



const char * params_get_conninfo_file(void)
{
	return(_conninfo_file);
}

