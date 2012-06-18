/*
	Command-line tool to SET a value in a cluster.
*/


#include <assert.h>
#include <opencluster.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

static char *_node = "127.0.0.1:13600";
static char *_map_s = NULL;
static char *_key_s = NULL;
static int _map_i = 0;
static int _key_i = 0;
static int _integer = 0;

static int _fieldmap = 0;


//-----------------------------------------------------------------------------
// print some info to the user, so that they can know what the parameters do.
static void usage(void) {
	printf(
		"Usage:\n"
		"  oc_get <options> -m|-M mapname -k|-K keyname\n\n"
		"  -i           Indicates expecting an integer.\n"
		"  -n <node>    Cluster node to connect to.\n"
		"  -h           print this help and exit\n\n"
		"The -m, -k and -v options indicate strings.\n"
		"The -M, -K and -V options indicate integers.\n\n"
		"For example, this means that you can have a string map with an integer key, \n"
		"and a string payload (using -m -K -v)\n\n"
		"-v and -s both mean a string value, -V and -i both mean an integer value.\n"
	);
	return;
}

static void parse_params(int argc, char **argv)
{
	int c;
	
	assert(argc >= 0);
	assert(argv);
	
	_fieldmap = 0;
	
	// process arguments
	/// Need to check the options in here, there're possibly ones that we dont need.
	while ((c = getopt(argc, argv, 
		"h"     /* help */
		"n:"    /* cluster node to connect to */
		"m:"    /* map string */
		"k:"    /* key string */
		"M:"    /* map integer */
		"K:"    /* key integer */
		"i"     /* expecting an integer */
		)) != -1) {
		switch (c) {
			
			/* help */
			case 'h':
				usage();
				exit(EXIT_SUCCESS);
				break;
				
			/* cluster node to connect to */
			case 'n':
				_node = optarg;
				break;
				
			/* map string */
			case 'm':
				assert(_map_i == 0);
				assert((_fieldmap & 1) == 0);
				_map_s = optarg;
				_fieldmap |= 1;
				break;
				
			/* key string */
			case 'k':
				assert(_key_i == 0);
				assert((_fieldmap & 2) == 0);
				_key_s = optarg;
				_fieldmap |= 2;
				break;
				
			/* map integer */
			case 'M':
				assert(_map_s == NULL);
				assert((_fieldmap & 1) == 0);
				_map_i = atoi(optarg);
				_fieldmap |= 1;
				break;
			
			/* key integer */
			case 'K':
				assert(_key_s == NULL);
				assert((_fieldmap & 2) == 0);
				_key_i = atoi(optarg);
				_fieldmap |= 2;
				break;
				
			case 'i':
				_integer = 1;
				break;
				
			default:
				fprintf(stderr, "Unexpected argument '\"%c\"''\n", c);
				return;
		}	
	}
	
	// we dont care about the map, but we must have key and value (4 + 2)
	if (_fieldmap < 2) {
		fprintf(stderr, "Insufficient parameters.\n");
		exit(1);
	}
}


int main(int argc, char **argv)
{
	cluster_t *cluster;
	int result;
	int nodes;
	char *str;
	int str_len;
	int data;

	// parse the command-line parameters.
	parse_params(argc, argv);
	
	// Initialising the cluster library
	cluster = cluster_init();
	
	// Adding node to the server list
	assert(_node);
	cluster_addserver(cluster, _node);
	
	nodes = cluster_connect(cluster);
	if (nodes <= 0) {
		fprintf(stderr, "Unable to connect to %s.", _node);
		cluster_free(cluster);
		exit(1);
	}
	else {
	
		// set an item in the cluster with some data.

		if (_key_s && _integer > 0) {
			// string key, integer value;

			data = 0;
 			result = cluster_getint(cluster, _key_s, &data);
 			assert(result == 0);
			printf ("%d\n", data);
		}
		else if (_key_s && _integer == 0) {
			result = cluster_getstr(cluster, _key_s, &str, &str_len);
			assert(result == 0);
			assert(str);
			assert(str_len > 0);
			printf("%s\n", str);
			free(str);
			str = NULL;
		}
		else {
			// we need to code in the other options.
			assert(0);
		}

		// Disconnecting from the cluster
		cluster_disconnect(cluster);		
	}
	
	// Shutting down
	cluster_free(cluster);
	
	return(0);
}

