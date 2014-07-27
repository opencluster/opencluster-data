/*
	Command-line tool to load the conninfo file and pack it, printing the packed version to standard output.
	If no command-line options, use stdin.
*/


#include <assert.h>
#include <conninfo.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>


//-----------------------------------------------------------------------------
// print some info to the user, so that they can know what the parameters do.
static void usage(void) {
	printf( 
		"Usage:\n"
		"  conninfo_get inputfile key\n\n"
		"Valid Keys:\n"
		"  name\n"
		"  remote_addr\n"
		"\n"
	);
	return;
}


int main(int argc, char **argv)
{
	int found = 0;	
	
	// if we dont have a param, then we display some usage info.
	if (argc < 3) {
		usage();
		return(1);
	}
	else {

		conninfo_t *conninfo;
		
		if (strcmp("-", argv[1]) == 0) {
			conninfo = conninfo_loadf(stdin);
		}
		else {
			conninfo = conninfo_load(argv[1]);
		}
		
		if (conninfo == NULL) {
			fprintf(stderr, "Error opening file: %s\n", argv[1]);
			return(1);
		}
		else {
			assert(conninfo);

			if (strcmp(argv[2], "remote_addr") == 0) {
				// remote_addr is a special value that is a combination of other values
				if (conninfo->remote_addr) { 
					printf("%s\n", conninfo->remote_addr);
					found = 1;
				}
			}
			else {
				// look up general values inside the object if they exist.
				char *value = conninfo_value_str(conninfo, argv[2]);
				if (value) {
					printf("%s\n", value);
					found = 1;
					free(value);
				}
			}
		}
	}
	
	// if the key was found, return 0, otherwise return 1.
	return(found > 0 ? 0 : 1);
}

