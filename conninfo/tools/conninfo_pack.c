/*
	Command-line tool to load the conninfo file and extract particular keys from it.
*/


#include <assert.h>
#include <conninfo.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>




//-----------------------------------------------------------------------------
// print some info to the user, so that they can know what the parameters do.
static void usage(void) {
	printf( 
		"Usage:\n"
		"  conninfo_pack inputfile > outputfile\n\n"
	);
	return;
}


int main(int argc, char **argv)
{
	// if we dont have a param, then we display some usage info.
	if (argc < 2) {
		usage();
		return(1);
	}
	else {

		conninfo_t *conninfo;
		conninfo = conninfo_load(argv[1]);
		if (conninfo == NULL) {
			fprintf(stderr, "Error opening file: %s\n", argv[1]);
			return(1);
		}
		else {
			assert(conninfo);
			assert(conninfo->conninfo_str);
			printf("%s\n", conninfo->conninfo_str);
		}
	}
	
	return(0);
}

