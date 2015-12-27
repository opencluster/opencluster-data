/*
	Command-line tool to load the conninfo file and change certain elements in it, printing the new version to standard output.
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
		"  conninfo_set inputfile key \"value\"\n\n"
		"Valid Keys:\n"
		"  name\n"
		"  ip\n"
		"  port\n"
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
		
		if (strcmp("-", argv[1]) == 0) { conninfo = conninfo_loadf(stdin); }
		else { conninfo = conninfo_load(argv[1]); }
		
		if (conninfo == NULL) {
			fprintf(stderr, "Error opening file: %s\n", argv[1]);
			return(1);
		}
		else {
			assert(conninfo);

			if (strcmp(argv[2], "remote_addr") == 0) {
				// remote_addr is a special value that is a combination of other values, so cannot be set directly.
				fprintf(stderr, "%s\n", "Unable to set remote_addr directly, as it is a combination of the 'ip' and 'port' settings.");
				assert(found == 0);
			}
			else {
				
				// the conninfo library is pretty much meant only for reading the conninfo data in a 
				// consistent manner.  Therefore, to make changes to the structure, we need to do it 
				// ourself.
				
				// since the conninfo was parsed, it should be at least marginally valid.
				assert(conninfo->root);

				json_t *js_new = NULL;

				// if the item already exists, then we will find out if it is already an integer try to keep the same type.
				json_t *js = json_object_get(conninfo->root, argv[2]);
				if (js) {
					if (json_is_integer(js)) {
						js_new = json_integer(atoi(argv[3]));
					}
				}
				
				// if it didn't exist, or it wasn't an integer, then we will create the new one as a string.
				if (js_new == NULL) {
					js_new = json_string(argv[3]);
				}

				assert(js_new);
				int success = json_object_set_new(conninfo->root, argv[2], js_new);
				assert(success == 0);

				char *output = json_dumps(conninfo->root, JSON_COMPACT | JSON_SORT_KEYS);
				assert(output);
				printf("%s\n", output);
				found++;
			}
		}
	}
	
	// if the key was found, return 0, otherwise return 1.
	return(found > 0 ? 0 : 1);
}

