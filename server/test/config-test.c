#include <stdio.h>

#include "../config.h"

int main(void) 
{

	config_load("testfile.conf");

	printf("test-setting: %s\n", config_get("test-setting"));

	config_free();

	return(0);
}
