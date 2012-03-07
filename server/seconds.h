#ifndef __SECONDS_H
#define __SECONDS_H



void seconds_init(void);
void seconds_shutdown(void);


#ifndef __SECONDS_C
	extern unsigned int _seconds;
#endif


#endif

