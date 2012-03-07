// server.h

#ifndef __SERVER_H
#define __SERVER_H

typedef struct {
	struct evconnlistener *listener;
} server_t;



void server_listen(void);
void server_shutdown(void);


#ifndef __SERVER_C
	extern const char *_interface;
	extern int _maxconns;

#endif


#endif