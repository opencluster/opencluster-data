// server.h

#ifndef __SERVER_H
#define __SERVER_H

// The server object is meant to manage a listening socket.  It also provides some common functionality for handling the incoming socket connections.



#include "event-compat.h"
#include "conninfo.h"


typedef struct {
	
} server_t;


void server_evbase(struct event_base *evbase);
void server_listen(conninfo_t *conninfo);
void server_shutdown(void);


conninfo_t * server_conninfo(void);
// void server_set_conninfo(conninfo_t *conninfo);

void server_conn_inc(void);
void server_conn_closed(void);


#endif