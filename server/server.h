// server.h

#ifndef __SERVER_H
#define __SERVER_H

#include "event-compat.h"
#include "conninfo.h"







void server_listen(struct event_base *evbase, conninfo_t *conninfo);
void server_shutdown(void);


conninfo_t * server_conninfo(void);
// void server_set_conninfo(conninfo_t *conninfo);

void server_conn_closed(void);


#endif