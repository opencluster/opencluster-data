// server.c


#include "server.h"

#include "event-compat.h"
#include "client.h"
#include "conninfo.h"
#include "logging.h"

#include <assert.h>
#include <fcntl.h>
#include <netdb.h>
#include <stdlib.h>
#include <string.h>

// number of connections that we are currently listening for activity from.
static int _conncount = 0;

// maximum number of connections we allow to be open.  Used to stop a node from taking too many resources.   Includes server-node connections.
static int _maxconns = 1024;

static conninfo_t *_conninfo = NULL;

static struct event_base *_evbase = NULL;

static struct evconnlistener *_listener = NULL;



//-----------------------------------------------------------------------------
// accept an http connection.  Create the client object, and then attach the
// file handle to it.
static void accept_conn_cb(
	struct evconnlistener *listener,
	evutil_socket_t fd,
	struct sockaddr *address,
	int socklen,
	void *ctx)
{
	client_t *client;

	assert(listener);
	assert(fd > 0);
	assert(address && socklen > 0);
	assert(ctx == NULL);

	// create client object.
	// TODO: We should be pulling these client objects out of a mempool.
	client = client_new();
	client_accept(client, fd, address, socklen);

	_conncount ++;
}


// the server is being told that a client connection was closed, so that it can update its counters.
void server_conn_closed(void)
{
	assert(_conncount > 0);
	_conncount --;
	assert(_conncount >= 0);
}


//-----------------------------------------------------------------------------
// Listen for socket connections on a particular interface.
void server_listen(struct event_base *evbase, conninfo_t *conninfo)
{
	struct sockaddr_in sin;
	int len;
	
	assert(evbase);
	assert(_evbase == NULL);
	_evbase = evbase;
	
	assert(_maxconns > 0);
	assert(_conncount == 0);
	assert(_listener == NULL);

	memset(&sin, 0, sizeof(sin));
	// 	sin.sin_family = AF_INET;
	len = sizeof(sin);
	
	assert(_conninfo);
	const char *remote_addr = conninfo_remoteaddr(_conninfo);
	assert(remote_addr);
	
	assert(sizeof(struct sockaddr_in) == sizeof(struct sockaddr));
	if (evutil_parse_sockaddr_port(remote_addr, (struct sockaddr *)&sin, &len) != 0) {
		assert(0);
	}
	else {
		
		assert(_listener == NULL);
		assert(_evbase);
		
		logger(LOG_INFO, "listen: %s", remote_addr);

		_listener = evconnlistener_new_bind(
								_evbase,
								accept_conn_cb,
								NULL,
								LEV_OPT_CLOSE_ON_FREE|LEV_OPT_REUSEABLE,
								-1,
								(struct sockaddr*)&sin,
								sizeof(sin)
							);
		assert(_listener);
	}
}




void server_shutdown(void)
{
	assert(_conninfo);
	const char *remote_addr = conninfo_remoteaddr(_conninfo);
		
	logger(LOG_INFO, "Shutting down server interface: %s", remote_addr);

	// need to close the listener socket.
	if (_listener) {
		evconnlistener_free(_listener);
		_listener = NULL;
		logger(LOG_INFO, "Stopping listening on: %s", remote_addr);
	}
}



// void server_set_conninfo(conninfo_t *conninfo)
// {
// 	assert(_conninfo == NULL);
// 	assert(conninfo);
// 	
// 	_conninfo = conninfo;
// }



conninfo_t * server_conninfo(void)
{
	assert(_conninfo);
	return(_conninfo);
}


