// server.c
#define __SERVER_C
#include "server.h"
#undef __SERVER_C

#include "event-compat.h"
#include "client.h"
#include "globals.h"
#include "logging.h"

#include <assert.h>
#include <fcntl.h>
#include <netdb.h>
#include <stdlib.h>
#include <string.h>

// number of connections that we are currently listening for activity from.
int _conncount = 0;

// list of listening sockets (servers). 
server_t *_server = NULL;

// default interface.
// const char *_interface = "127.0.0.1:13600";

int _maxconns = 1024;




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
	assert(ctx);

	// create client object.
	// TODO: We should be pulling these client objects out of a mempool.
	client = client_new();
	client_accept(client, fd, address, socklen);
}



//-----------------------------------------------------------------------------
// Listen for socket connections on a particular interface.
void server_listen(void)
{
	struct sockaddr_in sin;
	int len;
	
	assert(_server == NULL);
	assert(_interface);
	assert(_maxconns > 0);
	assert(_conncount == 0);
	
	_server = calloc(1, sizeof(*_server));
	assert(_server);

	_server->listener = NULL;

	memset(&sin, 0, sizeof(sin));
	// 	sin.sin_family = AF_INET;
	len = sizeof(sin);
	
	assert(sizeof(struct sockaddr_in) == sizeof(struct sockaddr));
	if (evutil_parse_sockaddr_port(_interface, (struct sockaddr *)&sin, &len) != 0) {
		assert(0);
	}
	else {
		
		assert(_server->listener == NULL);
		assert(_evbase);
		
		logger(LOG_INFO, "listen: %s", _interface);

		_server->listener = evconnlistener_new_bind(
								_evbase,
								accept_conn_cb,
								_server,
								LEV_OPT_CLOSE_ON_FREE|LEV_OPT_REUSEABLE,
								-1,
								(struct sockaddr*)&sin,
								sizeof(sin)
							);
		assert(_server->listener);
}	}




void server_shutdown(void)
{
	if (_server) {
		logger(LOG_INFO, "Shutting down server interface: %s", _interface);

		// need to close the listener socket.
		if (_server->listener) {
			evconnlistener_free(_server->listener);
			_server->listener = NULL;
			logger(LOG_INFO, "Stopping listening on: %s", _interface);
		}
		
		free(_server);
		_server = NULL;
	}
}

