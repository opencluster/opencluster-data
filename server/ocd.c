//-----------------------------------------------------------------------------
// ocd - Open Cluster Daemon
//	Enhanced hash-map storage cluster.
//-----------------------------------------------------------------------------


#include "event-compat.h"

// includes

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <jansson.h>
#include <netdb.h>
#include <pwd.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>



#define PACKAGE 						"ocd"
#define VERSION 						"0.10"


#define DEFAULT_BUFSIZE 4096
// #define DEFAULT_MAX_SPLIT (1024 * 1024 * 1024)

#ifndef INVALID_HANDLE
#define INVALID_HANDLE -1
#endif




//-----------------------------------------------------------------------------
// common structures.

typedef struct {
	struct evconnlistener *listener;
	void **clients;
	int clientcount;
} server_t;



typedef struct {
	evutil_socket_t handle;
	struct event *read_event;
	struct event *write_event;
	server_t *server;

	char *out_buffer;
	int out_offset;
	int out_length;
	int out_max;

	char *in_buffer;
	int in_offset;
	int in_length;
	int in_max;

	
	int closing;
} client_t;




//-----------------------------------------------------------------------------
// Global Variables.    
// If we ever add threads then we need to be careful when accessing these 
// global variables.

// event base for listening on the sockets, and timeout events.
struct event_base *_evbase = NULL;


// linked list of listening sockets. 
server_t **_servers = NULL;		// server_t
int _server_count = 0;

// generic read buffer for reading from the socket or from the file.  It 
// should always be empty, and anything left after processing should be put in 
// a pending buffer.
char *_readbuf[DEFAULT_BUFSIZE];


// number of connections that we are currently listening for activity from.
int _conncount = 0;

// startup settings.
const char *_interfaces = "127.0.0.1:13600";
int _maxconns = 1024;
int _verbose = 0;
int _daemonize = 0;
unsigned int _threshold = 0;
const char *_username = NULL;
const char *_pid_file = NULL;


// signal catchers that are used to clean up, and store final data before 
// shutting down.
struct event *_sigint_event = NULL;
struct event *_sighup_event = NULL;







//-----------------------------------------------------------------------------
// Pre-declare our handlers, because often they need to interact with functions
// that are used to invoke them.
static void accept_conn_cb(struct evconnlistener *listener, evutil_socket_t fd, struct sockaddr *address, int socklen, void *ctx);
static void read_handler(int fd, short int flags, void *arg);
static void write_handler(int fd, short int flags, void *arg);





//-----------------------------------------------------------------------------
// Initialise the server object.
static void server_init(server_t *server)
{
	assert(server);

	server->listener = NULL;

	assert(_maxconns > 0);
	assert(_conncount == 0);

	server->clients = NULL;
	server->clientcount = 0;
}


//-----------------------------------------------------------------------------
// Listen for socket connections on a particular interface.
static void server_listen(server_t *server, char *interface)
{
	struct sockaddr_in sin;
	int len;
	
	assert(server);
	assert(interface);
	
	memset(&sin, 0, sizeof(sin));
	// 	sin.sin_family = AF_INET;
	len = sizeof(sin);
	
	assert(sizeof(struct sockaddr_in) == sizeof(struct sockaddr));
	if (evutil_parse_sockaddr_port(interface, (struct sockaddr *)&sin, &len) != 0) {
		assert(0);
	}
	else {
		
		assert(server->listener == NULL);
		assert(_evbase);
		
		server->listener = evconnlistener_new_bind(
								_evbase,
								accept_conn_cb,
								server,
								LEV_OPT_CLOSE_ON_FREE|LEV_OPT_REUSEABLE,
								-1,
								(struct sockaddr*)&sin,
								sizeof(sin)
							);
		assert(server->listener);
	}	
}



static void init_servers(void)
{
	server_t *server;
	char *copy;
	char *next;
	char *argument;
	
	assert(_interfaces);
	assert(_servers == NULL);
	assert(_server_count == 0);

	// make a copy of the supplied string, because we will be splitting it into
	// its key/value pairs. We dont want to mangle the string that was supplied.
	copy = strdup(_interfaces);
	assert(copy);

	next = copy;
	while (next != NULL && *next != '\0') {
		argument = strsep(&next, ",");
		if (argument) {
		
			// remove spaces from the begining of the key.
			while(*argument==' ') { argument++; }
			
			if (strlen(argument) > 0) {
				server = malloc(sizeof(*server));
				server_init(server);
				_servers = realloc(_servers, sizeof(void*)*(_server_count+1));
				_servers[_server_count] = server;
				_server_count++;
		
				if (_verbose) printf("listen: %s\n", argument);
				
				server_listen(server, argument);
			}
		}
	}
	
	free(copy);
}






//-----------------------------------------------------------------------------
// Initialise the client structure.
static void client_init ( client_t *client, server_t *server, evutil_socket_t handle, struct sockaddr *address, int socklen)
{
	assert(client);
	assert(server);

	assert(handle > 0);
	client->handle = handle;
	
	if (_verbose) printf("New client - handle=%d\n", handle);
	
	client->read_event = NULL;
	client->write_event = NULL;
	client->server = server;
	
	client->out_buffer = NULL;
	client->out_offset = 0;
	client->out_length = 0;
	client->out_max = 0;
	
	client->in_buffer = NULL;
	client->in_offset = 0;
	client->in_length = 0;
	client->in_max = 0;
	
	client->closing = 0;

	// add the client to the list for the server.
	if (server->clientcount > 0) {
		assert(server->clients != NULL);
		if (server->clients[server->clientcount-1] == NULL) {
			server->clients[server->clientcount-1] = client;
		}
		else {
			server->clients = realloc(server->clients, sizeof(void*)*(server->clientcount+1));
			assert(server->clients);
			server->clients[server->clientcount] = client;
			server->clientcount ++;
		}
	}	
	else {
		assert(server->clients == NULL);
		server->clients = malloc(sizeof(void*));
		server->clients[0] = client;
		server->clientcount = 1;
	}
	assert(server->clients && server->clientcount > 0);

	// assign fd to client object.
	assert(_evbase);
	assert(client->handle > 0);
	client->read_event = event_new(
		_evbase,
		client->handle,
		EV_READ|EV_PERSIST,
		read_handler,
		client);
	assert(client->read_event);
	int s = event_add(client->read_event, NULL);
	assert(s == 0);
}


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
	server_t *server = (server_t *) ctx;

	assert(listener);
	assert(fd > 0);
	assert(address && socklen > 0);
	assert(ctx);
	assert(server);

	// create client object.
	// TODO: We should be pulling these client objects out of a mempool.
	client = calloc(1, sizeof(*client));
	assert(client);
	client_init(client, server, fd, address, socklen);
}


//-----------------------------------------------------------------------------
// Free the resources used by the client object.
static void client_free(client_t *client)
{
	char found=0, resize=0;
	int i;
	
	assert(client);
	
	assert(client->server);
	if (_verbose >= 2) printf("client_free: handle=%d\n", client->handle);
	
	assert(client->out_length == 0);
	assert(client->out_offset == 0);
	if (client->out_buffer) {
		free(client->out_buffer);
		client->out_buffer = NULL;
		client->out_max = 0;
	}

	assert(client->in_length == 0);
	assert(client->in_offset == 0);;;
	if (client->in_buffer) {
		free(client->in_buffer);
		client->in_buffer = NULL;
		client->in_max = 0;
	}

	
	if (client->read_event) {
		event_free(client->read_event);
		client->read_event = NULL;
	}
	
	if (client->write_event) {
		event_free(client->write_event);
		client->write_event = NULL;
	}
	
	if (client->handle != INVALID_HANDLE) {
		EVUTIL_CLOSESOCKET(client->handle);
		client->handle = INVALID_HANDLE;
	}
	
	
	// remove the client from the server list.
	assert(client->server);
	assert(client->server->clients);
	assert(client->server->clientcount > 0);
	assert(found == 0);
	assert(resize == 0);
	for (i=0; i < client->server->clientcount; i++) {
		if (client->server->clients[i] == client) {
			found ++;
			client->server->clients[i] = NULL;
			if (i == (client->server->clientcount - 1)) {
				// client was at the end of the list, so we need to shorten it.
				client->server->clientcount --;
				assert(client->server->clientcount >= 0);
				resize ++;
			}
			else {
				i = client->server->clientcount;
			}
		}
	}
	assert(found == 1);
	if (client->server->clientcount > 2) {
		// if the first client entry is null, but the last one isnt, then move the last one to the front.
		if (client->server->clients[0] == NULL) {
			if (client->server->clients[client->server->clientcount-1] != NULL) {
				client->server->clients[0] = client->server->clients[client->server->clientcount-1];
				client->server->clients[client->server->clientcount-1] = NULL;
				client->server->clientcount --;
				resize ++;
			}
		}
	}
	
	if (resize > 0) {
		client->server->clients = realloc(
			client->server->clients, 
			sizeof(void*)*client->server->clientcount);
	}
	
	assert(client->server->clientcount >= 0);
	
	client->server = NULL;
}


static void client_shutdown(client_t *client)
{
	assert(client);
	
	assert(client->handle >= 0);
	
	assert(client->read_event);
	assert(client->server);
	
	// if we are still waiting to send some data, then we cant close the socket yet.
	assert(client->closing == 0);
	if (client->out_length > 0) {
		client->closing = 1;
	}
	else {
		client_free(client);
	}
}



static void server_shutdown(server_t *server)
{
	assert(server);

	// need to close the listener socket.
	if (server->listener) {
		evconnlistener_free(server->listener);
		server->listener = NULL;
		printf("Stopping listening\n");
	}

	while (server->clientcount > 0) {
		server->clientcount --;
		client_shutdown(server->clients[server->clientcount]);
	}
}


//-----------------------------------------------------------------------------
// Cleanup the server object.
static void server_free(server_t *server)
{
	assert(server);
	
	server_shutdown(server);
	
	assert(server->clientcount == 0);
	if (server->clients) {
		free(server->clients);
		server->clients = NULL;
	}
}


static void cleanup_servers(void)
{
	if (_servers) {
		while (_server_count > 0) {
			_server_count --;
			assert(_servers[_server_count]);
			server_free(_servers[_server_count]);
			free(_servers[_server_count]);
		}
	}

	free(_servers);
	_servers = NULL;
}



//-----------------------------------------------------------------------------
static void sigint_handler(evutil_socket_t fd, short what, void *arg)
{
	int i;

	assert(arg == NULL);
	
 	printf("SIGINT received (server_count=%d).\n\n", _server_count);
	
	// need to send a message to each node telling them that we are shutting down.
	
	assert(_servers);
	for (i=0; i<_server_count; i++) {
		assert(_servers[i]);
// 		printf("shutting down server.\n");
		server_shutdown(_servers[i]);
	}
	
	// delete the signal events.
	assert(_sigint_event);
	event_free(_sigint_event);
	_sigint_event = NULL;

	assert(_sighup_event == NULL);
//	event_free(_sighup_event);
//	_sighup_event = NULL;
	
// 	printf("SIGINT complete\n");
}


//-----------------------------------------------------------------------------
// When SIGHUP is received, we need to re-load the config database.  At the
// same time, we should flush all caches and buffers to reduce the system's
// memory footprint.   It should be as close to a complete app reset as
// possible.
static void sighup_handler(evutil_socket_t fd, short what, void *arg)
{
	assert(arg == NULL);

	// clear out all cached objects.
	assert(0);

	// reload the config database file.
	assert(0);

}









//-----------------------------------------------------------------------------
// when the write event fires, we will try to write everything to the socket.
// If everything has been sent, then we will remove the write_event, and the
// out_buffer.
static void write_handler(int fd, short int flags, void *arg)
{
	client_t *client;
	int res;
	
	assert(fd > 0);
	assert(arg);

	client = arg;

	// PERF: if a performance issue is found with sending large chunks of data 
	//       that dont fit in single send, we might be wasting time by purging 
	//       sent data from hte buffer which results in moving data in memory.
	
	assert(client->write_event);
	assert(client->out_buffer);
	assert(client->out_length > 0);
	assert( ( client->out_offset + client->out_length ) <= client->out_max);
	
	assert(client->handle > 0);
	
	res = send(client->handle, client->out_buffer + client->out_offset, client->out_length, 0);
	if (res > 0) {
		
		assert(res <= client->out_length);
		if (res == client->out_length) {
			client->out_offset = 0;
		}
		else {
			client->out_offset += res;
			client->out_length -= res;
		}

		assert(client->out_length >= 0);
		if (client->out_length == 0) {
			// all data has been sent, so we clear the write event.
			assert(client->write_event);
			event_free(client->write_event);
			client->write_event = NULL;
			assert(client->read_event);
		}
	}
	else if (res == 0 || (errno != EAGAIN && errno != EWOULDBLOCK)) {
		// the connection has closed, so we need to clean up.
		client_free(client);
	}
}


// This function is called when data is available on the socket.  We need to 
// read the data from the socket, and process as much of it as we can.  We 
// need to remember that we might possibly have leftover data from previous 
// reads, so we will need to append the new data in that case.
static void read_handler(int fd, short int flags, void *arg)
{
	client_t *client = (client_t *) arg;
	int avail;
	int res;
	
	assert(fd >= 0);
	assert(flags != 0);
	assert(client);
	assert(client->handle == fd);
	assert(client->server);

	// Make sure we have room in our inbuffer.
	assert((client->in_length + client->in_offset) <= client->in_max);
	avail = client->in_max - client->in_length - client->in_offset;
	if (avail < DEFAULT_BUFSIZE) {
		/* we want to increase buffer size, so we'll add another DEFAULT_BUFSIZE to the 
		   max.  This should keep it in multiples of DEFAULT_BUFSIZE, regardless of how 
		   much is available for each read.
		*/
		
		client->in_buffer = realloc(client->in_buffer, client->in_max + DEFAULT_BUFSIZE);
		client->in_max += DEFAULT_BUFSIZE;
		avail += DEFAULT_BUFSIZE;
	}
	assert(avail >= DEFAULT_BUFSIZE);
	
	// read data from the socket.
	assert(client->in_buffer);
	res = read(fd, client->in_buffer + client->in_offset, avail);
	if (res > 0) {
		
		// got some data.
		assert(res <= avail);
		client->in_length += res;
		

		/// ***** this is where we need to process the data.
	}
	else {
		// the connection was closed, or there was an error.
// 		printf("socket %d closed. res=%d, errno=%d,'%s'\n", fd, res, errno, strerror(errno));
		
		// free the client resources.
		client_free(client);
		client = NULL;
	}
}




//-----------------------------------------------------------------------------
// print some info to the user, so that they can know what the parameters do.
static void usage(void) {
	printf(PACKAGE " " VERSION "\n");
	printf("-l <ip_addr:port>  interface to listen on, default is INDRR_ANY\n");
	printf("-c <num>           max simultaneous connections, default is 1024\n");
	printf("-b <path>          storage path\n");
	printf("-m <max>           max storage file size (in mb) before splitting (default: 1024).\n");
	printf("-t <threshold>     data threshold.\n");
	printf("\n");
	printf("-d                 run as a daemon\n");
	printf("-P <file>          save PID in <file>, only used with -d option\n");
	printf("-u <username>      assume identity of <username> (only when run as root)\n");
	printf("\n");
	printf("-v                 verbose (print errors/warnings while in event loop)\n");
	printf("-h                 print this help and exit\n");
	return;
}



static void parse_params(int argc, char **argv)
{
	int c;
	
	assert(argc >= 0);
	assert(argv);
	
	// process arguments
	/// Need to check the options in here, there're possibly ones that we dont need.
	while ((c = getopt(argc, argv, 
		"c:"    /* max connections. */
		"h"     /* help */
		"v"     /* verbosity */
		"d"     /* daemon */
		"u:"    /* user to run as */
		"P:"    /* PID file */
		"l:"    /* interfaces to listen on */
		)) != -1) {
		switch (c) {
			case 'c':
				_maxconns = atoi(optarg);
				assert(_maxconns > 0);
				break;
			case 'h':
				usage();
				exit(EXIT_SUCCESS);
			case 'v':
				_verbose++;
				break;
			case 'd':
				assert(_daemonize == 0);
				_daemonize = 1;
				break;
			case 'u':
				assert(_username == NULL);
				_username = strdup(optarg);
				assert(_username != NULL);
				assert(_username[0] != 0);
				break;
			case 'P':
				assert(_pid_file == NULL);
				_pid_file = strdup(optarg);
				assert(_pid_file != NULL);
				assert(_pid_file[0] != 0);
				break;
			case 'l':
				_interfaces = optarg;
				assert(_interfaces != NULL);
				assert(_interfaces[0] != 0);
				break;
				
			default:
				fprintf(stderr, "Illegal argument \"%c\"\n", c);
				return;
				
		}
	}
}


void daemonize(const char *username, const char *pidfile, const int noclose)
{
	struct passwd *pw;
	struct sigaction sa;
	int fd;
	FILE *fp;
	
	if (getuid() == 0 || geteuid() == 0) {
		if (username == 0 || *username == '\0') {
			fprintf(stderr, "can't run as root without the -u switch\n");
			exit(EXIT_FAILURE);
		}
		assert(username);
		pw = getpwnam((const char *)username);
		if (pw == NULL) {
			fprintf(stderr, "can't find the user %s to switch to\n", username);
			exit(EXIT_FAILURE);
		}
		if (setgid(pw->pw_gid) < 0 || setuid(pw->pw_uid) < 0) {
			fprintf(stderr, "failed to assume identity of user %s\n", username);
			exit(EXIT_FAILURE);
		}
	}
	
	sa.sa_handler = SIG_IGN;
	sa.sa_flags = 0;
	if (sigemptyset(&sa.sa_mask) == -1 || sigaction(SIGPIPE, &sa, 0) == -1) {
		perror("failed to ignore SIGPIPE; sigaction");
		exit(EXIT_FAILURE);
	}
	
	switch (fork()) {
		case -1:
			exit(EXIT_FAILURE);
		case 0:
			break;
		default:
			_exit(EXIT_SUCCESS);
	}
	
	if (setsid() == -1)
		exit(EXIT_FAILURE);
	
	(void)chdir("/");
	
	if (noclose == 0 && (fd = open("/dev/null", O_RDWR, 0)) != -1) {
		(void)dup2(fd, STDIN_FILENO);
		(void)dup2(fd, STDOUT_FILENO);
		(void)dup2(fd, STDERR_FILENO);
		if (fd > STDERR_FILENO)
			(void)close(fd);
	}
	
	// save the PID in if we're a daemon, do this after thread_init due to a
	// file descriptor handling bug somewhere in libevent
	if (pidfile != NULL) {
		if ((fp = fopen(pidfile, "w")) == NULL) {
			fprintf(stderr, "Could not open the pid file %s for writing\n", pidfile);
			exit(EXIT_FAILURE);
		}
		
		fprintf(fp,"%ld\n", (long)getpid());
		if (fclose(fp) == -1) {
			fprintf(stderr, "Could not close the pid file %s.\n", pidfile);
			exit(EXIT_FAILURE);
		}
	}
	
	
}



//-----------------------------------------------------------------------------
// Main... process command line parameters, and then setup our listening 
// sockets and event loop.
int main(int argc, char **argv) 
{

///============================================================================
/// Initialization.
///============================================================================

	parse_params(argc, argv);
	
	// daemonize
	if (_daemonize) {
		daemonize(_username, _pid_file, _verbose);
	}
	
	// create our event base which will be the pivot point for pretty much everything.
#if ( _EVENT_NUMERIC_VERSION >= 0x02000000 )
	assert(event_get_version_number() == LIBEVENT_VERSION_NUMBER);
#endif
	
	_evbase = event_base_new();
	assert(_evbase);

	// initialise signal handlers.
	assert(_evbase);
	_sigint_event = evsignal_new(_evbase, SIGINT, sigint_handler, NULL);
// 	_sighup_event = evsignal_new(_evbase, SIGHUP, sighup_handler, NULL);
	assert(_sigint_event);
// 	assert(_sighup_event);
	event_add(_sigint_event, NULL);
// 	event_add(_sighup_event, NULL);
	
	


	// initialise the servers that we listen on.
	init_servers();
	

///============================================================================
/// Main Event Loop.
///============================================================================

	// enter the processing loop.  This function will not return until there is
	// nothing more to do and the service has shutdown.  Therefore everything
	// needs to be setup and running before this point.  Once inside the
	// rq_process function, everything is initiated by the RQ event system.
	if (_verbose > 0) { printf("Starting main loop.\n"); }
	assert(_evbase);
	event_base_dispatch(_evbase);

///============================================================================
/// Shutdown
///============================================================================

	if (_verbose > 0) { printf("Freeing the event base.\n"); }
	assert(_evbase);
	event_base_free(_evbase);
	_evbase = NULL;

	// cleanup the servers objects.
	if (_verbose > 0) { printf("Cleaning up servers.\n"); }
	cleanup_servers();

	// make sure signal handlers have been cleared.
	assert(_sigint_event == NULL);
	assert(_sighup_event == NULL);

	
	// we are done, cleanup what is left in the control structure.
	if (_verbose > 0) { printf("Final cleanup.\n"); }
	
	
	_interfaces = NULL;
	
	if (_username) { free((void*)_username); _username = NULL; }
	if (_pid_file) { free((void*)_pid_file); _pid_file = NULL; }
	
	assert(_conncount == 0);
	assert(_sigint_event == NULL);
	assert(_sighup_event == NULL);
	assert(_evbase == NULL);
	
	return 0;
}


