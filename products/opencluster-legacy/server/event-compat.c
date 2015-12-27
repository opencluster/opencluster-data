/* 
 * event-compat is a wedge object used to provide missing functionality from commonly installed 
 * older version of libevent.  It is quite common now for libevent2 to be available, but on some 
 * older systems libevent 1.x is the only one available.  It is a compile-time option and 
 * therefore the product will need to be compiled on a system with the same libevent libraries as 
 * the machine it will be running on.
*/

#include "event-compat.h"

#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>




#if ( _EVENT_NUMERIC_VERSION < 0x02000000 )

// event_new is a 2.0 function that creates a new event, sets it and then returns the pointer to the new event structure.   1.4 does not have that function, so we wrap an event_set here instead.
struct event * event_new(struct event_base *evbase, evutil_socket_t sfd, short flags, void (*fn)(int, short, void *), void *arg) {
        struct event *ev;

        assert(evbase && sfd >= 0 && flags != 0 && fn);
        ev = calloc(1, sizeof(*ev));
        assert(ev);
        event_set(ev, sfd, flags, fn, arg);
        event_base_set(evbase, ev);
        
        return(ev);
}

void event_free(struct event *ev)
{
        assert(ev);
        event_del(ev);
        free(ev);
}

struct event * evsignal_new(struct event_base *evbase, int sig, void (*fn)(int, short, void *), void *arg)
{
        struct event *ev;
        ev = event_new(evbase, sig, EV_SIGNAL|EV_PERSIST, fn, arg);
        assert(ev);
        return(ev);
}


struct event * evtimer_new(struct event_base *evbase, void (*fn)(int, short, void *), void *arg)
{
	struct event *ev;
	
	assert(evbase && fn);
	ev = calloc(1, sizeof(*ev));
	assert(ev);
	event_set(ev, -1, EV_TIMEOUT, fn, arg);
	event_base_set(evbase, ev);
	
	return(ev);
}


// pulled in from libevent 2.0.3 (alpha) to add compatibility for older libevents.
int evutil_parse_sockaddr_port(const char *ip_as_string, struct sockaddr *out, int *outlen) {
	int port;
	char buf[128];
	const char *cp, *addr_part, *port_part;
	int is_ipv6;
	/* recognized formats are:
	 * [ipv6]:port
	 * ipv6
	 * [ipv6]
	 * ipv4:port
	 * ipv4
	 */
	
	cp = strchr(ip_as_string, ':');
	if (*ip_as_string == '[') {
		int len;
		if (!(cp = strchr(ip_as_string, ']'))) { return -1; }
		len = cp-(ip_as_string + 1);
		if (len > (int)sizeof(buf)-1) { return -1; }
		memcpy(buf, ip_as_string+1, len);
		buf[len] = '\0';
		addr_part = buf;
		if (cp[1] == ':') port_part = cp+2;
			else port_part = NULL;
			is_ipv6 = 1;
	} else if (cp && strchr(cp+1, ':')) {
		is_ipv6 = 1;
		addr_part = ip_as_string;
		port_part = NULL;
	} else if (cp) {
		is_ipv6 = 0;
		if (cp - ip_as_string > (int)sizeof(buf)-1) { return -1; }
		memcpy(buf, ip_as_string, cp-ip_as_string);
		buf[cp-ip_as_string] = '\0';
	addr_part = buf;
	port_part = cp+1;
	} else {
		addr_part = ip_as_string;
		port_part = NULL;
		is_ipv6 = 0;
	}
	
	if (port_part == NULL) { port = 0; } 
	else {
		port = atoi(port_part);
		if (port <= 0 || port > 65535) { return -1; }
	}
	
	if (!addr_part) return -1; /* Should be impossible. */
		
		struct sockaddr_in sin;
	memset(&sin, 0, sizeof(sin));
	sin.sin_family = AF_INET;
	sin.sin_port = htons(port);
	if (1 != inet_pton(AF_INET, addr_part, &sin.sin_addr)) return -1;
			   if (sizeof(sin) > *outlen) return -1;
			   memset(out, 0, *outlen);
	memcpy(out, &sin, sizeof(sin));
	*outlen = sizeof(sin);
	return 0;
}

// static void accept_conn_cb_old(evutil_socket_t fd, const short flags, void *ctx);


static void evconn_cb(int ls_sfd, short flags, void *arg)
{
	struct evconnlistener *listener;
	socklen_t addrlen;
	struct sockaddr addr;
	evutil_socket_t sfd;
	
	assert(ls_sfd >= 0);
	assert(flags == EV_READ);
	assert(arg);
	
	listener = arg;
	assert(listener->handle >= 0);
	assert(listener->handle == ls_sfd);
	
	addrlen = sizeof(addr);
	if ((sfd = accept(listener->handle, (struct sockaddr *)&addr, &addrlen)) == -1) {
		// somehow the accept failed... need to handle it gracefully.
		assert(0);
	}
	else {
		// 		void (*fn)(struct evconnlistener *, int, struct sockaddr *, int, void *ctx)
		assert(listener->fn);
		assert(listener->arg);
		listener->fn(listener, sfd, &addr, addrlen, listener->arg);
	}
}



// simulate the listener stuff from libevent2.
// static void accept_conn_cb(struct evconnlistener *listener, evutil_socket_t fd, struct sockaddr *address, int socklen, void *ctx);
struct evconnlistener * evconnlistener_new_bind(struct event_base *evbase, void (*fn)(struct evconnlistener *, int, struct sockaddr *, int, void *), void *arg, int flags, int queues, struct sockaddr *sin, int slen )
{
	struct evconnlistener *listener = NULL;
	int sfd;
	int error;
	int sockflags =1;
	struct linger ling = {0, 0};
	
	assert(evbase && fn);
	assert(flags == (LEV_OPT_CLOSE_ON_FREE|LEV_OPT_REUSEABLE));
	assert(sin && slen > 0);
	
	sfd = socket(AF_INET, SOCK_STREAM, 0);
	
	setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, (void *)&sockflags, sizeof(sockflags));
	error = setsockopt(sfd, SOL_SOCKET, SO_KEEPALIVE, (void *)&sockflags, sizeof(sockflags));
	assert(error == 0);
	error = setsockopt(sfd, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling));
	assert(error == 0);
	#ifdef TCP_NODELAY
		error = setsockopt(sfd, IPPROTO_TCP, TCP_NODELAY, (void *)&sockflags, sizeof(sockflags));
		assert(error == 0);
	#endif
		
	if (bind(sfd, sin, slen) == -1) {
		close(sfd);
		sfd = -1;
		assert(listener == NULL);
	} 
	else {
		if (listen(sfd, 5) == -1) {
			close(sfd);
			sfd = -1;
			assert(listener == NULL);
		}
		else {
			// we've got a listener.
			
			listener = calloc(1, sizeof(*listener));
			assert(listener);
			
			listener->handle = sfd;
			listener->listen_event = event_new(evbase, sfd, EV_READ | EV_PERSIST, evconn_cb, (void *)listener);
			event_add(listener->listen_event, NULL);
			listener->fn = fn;
			listener->arg = arg;
		}
	}
	
	return(listener);
}


void evconnlistener_free(struct evconnlistener * listener)
{
	assert(listener);
	assert(listener->handle >= 0);
	
	if(listener->handle >= 0) {
		assert(listener->listen_event);
		event_del(listener->listen_event);
		close(listener->handle);
		listener->handle = -1;
	}
	
	free(listener);	
}








#endif

