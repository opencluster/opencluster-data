// event compatability.

#ifndef __EVENT_COMPAT_H
#define __EVENT_COMPAT_H

/******************************************************************************
 * 
 * Version 1.0
 * 
 * This compatibility wrapper is intended to allow programs that were written 
 * for libevent2.0 to be compiled and usable against libevent1.x.   It does not 
 * provide full functionality, but at least provides some of the things that 
 * are needed.
 * 
 * Also, it is not proided as a library, because often the applications that 
 * must use 1.x are probably on systems that cannot have extra libraries 
 * installed (due to lockdown restrictions), and so all this compatibility 
 * wrapping should be compiled in directly.
 * 
 * 
 *****************************************************************************/






#include <arpa/inet.h>

#include <event.h>

#if ( _EVENT_NUMERIC_VERSION < 0x02000000 )

#define LEV_OPT_CLOSE_ON_FREE 1
#define LEV_OPT_REUSEABLE 2


typedef int evutil_socket_t;

struct event * event_new(struct event_base *evbase, evutil_socket_t sfd, short flags, void (*fn)(int, short, void *), void *arg);
void event_free(struct event *ev);
struct event * evsignal_new(struct event_base *evbase, int sig, void (*fn)(int, short, void *), void *arg);
struct event * evtimer_new(struct event_base *evbase, void (*fn)(int, short, void *), void *arg);
int evutil_parse_sockaddr_port(const char *ip_as_string, struct sockaddr *out, int *outlen);

struct evconnlistener {
	evutil_socket_t handle;
	struct event *listen_event;
	void (*fn)(struct evconnlistener *, int, struct sockaddr *, int, void *);
	void *arg;
};

struct evconnlistener * evconnlistener_new_bind(struct event_base *evbase, void (*fn)(struct evconnlistener *, int, struct sockaddr *, int, void *), void *arg, int flags, int queues, struct sockaddr *sin, int slen );
void evconnlistener_free(struct evconnlistener * listener);


#else
	#include <event2/listener.h>
#endif



#endif
