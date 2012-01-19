## make file for ocd (open cluster daemon)

all: ocd
debug: ocd.debug

DEBUG_LIBS=
#DEBUG_LIBS=-lefence -lpthread

ARGS=-Wall -O2 -g $(DEBUG_LIBS)
LIBS=-levent 
OBJS=event-compat.o queue.o

DEBUG_OBJS=event-compat.debug.o queue.debug.o



ocd: ocd.c $(OBJS) event-compat.h queue.h
	gcc `pkg-config --cflags --libs glib-2.0` -o $@ ocd.c -DNDEBUG $(OBJS) $(LIBS) $(ARGS)

ocd.debug: ocd.c $(DEBUG_OBJS) event-compat.h queue.h
	gcc `pkg-config --cflags --libs glib-2.0` -o $@ ocd.c $(DEBUG_OBJS) $(LIBS) $(ARGS)



# shared objects

queue.o: queue.c queue.h
	gcc -c -o $@ queue.c -DNDEBUG $(ARGS)

queue.debug.o: queue.c queue.h
	gcc -c -o $@ queue.c $(ARGS)

event-compat.o: event-compat.c event-compat.h
	gcc -c -o $@ event-compat.c -DNDEBUG $(ARGS)

event-compat.debug.o: event-compat.c event-compat.h
	gcc -c -o $@ event-compat.c $(ARGS)


install: ocd 
	@cp ocd /usr/sbin/

clean:
	@-rm ocd ocd.debug
	@-rm $(OBJS)
	@-rm $(DEBUG_OBJS)

