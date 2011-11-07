## make file for ocd (open cluster daemon)

all: ocd

DEBUG_LIBS=
#DEBUG_LIBS=-lefence -lpthread

ARGS=-Wall -O2 -g $(DEBUG_LIBS)
LIBS=-levent 
OBJS=event-compat.o




ocd: ocd.c $(OBJS) event-compat.h
	gcc -o $@ ocd.c $(OBJS) $(LIBS) $(ARGS)



# shared objects

event-compat.o: event-compat.c event-compat.h
	gcc -c -o $@ event-compat.c $(ARGS)


install: ocd 
	@cp ocd /usr/sbin/

clean:
	@-rm ocd 
	@-rm $(OBJS)

