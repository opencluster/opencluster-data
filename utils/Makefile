## make file for gamut

all: oc_get oc_set

DEBUG_LIBS=
#DEBUG_LIBS=-lefence -lpthread

ARGS=-Wall -O2 -g $(DEBUG_LIBS)
LIBS=-lopencluster
OBJS=




oc_get: oc_get.c $(OBJS)
	gcc `pkg-config --cflags --libs glib-2.0` -o $@ oc_get.c $(OBJS) $(LIBS) $(ARGS)

oc_set: oc_set.c $(OBJS)
	gcc `pkg-config --cflags --libs glib-2.0` -o $@ oc_set.c $(OBJS) $(LIBS) $(ARGS)



# shared objects

#event-compat.o: event-compat.c event-compat.h
#	gcc -c -o $@ event-compat.c $(ARGS)


install: oc_get oc_set
	@cp oc_get /usr/sbin/
	@cp oc_set /usr/sbin/

clean:
	@-rm oc_get oc_set 
	@-rm $(OBJS)


