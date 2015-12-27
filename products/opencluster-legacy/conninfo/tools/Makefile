## make file for gamut

ALL=conninfo_pack conninfo_get conninfo_set
all: $(ALL)

DEBUG_LIBS=
#DEBUG_LIBS=-lefence -lpthread

ARGS=-Wall -O2 -g $(DEBUG_LIBS)
LIBS=`pkg-config --cflags --libs conninfo`
OBJS=




conninfo_get: conninfo_get.c $(OBJS)
	gcc  -o $@ conninfo_get.c $(OBJS) $(LIBS) $(ARGS)

conninfo_pack: conninfo_pack.c $(OBJS)
	gcc  -o $@ conninfo_pack.c $(OBJS) $(LIBS) $(ARGS)

conninfo_set: conninfo_set.c $(OBJS)
	gcc  -o $@ conninfo_set.c $(OBJS) $(LIBS) $(ARGS)


# shared objects

#event-compat.o: event-compat.c event-compat.h
#	gcc -c -o $@ event-compat.c $(ARGS)


install: $(ALL)
	@cp conninfo_pack /usr/sbin/
	@cp conninfo_get  /usr/sbin/
	@cp conninfo_set  /usr/sbin/
	

clean:
	@-rm $(ALL) 
	@-rm $(OBJS)

