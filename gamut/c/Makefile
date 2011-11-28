## make file for gamut

all: gamut

DEBUG_LIBS=
#DEBUG_LIBS=-lefence -lpthread

ARGS=-Wall -O2 -g $(DEBUG_LIBS)
LIBS=-lopencluster
OBJS=




gamut: gamut.c $(OBJS)
	gcc `pkg-config --cflags --libs glib-2.0` -o $@ gamut.c $(OBJS) $(LIBS) $(ARGS)



# shared objects

#event-compat.o: event-compat.c event-compat.h
#	gcc -c -o $@ event-compat.c $(ARGS)


install: gamut 
	@cp gamut /usr/sbin/

clean:
	@-rm gamut 
	@-rm $(OBJS)

