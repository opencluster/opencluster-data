## make file for libconninfo.


ARGS=-g -Wall
OBJS=libconninfo.o
MANPATH=/usr/share/man
SO_VER=1.0.1
OBJ_VER=1

all: libconninfo.so.$(SO_VER)
install: install_lib install_dev


libconninfo.o: libconninfo.c conninfo.h
	gcc -c -fPIC libconninfo.c  -o $@ $(ARGS)


libconninfo.a: $(OBJS)
	@>$@
	@rm $@
	ar -r $@
	ar -r $@ $^

libconninfo.so.$(SO_VER): $(OBJS)
	gcc -shared -Wl,-soname,libconninfo.so.$(OBJ_VER) -o libconninfo.so.$(SO_VER) $(OBJS)
	

# will install only the files required to use the library from other applications.
install_lib: libconninfo.so.$(SO_VER)
	@cp libconninfo.so.$(SO_VER) /usr/lib/
	@-test -e /usr/lib/libconninfo.so && rm /usr/lib/libconninfo.so
	ln -s /usr/lib/libconninfo.so.$(SO_VER) /usr/lib/libconninfo.so
	ldconfig

# will install the additional files to make applications that use this library.  
# The library files themselves will also be installed.
install_dev: conninfo.h install_lib conninfo.pc
	@-test -e /usr/include/conninfo.h && rm /usr/include/conninfo.h
	cp conninfo.h /usr/include/
	cp conninfo.pc /usr/lib/pkgconfig/
#	@for i in manpages/*.3; do gzip -c $$i > $(MANPATH)/man3/$$i.gz; done


uninstall: 
	rm /usr/include/conninfo.h
	rm /usr/lib/libconninfo.so.$(SO_VER)
	rm /usr/lib/libconninfo.so.1
	rm /usr/lib/libconninfo.so
	


clean:
	@-[ -e libconninfo.o ] && rm libconninfo.o
	@-[ -e libconninfo.so* ] && rm libconninfo.so*
	@-rm manpages/*.3.gz
	
