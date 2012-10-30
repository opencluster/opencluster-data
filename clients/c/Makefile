## make file for libopencluster.


ARGS=-g -Wall
OBJS=libopencluster.o
MANPATH=/usr/share/man
SO_VER=1.0.1
OBJ_VER=1

all: libopencluster.so.$(SO_VER)
install: install_lib install_dev


libopencluster.o: libopencluster.c opencluster.h
	gcc -c -fPIC libopencluster.c  -o $@ $(ARGS)


libopencluster.a: $(OBJS)
	@>$@
	@rm $@
	ar -r $@
	ar -r $@ $^

libopencluster.so.$(SO_VER): $(OBJS)
	gcc -shared -Wl,-soname,libopencluster.so.$(OBJ_VER) -o libopencluster.so.$(SO_VER) $(OBJS)
	

# will install only the files required to use the library from other applications.
install_lib: libopencluster.so.$(SO_VER)
	@cp libopencluster.so.$(SO_VER) /usr/lib/
	@-test -e /usr/lib/libopencluster.so && rm /usr/lib/libopencluster.so
	ln -s /usr/lib/libopencluster.so.$(SO_VER) /usr/lib/libopencluster.so
	ldconfig

# will install the additional files to make applications that use this library.  
# The library files themselves will also be installed.
install_dev: opencluster.h install_lib
	@-test -e /usr/include/opencluster.h && rm /usr/include/opencluster.h
	cp opencluster.h /usr/include/
#	@for i in manpages/*.3; do gzip -c $$i > $(MANPATH)/man3/$$i.gz; done


uninstall: 
	rm /usr/include/opencluster.h
	rm /usr/lib/libopencluster.so.$(SO_VER)
	rm /usr/lib/libopencluster.so.1
	rm /usr/lib/libopencluster.so
	


clean:
	@-[ -e libopencluster.o ] && rm libopencluster.o
	@-[ -e libopencluster.so* ] && rm libopencluster.so*
	@-rm manpages/*.3.gz
	
