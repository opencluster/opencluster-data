## make file for ocd (open cluster daemon)

all: ocd

OUTPUT_FILE=ocd
DEBUG_ARGS=-DNDEBUG

debug: OUTPUT_FILE=ocd.debug
debug: DEBUG_ARGS=-g
debug: ocd



DEBUG_LIBS=
#DEBUG_LIBS=-lefence -lpthread

ARGS=-Wall -O2
LIBS=-levent 

OBJS=\
	bucket.o bucket_data.o\
	client.o commands.o \
	event-compat.o \
	item.o \
	node.o \
	payload.o process.o push.o \
	seconds.o server.o settle.o stats.o \
	timeout.o \
	value.o \
	ocd.o

# we define the headers and their dependencies.
H_HASH=hash.h
H_VALUE=value.h
H_ITEM=item.h $(H_HASH) $(H_VALUE)
H_PROTOCOL=protocol.h
H_CONSTANTS=constants.h
H_SERVER=server.h
H_HEADER=header.h
H_CLIENT=client.h event-compat.h $(H_HEADER)
H_NODE=node.h $(H_CLIENT)
H_BUCKET_DATA=bucket_data.h $(H_VALUE) $(H_HASH) $(H_ITEM) $(H_CLIENT) $(H_CONSTANTS)
H_BUCKET=bucket.h $(H_HASH) $(H_NODE) $(H_BUCKET_DATA) $(H_VALUE)
H_PUSH=push.h $(H_CLIENT) $(H_BUCKET)
H_GLOBALS=globals.h event-compat.h
H_STATS=stats.h
H_SECONDS=seconds.h
H_PROCESS=process.h $(H_CLIENT) $(H_HEADER)
H_COMMANDS=commands.h $(H_CLIENT) $(H_HEADER)
H_PAYLOAD=payload.h
H_TIMEOUT=timeout.h
H_SETTLE=settle.h

# set the header includes here, because we need to keep them in sync over the release/debug versions.
INC_BUCKET_DATA=\
	$(H_BUCKET_DATA) \
	$(H_BUCKET) \
	$(H_HASH) \
	$(H_ITEM) \
	$(H_GLOBALS) \
	$(H_PUSH)

INC_BUCKET= \
	$(H_BUCKET) \
	$(H_ITEM) \
	$(H_GLOBALS) \
	$(H_PUSH) \
	$(H_TIMEOUT)

INC_CLIENT= \
	$(H_CLIENT) \
	$(H_CONSTANTS) \
	$(H_GLOBALS) \
	$(H_NODE) \
	$(H_PUSH) \
	$(H_PROTOCOL) \
	$(H_HEADER) \
	$(H_PROCESS) \
	$(H_COMMANDS) \
	$(H_TIMEOUT)

INC_COMMANDS= \
	$(H_COMMANDS) \
	$(H_VALUE) \
	$(H_GLOBALS) \
	$(H_BUCKET) \
	$(H_PAYLOAD) \
	$(H_PROTOCOL) \
	$(H_PUSH) \
	$(H_TIMEOUT)

INC_ITEM=$(H_ITEM)

INC_NODE= \
	$(H_NODE) \
	event-compat.h \
	$(H_GLOBALS) \
	$(H_TIMEOUT) \
	$(H_PUSH)

INC_OCD= \
	$(H_ITEM) \
	$(H_CONSTANTS) \
	$(H_BUCKET) \
	$(H_TIMEOUT) \
	$(H_SERVER) \
	$(H_SECONDS) \
	$(H_STATS) \
	$(H_SETTLE) \
	$(H_PAYLOAD)

INC_PAYLOAD=$(H_PAYLOAD)

INC_PROCESS= \
	$(H_PROCESS) \
	$(H_PROTOCOL) \
	$(H_NODE) \
	$(H_GLOBALS) \
	$(H_BUCKET) \
	$(H_PAYLOAD) \
	$(H_CONSTANTS) \
	$(H_PUSH) \
	$(H_ITEM) \
	$(H_BUCKET_DATA)

INC_PUSH= \
	$(H_PUSH) \
	$(H_PROTOCOL) \
	$(H_PAYLOAD) \
	$(H_GLOBALS) \
	$(H_NODE) \
	$(H_SERVER) \
	$(H_CLIENT)

INC_SECONDS= \
	$(H_SECONDS) \
	event-compat.h \
	$(H_TIMEOUT) \
	$(H_GLOBALS)

INC_SERVER= \
	$(H_SERVER) \
	event-compat.h \
	$(H_GLOBALS) \
	$(H_CLIENT)

INC_SETTLE= \
	$(H_SETTLE) \
	event-compat.h \
	$(H_GLOBALS) \
	$(H_NODE) \
	$(H_CONSTANTS) \
	$(H_BUCKET) \
	$(H_TIMEOUT)

INC_STATS= \
	$(H_STATS) \
	event-compat.h \
	$(H_NODE) \
	$(H_GLOBALS) \
	$(H_TIMEOUT)

INC_TIMEOUT=$(H_TIMEOUT)

INC_VALUE=$(H_VALUE)


ocd: $(OBJS)
	gcc `pkg-config --libs glib-2.0` -o $(OUTPUT_FILE) $(OBJS) $(LIBS) $(DEBUG_LIBS) $(ARGS)



bucket.o: bucket.c $(INC_BUCKET)
	gcc `pkg-config --cflags glib-2.0` -c -o $@ bucket.c $(DEBUG_ARGS) $(ARGS)

bucket_data.o: bucket_data.c $(INC_BUCKET_DATA)
	gcc `pkg-config --cflags glib-2.0` -c -o $@ bucket_data.c $(DEBUG_ARGS) $(ARGS)

client.o: client.c $(INC_CLIENT)
	gcc `pkg-config --cflags glib-2.0` -c -o $@ client.c $(DEBUG_ARGS) $(ARGS)

commands.o: commands.c $(INC_COMMANDS)
	gcc `pkg-config --cflags glib-2.0` -c -o $@ commands.c $(DEBUG_ARGS) $(ARGS)

item.o: item.c $(INC_ITEM)
	gcc -c -o $@ item.c $(DEBUG_ARGS) $(ARGS)

node.o: node.c $(INC_NODE)
	gcc `pkg-config --cflags glib-2.0` -c -o $@ node.c $(DEBUG_ARGS) $(ARGS)

ocd.o: ocd.c event-compat.h $(INC_OCD)
	gcc `pkg-config --cflags glib-2.0` -c -o $@ ocd.c $(DEBUG_ARGS) $(ARGS)

payload.o: payload.c $(INC_PAYLOAD)
	gcc -c -o $@ payload.c $(DEBUG_ARGS) $(ARGS)

process.o: process.c $(INC_PROCESS)
	gcc `pkg-config --cflags glib-2.0` -c -o $@ process.c $(DEBUG_ARGS) $(ARGS)

push.o: push.c $(INC_PUSH)
	gcc `pkg-config --cflags glib-2.0` -c -o $@ push.c $(DEBUG_ARGS) $(ARGS)

seconds.o: seconds.c $(INC_SECONDS)
	gcc `pkg-config --cflags glib-2.0` -c -o $@ seconds.c $(DEBUG_ARGS) $(ARGS)

settle.o: settle.c $(INC_SETTLE)
	gcc `pkg-config --cflags glib-2.0` -c -o $@ settle.c $(DEBUG_ARGS) $(ARGS)

server.o: server.c $(INC_SERVER)
	gcc `pkg-config --cflags glib-2.0` -c -o $@ server.c $(DEBUG_ARGS) $(ARGS)

stats.o: stats.c $(INC_STATS)
	gcc -c -o $@ stats.c $(DEBUG_ARGS) $(ARGS)

timeout.o: timeout.c $(INC_TIMEOUT)
	gcc -c -o $@ timeout.c $(DEBUG_ARGS) $(ARGS)

value.o: value.c $(INC_VALUE)
	gcc `pkg-config --cflags glib-2.0` -c -o $@ value.c $(DEBUG_ARGS) $(ARGS)





# shared objects

queue.o: queue.c queue.h
	gcc -c -o $@ queue.c $(DEBUG_ARGS) $(ARGS)

event-compat.o: event-compat.c event-compat.h
	gcc -c -o $@ event-compat.c $(DEBUG_ARGS) $(ARGS)


install: ocd 
	@cp ocd /usr/sbin/

clean:
	@-rm ocd ocd.debug
	@-rm $(OBJS)
	@-rm $(DEBUG_OBJS)

