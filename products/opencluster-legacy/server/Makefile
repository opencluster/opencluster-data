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
LIBS=`pkg-config --libs libevent jansson glib-2.0 conninfo`

OBJS=\
	auth.o \
	bucket.o bucket_data.o \
	client.o commands.o config.o \
	daemon.o data.o \
	event-compat.o \
	hashfn.o \
	item.o \
	node.o \
	params.o payload.o process.o push.o \
	seconds.o server.o stats.o shutdown.o \
	timeout.o \
	usage.o \
	value.o \
	ocd.o

# we define the headers and their dependencies.
H_CONFIG=config.h
H_CONNECTIONS=connections.h
H_DATA=data.h
H_AUTH=auth.h
H_USAGE=usage.h
H_DAEMON=daemon.h
H_PARAMS=params.h
H_HASH=hash.h
H_HASHFN=hashfn.h $(H_HASH)
H_VALUE=value.h
H_ITEM=item.h $(H_HASH) $(H_VALUE)
H_PROTOCOL=protocol.h
H_CONSTANTS=constants.h
H_SERVER=server.h
H_HEADER=header.h
H_PAYLOAD=payload.h
H_CLIENT=client.h event-compat.h $(H_HEADER) $(H_HASH) $(H_PAYLOAD)
H_NODE=node.h $(H_CLIENT) $(H_HASH)
H_BUCKET_DATA=bucket_data.h $(H_VALUE) $(H_HASH) $(H_ITEM) $(H_CLIENT) $(H_CONSTANTS) $(H_NODE)
H_BUCKET=bucket.h $(H_HASH) $(H_NODE) $(H_BUCKET_DATA) $(H_VALUE)
H_PUSH=push.h $(H_CLIENT) $(H_ITEM)
H_STATS=stats.h
H_SECONDS=seconds.h event-compat.h
H_PROCESS=process.h $(H_CLIENT) $(H_HEADER)
H_COMMANDS=commands.h 
H_TIMEOUT=timeout.h
H_SHUTDOWN=shutdown.h

# set the header includes here for each c file, because we need to keep them in sync over the release/debug versions.

INC_AUTH=\
	$(H_AUTH)

INC_BUCKET_DATA=\
	$(H_BUCKET_DATA) \
	$(H_BUCKET) \
	$(H_HASH) \
	$(H_ITEM) \
	$(H_PUSH) \
	$(H_SECONDS) \
	$(H_STATS)

INC_BUCKET= \
	$(H_AUTH) \
	$(H_BUCKET) \
	$(H_CONSTANTS) \
	$(H_ITEM) \
	$(H_PUSH) \
	$(H_TIMEOUT) \
	$(H_STATS) \
	$(H_SERVER)

INC_CLIENT= \
	$(H_BUCKET) \
	$(H_CLIENT) \
	$(H_COMMANDS) \
	$(H_CONSTANTS) \
	$(H_HEADER) \
	$(H_NODE) \
	$(H_PROCESS) \
	$(H_PROTOCOL) \
	$(H_PUSH) \
	$(H_TIMEOUT) \
	$(H_SERVER) \
	$(H_STATS)

INC_COMMANDS= \
	$(H_BUCKET) \
	$(H_CLIENT) \
	$(H_COMMANDS) \
	$(H_HASHFN) \
	$(H_HEADER) \
	$(H_PAYLOAD) \
	$(H_PROTOCOL) \
	$(H_PUSH) \
	$(H_SERVER) \
	$(H_TIMEOUT) \
	$(H_VALUE) 
	
INC_CONFIG=$(H_CONFIG)
	
INC_CONNECTIONS=$(H_CONNECTIONS)

INC_DAEMON=$(H_DAEMON)

INC_DATA=$(H_DATA)

INC_HASHFN=$(H_HASHFN)

INC_ITEM=$(H_ITEM)

INC_NODE= \
	event-compat.h \
	$(H_NODE) \
	$(H_TIMEOUT) \
	$(H_PUSH) \
	$(H_SERVER) \
	$(H_STATS)

INC_OCD= \
	$(H_AUTH) \
	$(H_BUCKET) \
	$(H_CONSTANTS) \
	$(H_DAEMON) \
	$(H_ITEM) \
	$(H_PARAMS) \
	$(H_PAYLOAD) \
	$(H_SECONDS) \
	$(H_SERVER) \
	$(H_SHUTDOWN) \
	$(H_STATS) \
	$(H_TIMEOUT) \
	$(H_USAGE)

INC_PARAMS= $(H_PARAMS)
	
INC_PAYLOAD=$(H_PAYLOAD)

INC_PROCESS= \
	$(H_BUCKET) \
	$(H_CONSTANTS) \
	$(H_ITEM) \
	$(H_NODE) \
	$(H_PAYLOAD) \
	$(H_PROCESS) \
	$(H_PROTOCOL) \
	$(H_PUSH) \
	$(H_SERVER)

INC_PUSH= \
	$(H_PUSH) \
	$(H_PROTOCOL) \
	$(H_PAYLOAD) \
	$(H_NODE) \
	$(H_SERVER) \
	$(H_CLIENT) \
	$(H_SECONDS)

INC_SECONDS= \
	$(H_SECONDS) \
	event-compat.h \
	$(H_TIMEOUT) 

INC_SERVER= \
	event-compat.h \
	$(H_SERVER) \
	$(H_CLIENT) 

INC_SHUTDOWN= \
	$(H_SHUTDOWN) \
	$(H_BUCKET) \
	$(H_NODE) \
	$(H_SERVER) \
	$(H_SECONDS) \
	$(H_STATS) 

INC_STATS= \
	$(H_STATS) \
	event-compat.h \
	$(H_NODE) \
	$(H_TIMEOUT) \
	$(H_BUCKET)

INC_TIMEOUT=$(H_TIMEOUT)

INC_USAGE=$(H_USAGE) \
	$(H_CONSTANTS)

INC_VALUE=$(H_VALUE)


ocd: heading $(OBJS)
	gcc -o $(OUTPUT_FILE) $(OBJS) $(LIBS) $(DEBUG_LIBS) $(ARGS)

heading: 
	@echo "---------------------------------------------------------------------------------"

auth.o: auth.c $(INC_AUTH)
	gcc -c -o $@ auth.c $(DEBUG_ARGS) $(ARGS)

bucket.o: bucket.c $(INC_BUCKET)
	gcc `pkg-config --cflags glib-2.0` -c -o $@ bucket.c $(DEBUG_ARGS) $(ARGS)

bucket_data.o: bucket_data.c $(INC_BUCKET_DATA)
	gcc `pkg-config --cflags glib-2.0` -c -o $@ bucket_data.c $(DEBUG_ARGS) $(ARGS)

client.o: client.c $(INC_CLIENT)
	gcc `pkg-config --cflags glib-2.0` -c -o $@ client.c $(DEBUG_ARGS) $(ARGS)

commands.o: commands.c $(INC_COMMANDS)
	gcc `pkg-config --cflags glib-2.0` -c -o $@ commands.c $(DEBUG_ARGS) $(ARGS)

config.o: config.c $(INC_CONFIG)
	gcc -c -o $@ config.c $(DEBUG_ARGS) $(ARGS)

connections.o: connections.c $(INC_CONNECTIONS)
	gcc -c -o $@ connections.c $(DEBUG_ARGS) $(ARGS)

daemon.o: daemon.c $(INC_DAEMON)
	gcc -c -o $@ daemon.c $(DEBUG_ARGS) $(ARGS)

data.o: data.c $(INC_DATA)
	gcc -c -o $@ data.c $(DEBUG_ARGS) $(ARGS)

hashfn.o: hashfn.c $(INC_HASHFN)
	gcc -c -o $@ hashfn.c $(DEBUG_ARGS) $(ARGS)

item.o: item.c $(INC_ITEM)
	gcc -c -o $@ item.c $(DEBUG_ARGS) $(ARGS)

node.o: node.c $(INC_NODE)
	gcc `pkg-config --cflags glib-2.0` -c -o $@ node.c $(DEBUG_ARGS) $(ARGS)

ocd.o: ocd.c event-compat.h $(INC_OCD)
	gcc `pkg-config --cflags glib-2.0` -c -o $@ ocd.c $(DEBUG_ARGS) $(ARGS)

params.o: params.c $(INC_PARAMS)
	gcc -c -o $@ params.c $(DEBUG_ARGS) $(ARGS)

payload.o: payload.c $(INC_PAYLOAD)
	gcc -c -o $@ payload.c $(DEBUG_ARGS) $(ARGS)

process.o: process.c $(INC_PROCESS)
	gcc `pkg-config --cflags glib-2.0` -c -o $@ process.c $(DEBUG_ARGS) $(ARGS)

push.o: push.c $(INC_PUSH)
	gcc `pkg-config --cflags glib-2.0` -c -o $@ push.c $(DEBUG_ARGS) $(ARGS)

seconds.o: seconds.c $(INC_SECONDS)
	gcc `pkg-config --cflags glib-2.0` -c -o $@ seconds.c $(DEBUG_ARGS) $(ARGS)

server.o: server.c $(INC_SERVER)
	gcc `pkg-config --cflags glib-2.0` -c -o $@ server.c $(DEBUG_ARGS) $(ARGS)

shutdown.o: shutdown.c $(INC_SHUTDOWN)
	gcc `pkg-config --cflags glib-2.0` -c -o $@ shutdown.c $(DEBUG_ARGS) $(ARGS)

stats.o: stats.c $(INC_STATS)
	gcc `pkg-config --cflags glib-2.0` -c -o $@ stats.c $(DEBUG_ARGS) $(ARGS)

timeout.o: timeout.c $(INC_TIMEOUT)
	gcc -c -o $@ timeout.c $(DEBUG_ARGS) $(ARGS)

usage.o: usage.c $(INC_USAGE)
	gcc -c -o $@ usage.c $(DEBUG_ARGS) $(ARGS)

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

