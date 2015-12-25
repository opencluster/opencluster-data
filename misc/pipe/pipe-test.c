#include <assert.h>
#include <event.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

// event base for listening on the sockets, and timeout events.
struct event_base *_evbase = NULL;
struct event *command_event = NULL;
struct timeval event_timeout = {5,0};


char *buffer = NULL;
int buffer_len = 0;
int buffer_max = 0;
const int buffer_inc = 512;

static void command_handler(int fd, short int flags, void *arg)
{
	struct event *ev = arg;
	
	assert(fd >= 0);
	assert(flags != 0);

	if (flags & EV_TIMEOUT) {
		printf("command timeout\n");
	}
	else {
		// there is data waiting to be read from the command pipe.  
		// keep reading it into a buffer until the end-of-file is received.
		
		// we initially set read_length to a non-zero value just to start the loop.  After that, a zero will indicate that the end of the file has been reached.
		int read_length = 1;		
		while (read_length > 0) {
		
			// make sure we have some buffer space available.
			int avail = buffer_max - buffer_len;
			if (avail < buffer_inc) {
				buffer = realloc(buffer, buffer_max + buffer_inc + 1);		// plus add space for a possible NULL terminator.
				assert(buffer);
				buffer_max += buffer_inc;
				avail += buffer_inc;
			}

			read_length = read(fd, buffer + buffer_len, avail);
			if (read_length > 0) {
				buffer_len += read_length;
			}
			printf("read %d bytes\n", read_length);
			assert(buffer_len <= buffer_max);
		}
		
		// we have a buffer, now we have to parse it.
		if (buffer_len > 0) {
		
			// first we need to make sure it is null terminated.
			buffer[buffer_len] = 0;

			// now go through it, line by line.
			const char * curLine = buffer;
			while(curLine) {
				char * nextLine = strchr(curLine, '\n');
				if (nextLine) *nextLine = '\0';  // temporarily terminate the current line

				// got a command.
				printf("command=[%s]\n", curLine);
				
				
				if (nextLine) *nextLine = '\n';  // then restore newline-char, just to be tidy    
				curLine = nextLine ? (nextLine+1) : NULL;
			}
		}
		
		buffer_len = 0;
		printf("Command Cycle finished.\n");
		
		// delete the event
		event_del(ev);
	}
}




int main(void)
{
	// create the fifo pipe file.
	if (mkfifo("command", 0600) != 0) {
		fprintf(stderr, "Unable to create the command pipe\n");
		exit(1);
	}
	
	// open the command pipe.
	int command_fd = open("command", O_NONBLOCK | O_RDONLY);
	if (command_fd < 0) {
		fprintf(stderr, "Unable to open the command pipe\n");
		exit(1);
	}
	
	// create our event base which will be the pivot point for pretty much everything.
	_evbase = event_base_new();
	assert(_evbase);

	// create a handler for activity on the command pipe.
	assert(command_fd >= 0);
	command_event = event_new( _evbase, command_fd, EV_READ|EV_PERSIST, command_handler, NULL);
	assert(command_event);
	int s = event_add(command_event, &event_timeout);
	assert(s == 0);

	
	// main event loop.... this will only return when there are no more events in the system.
	assert(_evbase);
	event_base_dispatch(_evbase);

	// delete the command pipe
	unlink("command");
	
	return(0);
}
