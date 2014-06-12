// params.c


// parses the command-line parameters, and returns a params structure.
params_t * params_parse_args(int argc, char **argv)
{
	int c;
	
	assert(argc >= 0);
	assert(argv);
	
	// process arguments
	/// Need to check the options in here, there're possibly ones that we dont need.
	while ((c = getopt(argc, argv, 
		"c:"    /* max connections. */
		"h"     /* help */
		"v"     /* verbosity */
		"d"     /* daemon */
		"u:"    /* user to run as */
		"P:"    /* PID file */
		"l:"    /* connect-info filename */
		"n:"    /* other node to connect to */
		"g:"	/* logfile */
		"m:"	/* logfile maximum size */
		)) != -1) {
		switch (c) {
			case 'c':
				_maxconns = atoi(optarg);
				assert(_maxconns > 0);
				break;
			case 'h':
				usage();
				exit(EXIT_SUCCESS);
			case 'v':
				_verbose++;
				break;
			case 'd':
				assert(_daemonize == 0);
				_daemonize = 1;
				break;
			case 'u':
				assert(_username == NULL);
				_username = strdup(optarg);
				assert(_username != NULL);
				assert(_username[0] != 0);
				break;
			case 'P':
				assert(_pid_file == NULL);
				_pid_file = strdup(optarg);
				assert(_pid_file != NULL);
				assert(_pid_file[0] != 0);
				break;
			case 'l':
				_interface = optarg;
				assert(_interface != NULL);
				assert(_interface[0] != 0);
				break;
			case 'n':
				// disabled for now, because only single-node is working
				assert(0);
				
				assert(_node_file == NULL);
				_node_file = optarg;
				assert(_node_file);
				break;
				
			case 'g':
				assert(_logfile == NULL);
				_logfile = strdup(optarg);
				assert(_logfile);
				break;
				
			case 'm':
				assert(_logfile_max == 0);
				_logfile_max = atoi(optarg);
				assert(_logfile_max >= 0);
				break;
				
			default:
				fprintf(stderr, "Illegal argument \"%c\"\n", c);
				return;
				
		}	
	}
	
	if (_verbose <= 0) {
		_verbose = LOG_INFO;
	}
}




void params_free(params_t *params)
{
	assert(params);
	
	free(params);
}
