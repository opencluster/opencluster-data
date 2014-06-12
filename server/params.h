// params.h

typedef struct {
	int daemonize;
const char *_username = NULL;
const char *_pid_file = NULL;
const char *_logfile = NULL;
int _logfile_max = 0;
const char *_node_file = NULL;

} params_t;

params_t * params_parse_args(int argc, char **argv);
void params_free(params_t *params);