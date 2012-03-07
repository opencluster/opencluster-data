#ifndef __PROTOCOL_H
#define __PROTOCOL_H


// commands and replies
#define REPLY_ACK						 1
#define REPLY_UNKNOWN					 9
#define CMD_HELLO						 10
#define CMD_SHUTTINGDOWN				 15
#define CMD_PING						 30
#define CMD_SERVERHELLO					 50
#define CMD_SERVERINFO					 100
#define CMD_HASHMASK					 120
#define CMD_LOADLEVELS					 200
#define REPLY_LOADLEVELS				 210
#define CMD_ACCEPT_BUCKET				 300
#define REPLY_CANT_ACCEPT_BUCKET		 305
#define REPLY_ACCEPTING_BUCKET			 310
#define CMD_CONTROL_BUCKET				 320
#define REPLY_CONTROL_BUCKET_COMPLETE    330
#define REPLY_CONTROL_BUCKET_FAILED      335
#define CMD_SET_INT						 2000
#define CMD_SET_STR						 2020
#define CMD_GET_INT						 2100
#define REPLY_DATA_INT					 2105
#define CMD_GET_STR						 2120
#define REPLY_DATA_STR					 2125




#endif