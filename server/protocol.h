#ifndef __PROTOCOL_H
#define __PROTOCOL_H


// commands and replies
#define REPLY_ACK						 1
#define REPLY_FAIL						 2
#define REPLY_TRYELSEWHERE               4
#define REPLY_UNKNOWN					 9
#define CMD_HELLO						 10
#define CMD_SHUTTINGDOWN				 15
#define CMD_GOODBYE                      20
#define CMD_PING                         30
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
#define CMD_FINALISE_MIGRATION           340
#define REPLY_MIGRATION_ACK              345
#define REPLY_MIGRATION_FAIL             346
#define CMD_SET_INT						 2000
#define CMD_SET_STR						 2020
#define CMD_GET_INT						 2100
#define REPLY_DATA_INT                   2105
#define CMD_GET_STR                      2120
#define REPLY_DATA_STR                   2125
#define CMD_SYNC_INT                     3000
#define REPLY_SYNC_ACK                   3005
#define REPLY_SYNC_FAIL                  3006
#define CMD_SYNC_STRING                  3020
#define CMD_SYNC_NAME                    3100
#define CMD_SYNC_NAME_INT                3101
#define REPLY_SYNC_NAME_ACK              3105
#define REPLY_SYNC_NAME_FAIL             3106

// Added seperate command functionality for the migration even though the operation is almost 
// identicle to the sync operation.  We've chosen to do it this way because it was becoming 
// difficult to debug and ensure correct code pathing.
#define CMD_MIGRATE_INT                  3200
#define REPLY_MIGRATE_ACK                3205
#define REPLY_MIGRATE_FAIL               3206
#define CMD_MIGRATE_STRING               3220
#define CMD_MIGRATE_NAME                 3300
#define CMD_MIGRATE_NAME_INT             3301
#define REPLY_MIGRATE_NAME_ACK           3305
#define REPLY_MIGRATE_NAME_FAIL          3306




#endif
