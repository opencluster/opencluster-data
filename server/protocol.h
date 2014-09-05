#ifndef __PROTOCOL_H
#define __PROTOCOL_H


// commands and replies
#define COMMAND_HELLO                       0x0010
#define COMMAND_SHUTTINGDOWN                0x0030
#define COMMAND_GOODBYE                     0x0040
#define COMMAND_PING                        0x0050
#define COMMAND_SERVERHELLO                 0x0060
#define COMMAND_HASHMASK                    0x0080
#define COMMAND_LOADLEVELS                  0x0100
#define COMMAND_ACCEPT_BUCKET               0x0110
#define COMMAND_CONTROL_BUCKET              0x0120
#define COMMAND_FINALISE_MIGRATION          0x0130
#define COMMAND_GET_INT                     0x2010
#define COMMAND_GET_STRING                  0x2020
#define COMMAND_SET_INT                     0x2200
#define COMMAND_SET_STRING                  0x2210

#define COMMAND_SET_KEYVALUE                0x2500
#define COMMAND_GET_KEYVALUE                0x2520

#define COMMAND_SYNC_INT                    0x3000
#define COMMAND_SYNC_STRING                 0x3010
#define COMMAND_SYNC_KEYVALUE               0x3060
#define COMMAND_SYNC_KEYVALUE_INT           0x3070



#define RESPONSE_UNKNOWN          0x0002
#define RESPONSE_FAIL             0x0003
#define RESPONSE_WRONGTYPE        0x0005
#define RESPONSE_TOOLARGE         0x0006

#define RESPONSE_OK               0x0010
#define RESPONSE_KEYVALUE_HASH    0x001F
#define RESPONSE_KEYVALUE         0x0020
#define RESPONSE_LOADLEVELS       0x0013
#define RESPONSE_DATA_INT         0x0110
#define RESPONSE_DATA_STRING      0x0120



#endif
