
/* 
 * Authentication details for server to server authentication.
 */

#ifndef __AUTH_H
#define __AUTH_H


void auth_free(void);

int auth_sync_load(const char *sync_dir);
int auth_query_load(const char *sync_dir);

#endif