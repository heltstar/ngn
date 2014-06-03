#ifndef _CFS_MYSQL_
#define _CFS_MYSQL_
#include <mysql.h>

//
MYSQL *db_connect(const char *host, const char *user, const char *passwd, const char *db, const int port);

//
void db_close(MYSQL *sock);

// 
MYSQL_RES *db_select(MYSQL *conn_ptr, const char *sql);

// @param conn_ptr connect string
// @param sql query sql string
int db_query(MYSQL *conn_ptr, const char *sql);

// 
void db_free_result(MYSQL_RES *result);
#endif