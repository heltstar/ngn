#include <stdio.h>
#include "cfs_mysql.h"

MYSQL *db_connect(const char *host, const char *user, const char *passwd, const char *db, const int port)
{
    MYSQL *conn_ptr = NULL;

    conn_ptr = mysql_init(NULL);
    if (!conn_ptr) {
        return NULL;
    }
    conn_ptr = mysql_real_connect(conn_ptr, host, user, passwd, db, port, NULL, 0);

    return conn_ptr;
}

void db_close(MYSQL *sock)
{
    mysql_close(sock);
}

MYSQL_RES *db_select(MYSQL *conn_ptr, const char *sql)
{
    if (conn_ptr == NULL){
        return NULL;
    }
    MYSQL_RES *res_ptr = NULL;
    int res = 0;

    res = mysql_query(conn_ptr, sql);

    if (res){   //query error
        return NULL;
    }
    res_ptr = mysql_store_result(conn_ptr);

    return res_ptr;
}
void db_free_result(MYSQL_RES *result)
{
    mysql_free_result(result);
}

int db_query(MYSQL *conn_ptr, const char *sql)
{
    if (conn_ptr == NULL){
        return -1;
    }

    return mysql_query(conn_ptr, sql);;
}