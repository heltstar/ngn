#ifndef __CFS_H__
#define __CFS_H__
#include "cfs_mysql.h"

#define ERR_EXIT(m) \
do \
{ \
    perror(m); \
    exit(EXIT_FAILURE); \
} while(0)


#define LISTEN_NUMS 2048

enum {ERR, WARN, NOTICE};

typedef struct file_part
{
	int         flag;      //record if this part is download ok:0--not be download;-1-- failed; 1--ok ; 2-- is downloading
	char        pathname[256];
	long int    offset;
	long int    data_size;
}file_part_t;

typedef struct cfs_node_record 
{
    int         flag;       //record if the cfsnode host:port has file_path[256],flag--0,not exist;flag--1,exist.
    char        host[256];
    short       port;
    char        file_path[256];
}cfs_node_record_t;

typedef struct my_thread_arg { // thread struct 
    file_part_t         *fpt;
    cfs_node_record_t   *cnrt;
	int                 *nthread; 
} my_cfs_thread_arg_t;

typedef struct cfs_req {
	short       req_port;
	int         is_update;
	int         is_hdfs;
	char        *file_path;
	char        *req_host;
	char        *req_path;
	long long   file_size;
} cfs_req_t;

typedef struct send_struct
{    
	int                 file_exist_flag;
	char                file_path[256];
	long long unsigned  file_size;
    long int            offset;
    long int            data_size;
}send_struct_t;

typedef struct cfs_http_header {
	short       accept_ranges;
	short       http_code;
	short       hdfs_code;
	long long   content_length;
	char        content_type[32];
	char        server[32];
} cfs_http_header_t;

typedef struct cfs_thread_arg {
	int         *nthread;
	char        *path;
	cfs_req_t   *req;
	int          fd;
	long long    offset;
	long long    limit;
} cfs_thread_arg_t;

typedef struct cfs_origin_config {
	short    port;
	char     rate[8];
	char     cache[8];
	char     *host;
	char     ip[32];
	char     *username;
	char     *password;
	char     *api_download;
} cfs_origin_config_t;

typedef struct cfs_mysql_config {
	short    port;
	char     *host;
	char     *username;
	char     *password;
	char     *database;
} cfs_mysql_config_t;

/*added for cfs download*/
typedef struct cfs_cfsedge_config {
	char    *host;
	short   port;
	char	*key;
	struct cfs_cfsedge_config *next;
}cfs_cfsedge_config_t;

typedef struct cfs_config {
	unsigned short          io_utilization;
	unsigned short          io_port;
	unsigned short          server_port; //added for cfs download
	unsigned int            thread_size;
	cfs_origin_config_t     *origin;
	cfs_mysql_config_t      *mysql;
	cfs_cfsedge_config_t    *cfsedge;  //added for cfs download
    int                      cfsedge_nums;  // cfsedge numbers
	char                    *username;
	char                    *groupname;
	long long               log_size;
	long long               package_size;
	char                    *log_path;
	char                    *work_dir;
	char                    io_host[32];
	char                    app_name[64];
} cfs_config_t;

typedef struct cfs_disk {
	unsigned short  priority;
	unsigned int    id;
	char           *device;
	char           *work;
} cfs_disk_t;

static int      cfs_md5_encode (const unsigned char *str, size_t buff_len, char *result);
static void     cfs_config_init (int argc, char **argv);
static int      cfs_socket_init (const char *host, const short port);
static int      cfs_parse_domain (const char *domain, char *dest);
static int      cfs_preg_match (char pattern[], char str[]);
static int      cfs_get_localtime (char *date_str);
//     static void     cfs_clear_space (const char *str, char *dest);
static int      cfs_download (cfs_req_t *req_info, char *work_path);
static int      my_cfs_download (cfs_cfsedge_config_t *pccc, char *file_path, long long file_size);

static int      cfs_mkrdir (char *file_path, mode_t mode);
static void*    cfs_download_part (void *params);
static void*    my_cfs_download_part (void *params);

static ssize_t  cfs_writen (const int sock, void *data, size_t length);
static ssize_t  cfs_readn (const int fd, void *buf, size_t length);
static int      cfs_get_header (cfs_req_t *req, cfs_http_header_t *header);

static void     cfs_exit_download (cfs_thread_arg_t *args, void *body_buff, int code);
static void     my_cfs_exit_download (my_cfs_thread_arg_t *args, void *body_buff, int code);
static int      cfs_log (int level, char *fmt, ...);
static void     cfs_init();
static void    signal_hander(int num);
static void     cfs_disk_init(MYSQL *conn_ptr, cfs_disk_t **disk, unsigned long *len);
static int      cfs_iostats(const int fd, cfs_disk_t *disk, const unsigned long len, cfs_disk_t **result);
static char*    cfs_ioerror(char *buff);

void   cfs_free(void *ptr);
void   *cfs_malloc(size_t size);
void   cfs_split(char *str, const char delimiter, char **key, char **val);

void* cfs_server_run();
static unsigned long get_file_size(const char *path);
static void* do_send_file(void* params);
#endif
