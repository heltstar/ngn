#define _GNU_SOURCE
#include <unistd.h>
#include <sys/utsname.h>
#include <openssl/md5.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <libgen.h>
#include <pcre.h>
#include <syslog.h>
#include <fcntl.h>
#include <netdb.h>
#include <stdarg.h>
#include <sys/types.h>
#include <pwd.h>
#include <grp.h>

#include <sys/select.h>

// #include "cfs_mysql.h"
#include "iniparser.h"
#include "urlencode.h"
#include "cfs.h"

#define T_CDN_DOWN_QUEUE    "cdn_down_queue"
#define T_CDN_DISK          "cdn_disk"
#define VERSION             "0.3.1"

//#define cfs_log(

extern int h_errno;
extern int errno;
static pthread_mutex_t g_mutex_lock;
static int g_quit;
static cfs_config_t *g_config;
static int g_download_stat;
static char *g_user_agent;

//for download from cfs(0..n)
static pthread_t server_tid;
//static int g_download_flag = 0;

int main(int argc, char **argv)
{
		// malloc global config space
    g_config                    = (cfs_config_t *)cfs_malloc(sizeof(cfs_config_t));
    cfs_origin_config_t *origin = (cfs_origin_config_t *)cfs_malloc(sizeof(cfs_origin_config_t));
    g_config->origin            = origin;

    cfs_mysql_config_t *mysql = (cfs_mysql_config_t *)malloc(sizeof(cfs_mysql_config_t));
    g_config->mysql           = mysql;

    cfs_req_t *req_record = (cfs_req_t *)cfs_malloc(sizeof(cfs_req_t));

	my_req_t *my_record = (my_req_t*)cfs_malloc(sizeof(my_req_t)); // download ...

    // load config file
    cfs_config_init(argc, argv);

    // init run time info
    cfs_init();

    // connect mysql
    MYSQL *conn_ptr = NULL;

    conn_ptr = db_connect(mysql->host, mysql->username, mysql->password, mysql->database, mysql->port);
    if (conn_ptr == NULL){
        cfs_log(ERR, "connect mysql Access denied!");
        ERR_EXIT("connect mysql Access denied!");
    }
    if (db_query(conn_ptr, "SET NAMES utf8")){
        cfs_log(WARN, "set utf8");
    }

    // init node disk
    unsigned long disk_len;
    cfs_disk_t *disk = NULL;
    cfs_disk_init(conn_ptr, &disk, &disk_len);

    // io stat socket 
    int iostat_fd = cfs_socket_init(g_config->io_host, g_config->io_port);
    if (iostat_fd == -1){
        cfs_log(ERR, "connect io stat server failed");
        goto clean;
    }

    int perr = pthread_create(&server_tid, NULL, cfs_server_run, NULL); //cfs server phtread running... 
    if (perr != 0)
    {
        cfs_log(ERR, "can't create cfs server thread ");
        goto clean;
    }
    cfs_log(NOTICE, "cfs server thread is running, tid = %ld", server_tid);
    pthread_detach(server_tid);

    cfs_log(NOTICE, "for loop start...");
    for ( ;; ){
        cfs_log(NOTICE, "in for loop"); 
        MYSQL_ROW result_row = NULL;
        char sql[256] = {0};

        MYSQL_RES *data_res = NULL;
        sprintf(sql, "SELECT id, file_path, file_size, is_update, origin_host, origin_path FROM %s ORDER BY pv_num DESC, push_date DESC LIMIT 1", T_CDN_DOWN_QUEUE);
        data_res = db_select(conn_ptr, sql);
        my_ulonglong rows = mysql_num_rows(data_res);

        cfs_log(NOTICE, "after mysql select CMD, rows=%lld",rows); 
        printf("after mysql select CMD, rows=%lld\n",rows); 
        if (((long int)rows) != 0){
            int flag = 0; 
            result_row = mysql_fetch_row(data_res);

            memset(my_record, 0, sizeof(my_req_t));
   //         my_record->req_file_path = strdup(result_row[1]);
    //        cfs_log(NOTICE, "first try to get resources(file_path=%s) from other cfs nodes...",my_record->file_path);
            printf("first try to get resources(file_path=%s) from other cfs node \n",result_row[1]);
            cfs_cfsedge_config_t *pccc = g_config->cfsedge;// first try to get resources from other cfs nodes.
            while(pccc != NULL)//
            {	
                printf("while loop.\n");
                my_record->req_port = pccc->port;
                my_record->req_host = strdup(pccc->host);
                my_record->req_file_path = strdup(result_row[1]);

                printf("try to get file(file_path=%s) from cfsnode host:port is %s:%d\n",my_record->req_file_path, pccc->host, pccc->port);
				printf("cfs_download start\n");
				int flag = my_cfs_download(my_record);
				if(flag == -1)
				{
					cfs_free(my_record->req_host);
					cfs_free(my_record->req_file_path);
					pccc = pccc->next;
					continue;
				}
				else 
				{
					cfs_free(my_record->req_file_path);
					cfs_free(my_record->req_host);
					goto cfs;  // download resources success.
				}
			}

			char now_date_str[32] = {0};
			cfs_get_localtime(now_date_str);
			flag = 0;// init flag for origin below
			// update record start download date
			sprintf(sql, "UPDATE %s SET start_date = '%s' WHERE id = %s", T_CDN_DOWN_QUEUE, now_date_str, result_row[0]);
			if (db_query(conn_ptr, sql)){
				cfs_log(ERR, "query update date error");
			}

			cfs_log(NOTICE, "now try to get resources from origin node...");
			if (strlen(result_row[4]) > 0){
				char *key = NULL;
				char *val = NULL ;
				cfs_split(result_row[4], ':', &key, &val);
				if (val != NULL) {
					req_record->req_host = key;
					req_record->req_port = atoi(val);
					cfs_free(val);
				}else{
					req_record->req_host = strdup(result_row[4]);
					req_record->req_port = 80;
				}

				req_record->req_path = strdup(result_row[5]);
				req_record->is_hdfs  = 0;
				flag = 1;
			}else{
				req_record->req_host = origin->host;
				req_record->req_path = origin->api_download;
				req_record->req_port = origin->port;
				req_record->is_hdfs  = 1;
			}

			req_record->is_update = atoi(result_row[3]);

			cfs_http_header_t header = {0};
			int nres = cfs_get_header(req_record, &header);
			if (nres == 0){            
				// printf("path=%s\nsize=%lld\nhost=%s\napi=%s\nupdate=%d\nhdfs=%d\nhhttp=%d\nhttp=%d\n", 
				//     req_record->file_path, header.content_length, 
				//     req_record->req_host, req_record->req_path, req_record->is_update, 
				//     req_record->is_hdfs, header.hdfs_code, header.http_code);

				if (((header.http_code == 200 || header.http_code == 206) && req_record->is_hdfs == 1 && header.hdfs_code == 1000) || 
						((header.http_code == 200 || header.http_code == 206) && req_record->is_hdfs == 0)){
					req_record->file_size = header.content_length;
					// 
					if (g_config->io_utilization == 0) {
						cfs_download(req_record, g_config->work_dir);
					}else{
						cfs_disk_t *item = NULL;
						nres = cfs_iostats(iostat_fd, disk, disk_len, &item);
						if (nres == -1) {
							cfs_log(ERR, "get iostat failed sleep 15s");
							sleep(15);
							goto end;
						}
						if (item == NULL) {
							cfs_log(ERR, "get iostat failed item is null sleep 15s");
							sleep(15);
							goto end;
						}else{
							nres = 0;
							nres = cfs_download(req_record, item->work);
							if (nres == 0) {
								sprintf(sql, "UPDATE %s SET disk_id = %d WHERE id = %s", T_CDN_DOWN_QUEUE, item->id, result_row[0]);
								if (db_query(conn_ptr, sql)){
									cfs_log(ERR, "query update date error");
								}
							}
						}
					}
				}else{;
					cfs_log(ERR, "Get %s %s is failed code %d hcode %d", req_record->req_path, req_record->file_path, header.http_code, header.hdfs_code);
				}
			}else{
				cfs_log(WARN, "Connection %s:%d failed Get file path %s %s", req_record->req_host, req_record->req_port, req_record->req_path, req_record->file_path);
			}
cfs:
			// delete file record mysql trigger insert to cdn_file_records table if disk_id > 0 
			sprintf(sql, "DELETE FROM %s WHERE id = %s", T_CDN_DOWN_QUEUE, result_row[0]);
			if (db_query(conn_ptr, sql)){
				cfs_log(ERR, "query delete date error");
			}
end:
			// clean req_record space
			cfs_free(req_record->file_path);
			if (flag == 1) {
				cfs_free(req_record->req_host);
				cfs_free(req_record->req_path);
			}
			// sleep(1);
			// break;
		}else{
			cfs_log(NOTICE, "sleep 5 seconds");
			sleep(5);
		}
		db_free_result(data_res);
		if (g_quit == 1) {
			cfs_log(NOTICE, "Exiting ...");
			break;
		}
	}

clean:
	// clean memory space
	close(iostat_fd);
	db_close(conn_ptr);
	conn_ptr = NULL;

	int i;
	for (i = 0; i < disk_len; ++i)
	{
		cfs_free(disk[i].work);
		cfs_free(disk[i].device);
	}

	cfs_free(disk);

	cfs_free(g_user_agent);

	cfs_free(req_record);
	cfs_free(origin->api_download);
	cfs_free(origin->host);
	cfs_free(origin->password);
	cfs_free(origin->username);
	cfs_free(origin);

	cfs_cfsedge_config_t *ccc = g_config->cfsedge;
	while(ccc != NULL)
	{
		cfs_cfsedge_config_t *cfsedge_tmp = ccc;
		ccc = ccc->next;

		cfs_free(cfsedge_tmp->host); // free cfsX nodes
		cfs_free(cfsedge_tmp->key);
		cfs_free(cfsedge_tmp);
	}

	cfs_free(mysql->host);
	cfs_free(mysql->username);
	cfs_free(mysql->password);
	cfs_free(mysql->database);
	cfs_free(mysql);

	cfs_free(g_config->groupname);
	cfs_free(g_config->username);
	cfs_free(g_config->log_path);
	cfs_free(g_config->work_dir);
	cfs_free(g_config);
	pthread_mutex_destroy(&g_mutex_lock);

	return 0;
}

// static void 
// cfs_clear_space(const char *str, char *dest)
// {
//     char *tmp = dest;
//     while (*str != '\0'){
//         if (*str != ' '){
//             *tmp++ = *str++;
//         }else{
//             str++;
//         }
//     }
// }

	static int 
cfs_md5_encode(const unsigned char *str, size_t buff_len, char *result)
{
	unsigned char md[16];
	char tmp[3] = {'\0'};
	MD5(str, buff_len, md);
	int i = 0;
	for(i=0; i<16; i++)
	{
		sprintf(tmp, "%2.2x", md[i]);
		strcat(result, tmp);
	}
	return 0;
}

	static int 
cfs_mkrdir(char *path, mode_t mode)
{
	char *file_path = strdup(path);
	if (file_path == NULL) {
		return 0;
	}
	char dir_name[256] = {0};
	char split[] = "/";
	char *p = strtok(file_path, split);

	if (*file_path == '/') {
		strcat(dir_name, split);
	}

	while(p!=NULL) {
		strcat(dir_name, p);
		strcat(dir_name, split);
		if (access(dir_name, 0) != 0){	// dir not exists
			if (mkdir(dir_name, mode) == -1){
				return -1;
				break;
			}
		}
		p = strtok(NULL, split);
	}
	cfs_free(file_path);
	return 0;
}

	static int
cfs_download(cfs_req_t *req_info, char *work_path)
{
	char *dir_buff                        = NULL;
	char tmp_name[256]                    = {0};
	char dir[256]                         = {0};
	char *name_buff                       = NULL;
	char *name                            = NULL;
	char file_path[256]                   = {0};
	int  is_exists                        = 0;
	const unsigned long long package_size = g_config->package_size;
	int len;

	len = strlen(req_info->file_path);
	long long nwrite = req_info->file_size;

	dir_buff = cfs_malloc(len + 1);
	name_buff = cfs_malloc(len + 1);

	memcpy(dir_buff, req_info->file_path, len);
	memcpy(name_buff, req_info->file_path, len);

	name = basename(name_buff);

	sprintf(dir, "%s%s", work_path, dirname(dir_buff));
	sprintf(file_path, "%s/%s", dir, name);

	// file already exists
	if (access(file_path, 0) == 0) {
		is_exists = 1;
		if (req_info->is_update == 0) {
			cfs_log(NOTICE, "File is exist %s", file_path);
			return 0;
		}
	}

	// 
	if (access(dir, 0) != 0) {
		if (cfs_mkrdir(dir, 0777) == -1) {
			cfs_log(ERR, "mkdir %s failed: %s", dir, strerror(errno));
			return -1;
		}
	}

	char md5_name[36] = {0};
	cfs_md5_encode((unsigned char *)name, strlen(name), md5_name);
	sprintf(tmp_name, "%s/%s", dir, md5_name);

	cfs_free(name_buff);
	cfs_free(dir_buff);

	int nthread     = 0;
	clock_t st      = clock();
	time_t start    = time(0);
	int i           = 0;
	int max_threads = g_config->thread_size;

	// add thread download resource
	while (nwrite > 0){

		if (nthread >= max_threads) {
			sleep(1);
			continue;
		}

		cfs_thread_arg_t *args = (cfs_thread_arg_t *)malloc(sizeof(cfs_thread_arg_t));
		if (args == NULL){
			return -1;
		}
		args->req = req_info;
		args->offset = i * package_size;
		args->limit = (i * package_size) + package_size - 1;
		args->path = tmp_name;
		args->nthread = &nthread;

		pthread_t pid;
		int res = pthread_create(&pid, NULL, cfs_download_part, (void *)args);
		if (res == 0){
			pthread_mutex_lock(&g_mutex_lock);
			if (nthread < max_threads)
				nthread++;
			pthread_mutex_unlock(&g_mutex_lock);
		}
		nwrite -= package_size;
		i++;
	}

	// wait thread all download done
	while (!(nthread == 0)) {
		if (req_info->file_size > (30 * 1024 * 1024)){
			sleep(5);
		}else if (req_info->file_size > (5 * 1024 * 1024)){
			sleep(1);
		}else{
			usleep(250000);
		}
	}

	// download failed delete temp file
	if (g_download_stat == 1){
		if (req_info->is_hdfs == 1){
			cfs_log(NOTICE, "download %s %s failed", req_info->req_host, req_info->file_path);
		}else{
			cfs_log(NOTICE, "download %s %s failed", req_info->req_host, req_info->req_path);
		}
		unlink(tmp_name);
		return -1;
	}

	cfs_log(NOTICE, "write %s success %lld size %dus run time %ds", file_path, req_info->file_size, (unsigned int)(clock() - st), (unsigned int)(time(0) - start));
	if (is_exists == 1 && req_info->is_update == 1) {
		if (unlink(file_path)){
			cfs_log(WARN, "Delete Old file %s failed: %s", file_path, strerror(errno));
		}
	}

	if (rename(tmp_name, file_path)){
		cfs_log(WARN, "Rename %s to %s failed: %s", tmp_name, file_path, strerror(errno));
		return -1;
	}

	return 0;
}

	static int
my_cfs_download(my_req_t *req_info)
{
	char tmp_name[256]                    = {0};
	char file_path[256]                   = {0};
	const unsigned long long package_size = g_config->package_size;

	long long nwrite = req_info->file_size;

	// file already exists
	if (access(req_info->req_file_path, 0) == 0) {
		cfs_log(NOTICE, "File is exist %s", req_info->req_file_path);
		return 0;
	}

//	if (access(dir, 0) != 0) {
//		if (cfs_mkrdir(dir, 0777) == -1) {
//			cfs_log(ERR, "mkdir %s failed: %s", dir, strerror(errno));
//			return -1;
//		}
//	}

	int nthread     = 0;
	int i           = 0;
	int max_threads = g_config->thread_size;

	int sock = cfs_socket_init(req_info->req_host, req_info->req_port);
	if (sock < 0){
		return -1;
	}

	send_struct_t sst;
	sst.file_exist_flag = 0;
	sst.file_size = 0;
	strcpy(sst.file_path,req_info->req_file_path);
	cfs_writen(sock, &sst, sizeof(sst));
	memset(&sst, 0, sizeof(sst));
	int rnd = cfs_readn(sock,(void*)&sst,sizeof(sst));
	if(rnd != sizeof(sst))
	{
		printf("error.\n");
		return -1;
	}
	if(sst.file_exist_flag != 1 || sst.file_size == 0)
	{
		printf("file not exist or file size is 0\n");
		return -1;
	}
	close(sock);

	nwrite = sst.file_size ;
	// add thread download resource
	while (nwrite > 0){

		if (nthread >= max_threads) {
            sleep(1);
            continue;
        }

        my_thread_arg_t *args = (my_thread_arg_t *)malloc(sizeof(my_thread_arg_t));
        if (args == NULL){
            return -1;
        }
        args->req = req_info;
        args->offset = i * package_size;
        args->limit = (i * package_size) + package_size - 1;
        args->path = strdup(sst.file_path);
        args->nthread = &nthread;

        pthread_t pid;
        int res = pthread_create(&pid, NULL, my_cfs_download_part, (void *)args);
        if (res == 0){
            pthread_mutex_lock(&g_mutex_lock);
            if (nthread < max_threads)
                nthread++;
            pthread_mutex_unlock(&g_mutex_lock);
        }
        nwrite -= package_size;
        i++;
    }

    // wait thread all download done
    while (!(nthread == 0)) {
        if (req_info->file_size > (30 * 1024 * 1024)){
            sleep(5);
        }else if (req_info->file_size > (5 * 1024 * 1024)){
            sleep(1);
        }else{
            usleep(250000);
        }
    }

    // download failed delete temp file
    if (g_download_stat == 1){
        cfs_log(NOTICE, "download %s %s failed", req_info->req_host, req_info->req_file_path);
        unlink(tmp_name);
        return -1;
    }

    cfs_log(NOTICE, "write %s success %lld size", file_path, req_info->file_size);

    if (rename(tmp_name, file_path)){
        cfs_log(WARN, "Rename %s to %s failed: %s", tmp_name, file_path, strerror(errno));
        return -1;
    }

    return 0;
}
    static ssize_t
cfs_writen(const int sock, void *data, size_t length)
{
    char *buff = (char *)data;
    size_t nwrite = length;
    ssize_t nres;
    while (nwrite > 0) {
        if ((nres = write(sock, buff, nwrite)) < 0) {
            if (errno == EINTR)
                continue;
        }
        else if (nres == 0) {
            continue;
        }

        buff += nres;
        nwrite -= nres;
    }
    return length;
}

    static ssize_t
cfs_readn(const int fd, void *buf, size_t length)
{
    size_t nleft = length;
    ssize_t nread;
    char *bufp = (char*)buf;

    while (nleft > 0)
    {
        if ((nread = read(fd, bufp, nleft)) < 0)
        {
            if (errno == EINTR)
                continue;
            return -1;
        }
        else if (nread == 0)
            return length - nleft;

        bufp += nread;
        nleft -= nread;
    }
    return length;
}

    static void
cfs_exit_download(cfs_thread_arg_t *args, void *body_buff, int code)
{
    pthread_mutex_lock(&g_mutex_lock);
    int thread = *args->nthread;
    if (thread > 0){
        *args->nthread = --thread;
    }
    pthread_mutex_unlock(&g_mutex_lock);
    if (code == -1) {
        if (unlink(args->path)){
        }    
    }

    if (args != NULL) {
        free(args); 
        args = NULL;   
    }

    if (body_buff != NULL) {
        free(body_buff); 
        body_buff = NULL;   
    }
    pthread_exit(0);
}

    static void
my_cfs_exit_download(my_thread_arg_t *args, void *body_buff)
{
    pthread_mutex_lock(&g_mutex_lock);
    int thread = *args->nthread;
    if (thread > 0){
        *args->nthread = --thread;
    }
    pthread_mutex_unlock(&g_mutex_lock);

    if (args != NULL) {
        free(args); 
        args = NULL;   
    }

    if (body_buff != NULL) {
        free(body_buff); 
        body_buff = NULL;   
    }
    pthread_exit(0);
}

    static void *
cfs_download_part(void *params)
{
    cfs_thread_arg_t *args  = (cfs_thread_arg_t *)params;
    char   *rh              = NULL;
    void   *recv_buff       = NULL;
    char    log_buff[256]   = {0};
    char    tmp[256]        = {0};
    int     nres;

    cfs_origin_config_t *origin = NULL;
    if (g_config != NULL) {
        origin = g_config->origin;
    }

    rh = (char *)malloc(1024);
    if (rh == NULL) {
        cfs_exit_download(args, NULL, -1);
        return (void *)0;
    }
    memset(rh, 0, 1024);

    recv_buff = (void *)malloc(4096);
    if (recv_buff == NULL) {
        cfs_exit_download(args, NULL, -1);
        return (void *)0;
    }
    memset(recv_buff, 0, 4096);

    // long long size = args->limit - args->offset + 1;
    // body_buff = (char *)malloc(size + 1);
    // if (body_buff == NULL) {
    //     if (rh != NULL) {
    //         free(rh);
    //         rh = NULL;
    //     }
    //     cfs_exit_download(args, body_buff, -1);
    //     return (void *)0;
    // }
    // memset(body_buff, 0, size + 1);

    int sock = cfs_socket_init(args->req->req_host, args->req->req_port);
    if (sock < 0) {
        if (rh != NULL) {
            free(rh);
            rh = NULL;
        }
        pthread_mutex_lock(&g_mutex_lock);
        g_download_stat = 1;
        pthread_mutex_unlock(&g_mutex_lock);
        goto clean;
    }

    sprintf(tmp, "GET %s HTTP/1.1\r\n", args->req->req_path);
    strcat(rh, tmp);
    sprintf(tmp, "Host: %s:%d\r\n", args->req->req_host, args->req->req_port);
    strcat(rh, tmp);
    sprintf(tmp, "Accept: */*\r\n");
    strcat(rh, tmp);
    sprintf(tmp, "User-Agent: %s\r\n", g_user_agent);
    strcat(rh, tmp);
    sprintf(tmp, "Accept-Language: zh-CN,zh;q=0.8\r\n");
    strcat(rh, tmp);
    sprintf(tmp, "Range:bytes=%lld-%lld\r\n", args->offset, args->limit);
    strcat(rh, tmp);

    if (args->req->is_hdfs == 1) {
        char md5_buff[64] = {0};
        memset(md5_buff, 0, sizeof(md5_buff));
        if (cfs_md5_encode((unsigned char*)origin->username, strlen(origin->username), md5_buff) == -1){

        }
        sprintf(tmp, "user: %s\r\n", md5_buff);
        strcat(rh, tmp);

        memset(md5_buff, 0, sizeof(md5_buff));
        if (cfs_md5_encode((unsigned char *)origin->password, strlen(origin->password), md5_buff) == -1){

        }
        sprintf(tmp, "pass: %s\r\n", md5_buff);
        strcat(rh, tmp);

        // request url encode
        // char url_file_path[256] = {0};
        // if (url_encode(args->req->req_path, strlen(args->req->req_path), url_file_path, sizeof(url_file_path)) == 0){
        // close(sockfd);
        // writelog("url_encode error\r\n");
        //        }
        sprintf(tmp, "path: %s\r\n", args->req->file_path);
        strcat(rh, tmp);

        if (strlen(origin->rate) != 0) {
            sprintf(tmp, "rate: %s\r\n", origin->rate);
            strcat(rh, tmp);
        }

        if (strlen(origin->cache) != 0) {
            sprintf(tmp, "cached: %s\r\n", origin->cache);
            strcat(rh, tmp);
        }

        sprintf(tmp, "Accept-Encoding: gzip,deflate,sdch\r\n");
        strcat(rh, tmp);
    }

    sprintf(tmp, "Connection: close\r\n\r\n");
    strcat(rh, tmp);

    // write socket http request
    cfs_writen(sock, rh, strlen(rh));
    if (rh != NULL) {
        free(rh);
        rh = NULL;
    }

    int fd = open(args->path, O_CREAT|O_WRONLY, 0755);
    if (fd == -1) {
        pthread_mutex_lock(&g_mutex_lock);
        g_download_stat = 1;
        pthread_mutex_unlock(&g_mutex_lock);
        goto clean;
    }
    lseek(fd, args->offset, SEEK_SET);

    const char *split = "\r\n\r\n";

    while((nres = cfs_readn(sock, recv_buff, 4096)) != 0) {
        char *idx = strstr((char *)recv_buff, split);

        if (idx != NULL) {
            char *content        = idx + 4;
            int head_len         = content - (char *)recv_buff;
            char *recv_head_buff = (char *)malloc(head_len + 1);

            memset(recv_head_buff, 0, head_len + 1);
            memcpy(recv_head_buff, recv_buff, head_len);

            // verify http return code
            char *status = strstr(recv_head_buff, "HTTP/1.1");
            if (status == NULL) {
                status = strstr(recv_head_buff, "HTTP/1.0");
            }
            if (status != NULL){
                status = strchr(recv_head_buff, ' ');
                if (status != NULL){
                    status++;
                    short http_code = atoi(status);
                    if (http_code >= 400){
                        cfs_log(WARN, "Get %s %d", args->req->req_path, http_code);

                        pthread_mutex_lock(&g_mutex_lock);
                        g_download_stat = 1;
                        pthread_mutex_unlock(&g_mutex_lock);

                        if (recv_head_buff != NULL) {
                            free(recv_head_buff);
                            recv_head_buff = NULL;
                        }
                        goto clean;
                    }
                }
            }

            // verify hdfs return code
            if (args->req->is_hdfs == 1) {
                char *first = strstr(recv_head_buff, "result:");
                if (first != NULL){
                    first = strchr(first, ':');
                    first++;
                    int res_code = atoi(first);
                    int is_error = 0;
                    switch (res_code){
                        case 1001:
                            is_error = 1;
                            sprintf(log_buff, "Error: result code %d Message: Authentication failed!", res_code);
                            break;
                        case 1002:
                            is_error = 1;
                            sprintf(log_buff, "Error: result code %d Message: Resource is non-existent!", res_code);
                            break;
                        case 1005:
                            is_error = 1;
                            sprintf(log_buff, "Error: result code %d Message: Unknown error reasons!", res_code);
                            break;
                        case 1007:
                            is_error = 1;
                            sprintf(log_buff, "Error: result code %d Message: Path is not passed!", res_code);
                            break;
                        case 1009:
                            is_error = 1;
                            sprintf(log_buff, "Error: result code %d Message: HDFS operation failed!", res_code);
                            break;
                    }

                    // hdfs error exit current thread
                    if (is_error == 1){

                        if (args->req->is_hdfs == 1) {
                            cfs_log(WARN, "Get %s %s %s", args->req->req_path, args->req->file_path, log_buff);
                        }else{
                            cfs_log(WARN, "Get %s %s", args->req->req_path, log_buff);
                        }

                        if (recv_head_buff != NULL) {
                            free(recv_head_buff);
                            recv_head_buff = NULL;
                        }

                        pthread_mutex_lock(&g_mutex_lock);
                        g_download_stat = 1;
                        pthread_mutex_unlock(&g_mutex_lock);

                        goto clean;
                    }
                }
            }
            cfs_free(recv_head_buff);
            // write buff to file
            cfs_writen(fd, content, nres - head_len);
        }else{
            cfs_writen(fd, recv_buff, nres);
        }
    }

    // cfs_writen(fd, body_buff, nwrite);
clean:
    close(fd);
    close(sock);
    if (recv_buff != NULL) {
        free(recv_buff);
        recv_buff = NULL;
    }
    cfs_exit_download(args, NULL, 0);
    return (void *)0;
}

    static void *
my_cfs_download_part(void *params)
{
    my_thread_arg_t *args  = (my_thread_arg_t *)params;
    file_part_t    *filep;
    void   *recv_buff       = NULL;
    int     nres;

    filep = (file_part_t*)malloc(sizeof(file_part_t));
    if (filep == NULL) {
        my_cfs_exit_download(args, NULL);
        return (void *)0;
    }
    memset(filep, 0, sizeof(file_part_t));

    recv_buff = (void *)malloc(4096);
    if (recv_buff == NULL) {
        my_cfs_exit_download(args, NULL);
        return (void *)0;
    }
    memset(recv_buff, 0, 4096);

    int sock = cfs_socket_init(args->req->req_host, args->req->req_port);
    if (sock < 0) {
        if (filep != NULL) {
            free(filep);
            filep = NULL;
        }
        pthread_mutex_lock(&g_mutex_lock);
        g_download_stat = 1;
        pthread_mutex_unlock(&g_mutex_lock);
        goto clean;
    }

    strcpy(filep->pathname, args->req->req_file_path);
    filep->offset = args->offset;
    filep->limit = args->limit;

    // write socket http request
    cfs_writen(sock, filep, sizeof(*filep));
    if (filep != NULL) {
        free(filep);
        filep= NULL;
    }

    int fd = open(args->path, O_CREAT|O_WRONLY, 0755);
    if (fd == -1) {
        pthread_mutex_lock(&g_mutex_lock);
        g_download_stat = 1;
        pthread_mutex_unlock(&g_mutex_lock);
        goto clean;
    }
    lseek(fd, args->offset, SEEK_SET);
    while((nres = cfs_readn(sock, recv_buff, 4096)) != 0) 
    {
            // write buff to file
            cfs_writen(fd, recv_buff, nres);
    }

clean:
    close(fd);
    close(sock);
    if (recv_buff != NULL) {
        free(recv_buff);
        recv_buff = NULL;
    }
    my_cfs_exit_download(args, NULL);
    return (void *)0;
}
    static int
cfs_get_header(cfs_req_t *req, cfs_http_header_t *header)
{
    int     nres;
    char   *pos, *epos;
    char    tmp[256]     = {0};
    char   *head_buff    = NULL;
    char   *rh           = NULL;
    char    buff[32]     = {0};
    cfs_origin_config_t *origin = NULL;
    const char *split    = "\r\n\r\n";

    rh = (char *)cfs_malloc(1024);
    head_buff = (char *)cfs_malloc(1024);

    if (g_config != NULL) {
        origin = g_config->origin;
    }

    int sock = cfs_socket_init(req->req_host, req->req_port);
    if (sock < 0){
        return -1;
    }

    sprintf(tmp, "HEAD %s HTTP/1.1\r\n", req->req_path);
    strcat(rh, tmp);
    sprintf(tmp, "Host: %s:%d\r\n", req->req_host, req->req_port);
    strcat(rh, tmp);
    sprintf(tmp, "Accept: */*\r\n");
    strcat(rh, tmp);
    sprintf(tmp, "User-Agent: %s\r\n", g_user_agent);
    strcat(rh, tmp);
    sprintf(tmp, "Accept-Language: zh-CN,zh;q=0.8\r\n");
    strcat(rh, tmp);

    if (req->is_hdfs == 1) {
        char md5_buff[64] = {0};
        memset(md5_buff, 0, sizeof(md5_buff));
        if (cfs_md5_encode((unsigned char*)origin->username, strlen(origin->username), md5_buff) == -1){

        }

        sprintf(tmp, "user: %s\r\n", md5_buff);
        strcat(rh, tmp);

        memset(md5_buff, 0, sizeof(md5_buff));
        if (cfs_md5_encode((unsigned char *)origin->password, strlen(origin->password), md5_buff) == -1){

        }
        sprintf(tmp, "pass: %s\r\n", md5_buff);
        strcat(rh, tmp);
        sprintf(tmp, "path: %s\r\n", req->file_path);
        strcat(rh, tmp);
        sprintf(tmp, "Accept-Encoding: gzip,deflate,sdch\r\n");
        strcat(rh, tmp);
    }

    sprintf(tmp, "Connection: close\r\n\r\n");
    strcat(rh, tmp);

    cfs_writen(sock, rh, strlen(rh));
    cfs_free(rh);

    while((nres = cfs_readn(sock, head_buff, 1024)) != 0){
        char *idx = strstr((char *)head_buff, split);

        if (idx != NULL) {
            pos = strstr(head_buff, "HTTP/1.1");
            if (pos == NULL) {
                pos = strstr(head_buff, "HTTP/1.0");
            }
            if (pos != NULL){
                pos = strchr(pos, ' ');
                if (pos != NULL){
                    pos++;
                    header->http_code = atoi(pos);
                }
            }

            pos = strcasestr(head_buff, "Content-Type:");
            if (pos != NULL){
                pos = strchr(pos, ':');
                if (pos != NULL){
                    pos++;
                    epos = strcasestr(pos, "\r\n");
                    if (epos != NULL){
                        memset(buff, 0, sizeof(buff));
                        strncpy(buff, pos, epos-pos);
                        if (buff[0] == ' ')
                            strcpy(header->content_type, &buff[1]);
                        else
                            strcpy(header->content_type, &buff[0]);
                    }
                }
            }

            pos = strcasestr(head_buff, "Server:");
            if (pos != NULL){
                pos = strchr(pos, ':');
                if (pos != NULL){
                    pos++;
                    epos = strcasestr(pos, "\r\n");
                    if (epos != NULL){
                        memset(buff, 0, sizeof(buff));
                        strncpy(buff, pos, epos-pos);
                        if (buff[0] == ' ')
                            strcpy(header->server, &buff[1]);
                        else
                            strcpy(header->server, &buff[0]);
                    }
                }
            }

            pos = strcasestr(head_buff, "Content-Length:");
            if (pos != NULL){
                pos = strchr(pos, ':');
                if (pos != NULL){
                    pos++;
                    long long length = atoll(pos);
                    header->content_length = length;
                }
            }

            pos = strcasestr(head_buff, "Accept-Ranges:");
            if (pos != NULL){
                header->accept_ranges = 1;
            }

            if (req->is_hdfs == 1) {
                char *first = strcasestr(head_buff, "result:");
                if (first != NULL){
                    first = strchr(first, ':');
                    first++;
                    int res_code = atoi(first);
                    header->hdfs_code = res_code;
                }
            }
        }
    }

    cfs_free(head_buff);
    close(sock);

    return 0;
}

//
//	static int
//my_cfs_get_header(cfs_req_t *req, cfs_http_header_t *header)
//{
//	int     nres;
//	char   *pos, *epos;
//	char    tmp[256]     = {0};
//	char   *head_buff    = NULL;
//	char   *rh           = NULL;
//	char    buff[32]     = {0};
//	const char *split    = "\r\n\r\n";
//
//	rh = (char *)cfs_malloc(1024);
//	head_buff = (char *)cfs_malloc(1024);
//
//	int sock = cfs_socket_init(req->req_host, req->req_port);
//	if (sock < 0){
//		printf("%d:cfs_socekt_init <0\n",__LINE__);
//		return -1;
//	}
//
//	sprintf(tmp, "HEAD %s HTTP/1.1\r\n", req->req_path);
//	strcat(rh, tmp);
//	sprintf(tmp, "Host: %s:%d\r\n", req->req_host, req->req_port);
//	strcat(rh, tmp);
//	sprintf(tmp, "Accept: */*\r\n");
//	strcat(rh, tmp);
//	sprintf(tmp, "User-Agent: %s\r\n", g_user_agent);
//	strcat(rh, tmp);
//	sprintf(tmp, "Accept-Language: zh-CN,zh;q=0.8\r\n");
//	strcat(rh, tmp);
//	sprintf(tmp, "Accept-Encoding: gzip,deflate,sdch\r\n");
//	strcat(rh, tmp);
//	//    sprintf(tmp, "key: %s\r\n", req->file_path);
//	//   strcat(rh, tmp);
//	sprintf(tmp, "Connection: close\r\n\r\n");
//	strcat(rh, tmp);
//
//	cfs_writen(sock, rh, strlen(rh));
//	printf("send HEAD http dataes:\n%s\n",rh);
//	cfs_free(rh);
//
//	while((nres = cfs_readn(sock, head_buff, 1024)) != 0){
//		char *idx = strstr((char *)head_buff, split);
//
//		if (idx != NULL) {
//			pos = strstr(head_buff, "HTTP/1.1");
//			if (pos == NULL) {
//				pos = strstr(head_buff, "HTTP/1.0");
//			}
//			if (pos != NULL){
//				pos = strchr(pos, ' ');
//				if (pos != NULL){
//					pos++;
//					header->http_code = atoi(pos);
//				}
//			}
//
//			pos = strcasestr(head_buff, "Content-Type:");
//			if (pos != NULL){
//				pos = strchr(pos, ':');
//				if (pos != NULL){
//					pos++;
//					epos = strcasestr(pos, "\r\n");
//					if (epos != NULL){
//						memset(buff, 0, sizeof(buff));
//						strncpy(buff, pos, epos-pos);
//						if (buff[0] == ' ')
//							strcpy(header->content_type, &buff[1]);
//						else
//							strcpy(header->content_type, &buff[0]);
//					}
//				}
//			}
//
//			pos = strcasestr(head_buff, "Server:");
//			if (pos != NULL){
//				pos = strchr(pos, ':');
//				if (pos != NULL){
//					pos++;
//					epos = strcasestr(pos, "\r\n");
//					if (epos != NULL){
//						memset(buff, 0, sizeof(buff));
//						strncpy(buff, pos, epos-pos);
//						if (buff[0] == ' ')
//							strcpy(header->server, &buff[1]);
//						else
//							strcpy(header->server, &buff[0]);
//					}
//				}
//			}
//
//			pos = strcasestr(head_buff, "Content-Length:");
//			if (pos != NULL){
//				pos = strchr(pos, ':');
//				if (pos != NULL){
//					pos++;
//					long long length = atoll(pos);
//					header->content_length = length;
//				}
//			}
//
//			pos = strcasestr(head_buff, "Accept-Ranges:");
//			if (pos != NULL){
//				header->accept_ranges = 1;
//			}
//		}
//	}
//
//	cfs_free(head_buff);
//	close(sock);
//
//	return 0;
//}
//
    static int
cfs_socket_init(const char *req_host, const short port)
{
    int sock;
    int res;
    char req_ip[32] = {0};
    res = cfs_parse_domain(req_host, req_ip);
    if (res == -1){
        cfs_log(ERR, "cfs_parse_domain error");
        return -1;
    }
    if ((sock = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0){
        cfs_log(ERR, "socket_init error");
        return -1;
    }

    struct sockaddr_in servaddr;
    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(port);
    servaddr.sin_addr.s_addr = inet_addr(req_ip);

    int i;
    for (i = 1; i <= 3; ++i)
    {
        if (connect(sock, (struct sockaddr*)&servaddr, sizeof(servaddr)) != -1){
            res = 0;
            break;
        }else{
            cfs_log(ERR, "connect server error %ds reconnect", i);
            sleep(i);
            res = -1;
        }
    }
    if (res == -1){
        cfs_log(ERR, "connect server error %s:%d failed", req_ip, port);
        close(sock);
        return -1;
    }

    return sock;
}

    static int 
cfs_parse_domain(const char *domain, char *dest)
{
    struct hostent *h;
    if ((h = gethostbyname(domain)) == NULL){
        return -1;
    }
    char ips[32] = {0};
    sprintf(ips, "%d.%d.%d.%d",
            (h->h_addr_list[0][0]&0x00ff),
            (h->h_addr_list[0][1]&0x00ff),
            (h->h_addr_list[0][2]&0x00ff),
            (h->h_addr_list[0][3]&0x00ff));
    strcpy(dest, ips);
    return 0;
}

    static void 
cfs_config_init(int argc, char **argv)
{
    char config_name[64] = {0};
    char *data           = NULL;
    int  p               = 0;

    strcpy(g_config->app_name, basename(argv[0]));
    if (argv[1] != NULL) {
        strcpy(config_name, argv[1]);
    }else {
        sprintf(config_name, "%s.conf", g_config->app_name);
    }

    dictionary *ini = iniparser_load(config_name);
    if (ini == NULL) {
        ERR_EXIT("load config file failed");
    }

    data = iniparser_getstr(ini, "origin:host");
    if (data != NULL) {
        g_config->origin->host = strdup(data);
        char pattern[] = "(\\d{1,2}|1\\d|2[0-4]\\d|25[0-5])\\.(\\d{1,2}|1\\d\\d|2[0-4]\\d|25[0-5])\\.(\\d{1,2}|1\\d\\d|2[0-4]\\d|25[0-5])\\.(\\d{1,2}|1\\d\\d|2[0-4]\\d|25[0-5])";
        int res = cfs_preg_match(pattern, data);
        if (res < 0) {
            if (res == PCRE_ERROR_NOMATCH){
                if (cfs_parse_domain(data, g_config->origin->ip) < 0){
                    ERR_EXIT("[config] Get origin:host ip error");
                }
            }
        }else{
            strcpy(g_config->origin->ip, data);
        }
    }else{
        ERR_EXIT("[config] Get origin:host error");
    }
    p = iniparser_getint(ini, "origin:port", -1);
    if (p == -1 || p > 65535){
        ERR_EXIT("[config] origin:port config error");
    }
    g_config->origin->port = p;
    data = iniparser_getstr(ini, "origin:user");
    if (data != NULL) {
        g_config->origin->username = strdup(data);
    }else{
        ERR_EXIT("[config] origin:user config error");
    }
    data = iniparser_getstr(ini, "origin:password");
    if (data != NULL) {
        g_config->origin->password = strdup(data);
    }else{
        ERR_EXIT("[config] origin:password config error");
    }
    data = iniparser_getstr(ini, "origin:download");
    if (data != NULL) {
        g_config->origin->api_download = strdup(data);
    }else{
        ERR_EXIT("[config] origin:download config error");
    }

    // origindownload rate
    data = iniparser_getstr(ini, "origin:rate");
    if (data != NULL) {
        strcpy(g_config->origin->rate, data);
    }else{
        memset(g_config->origin->rate, 0, sizeof(g_config->origin->rate));
    }
    data = iniparser_getstr(ini, "origin:cache");
    if (data != NULL) {
        strcpy(g_config->origin->cache, data);
    }else{
        memset(g_config->origin->cache, 0, sizeof(g_config->origin->cache));
    }

    p = iniparser_getint(ini, "global:log_size", -1);
    if (p == -1){
        ERR_EXIT("[config] global:log_size config error");
    }
    g_config->log_size = p * 1024 * 1024;
    data = iniparser_getstr(ini, "global:document");
    if (data != NULL) {
        g_config->work_dir = strdup(data);
        if (chdir(g_config->work_dir)){
            ERR_EXIT("change dir error");
        }
    }else{
        ERR_EXIT("[config] global:document config error");
    }
    data = iniparser_getstr(ini, "global:log_path");
    if (data != NULL) {
        g_config->log_path = strdup(data);
    }else{
        ERR_EXIT("[config] global:log_path config error");
    }
    p = iniparser_getint(ini, "global:threads", -1);
    if (p == -1) {
        ERR_EXIT("[config] global:threads config error");
    }
    g_config->thread_size = p;
    p = iniparser_getint(ini, "global:package_size", -1);
    if (p == -1) {
        ERR_EXIT("[config] global:package_size config error");
    }
    g_config->package_size = p * 1024;

    // run time user group
    data = iniparser_getstr(ini, "global:username");
    if (data != NULL) {
        g_config->username = strdup(data);
    }else{
        ERR_EXIT("[config] global:username config error");
    }

    data = iniparser_getstr(ini, "global:groupname");
    if (data != NULL) {
        g_config->groupname = strdup(data);
    }else{
        ERR_EXIT("[config] global:groupname config error");
    }

    p = iniparser_getint(ini, "global:io-utilization", -1);
    if (p == -1) {
        ERR_EXIT("[config] global:io utilization config error");
    }
    g_config->io_utilization = p;

    p = iniparser_getint(ini, "global:io-port", -1);
    if (p == -1) {
        ERR_EXIT("[config] global:io port config error");
    }
    g_config->io_port = p;

    data = iniparser_getstr(ini, "global:io-host");
    if (data != NULL) {
        // in_addr_t r1 = inet_addr(data);
        strcpy(g_config->io_host, data);
    }else{
        ERR_EXIT("[config] mysql:host config error");
    }

    p = iniparser_getint(ini, "global:server-port", -1); // addede for server-port
    if(p == -1)
    {
        ERR_EXIT("[config] global:server port config error");
    }
    g_config->server_port = p;

    data = iniparser_getstr(ini, "mysql:host");
    if (data != NULL) {
        g_config->mysql->host = strdup(data);
    }else{
        ERR_EXIT("[config] mysql:host config error");
    }

    data = iniparser_getstr(ini, "mysql:user");
    if (data != NULL) {
        g_config->mysql->username = strdup(data);
    }else{
        ERR_EXIT("[config] mysql:user config error");
    }

    data = iniparser_getstr(ini, "mysql:password");
    if (data != NULL) {
        g_config->mysql->password = strdup(data);
    }else{
        ERR_EXIT("[config] mysql:password config error");
    }

    p = iniparser_getint(ini, "mysql:port", -1);
    if (p == -1){
        ERR_EXIT("[config] mysql:port config error");
    }
    g_config->mysql->port = p;

    data = iniparser_getstr(ini, "mysql:database");
    if (data != NULL) {
        g_config->mysql->database = strdup(data);
    }else{
        ERR_EXIT("[config] mysql:database config error");
    }

    int cfs_id = 0; // added for 
    for(; cfs_id < 20; cfs_id++) //20 js just for test.
    {
        char cfs_item[64] = {0};
        cfs_cfsedge_config_t *cfsedge = (cfs_cfsedge_config_t*)malloc(sizeof(cfs_cfsedge_config_t));
        memset(cfsedge, 0 ,sizeof(cfs_cfsedge_config_t));

        sprintf(cfs_item,"cfs%d:host",cfs_id);
        data = iniparser_getstr(ini, cfs_item);
        if (data != NULL) {
            cfsedge->host = strdup(data);
        }else{
            char tmp[32] = {0};
            sprintf(tmp, "[config] cfs%d:host config error",cfs_id);
            cfs_free(cfsedge);
            break;
        }

        sprintf(cfs_item,"cfs%d:port",cfs_id);
        p = iniparser_getint(ini, cfs_item, -1);
        if (p != -1) {
            cfsedge->port = p;
        }else{
            char tmp[32] = {0};
            sprintf(tmp, "[config] cfs%d:port config error",cfs_id);
            ERR_EXIT(tmp);
        }

        sprintf(cfs_item,"cfs%d:key",cfs_id);
        data = iniparser_getstr(ini, cfs_item);
        if (data != NULL) {
            cfsedge->key = strdup(data);
        }else{
            char tmp[32] = {0};
            sprintf(tmp, "[config] cfs%d:key config error",cfs_id);
            ERR_EXIT(tmp);
        }

        if(cfs_id == 0)
        {
            g_config->cfsedge = cfsedge;
        }
        else
        {
            cfsedge->next = g_config->cfsedge->next;
            g_config->cfsedge->next = cfsedge;
        }
    }

    iniparser_freedict(ini);
}

    static void
cfs_init()
{
    signal(SIGUSR1, signal_hander);
    g_quit          = 0;
    g_download_stat = 0;
    g_user_agent    = NULL;

    pthread_mutex_init(&g_mutex_lock, NULL);
    struct utsname uts;
    if (uname(&uts) != 0) {
        char agent[64] = {0};
        sprintf(agent, "reacheyes cfs/%s (x86-Linux)", VERSION);
        g_user_agent = (char *)cfs_malloc(strlen(agent) + 1);
        strcpy(g_user_agent, agent);
    }else{
        char agent[64] = {0};
        sprintf(agent, "reacheyes cfs/%s", VERSION);
        int len = strlen(agent) + strlen(uts.sysname) + strlen(uts.release) + strlen(uts.machine) + strlen(uts.version) + 8;
        g_user_agent = (char *)cfs_malloc(len);
        sprintf(g_user_agent, "%s (%s %s %s; %s)", agent, uts.sysname, uts.release, uts.machine, uts.version);
    }

    struct passwd *pwd = getpwnam(g_config->username);
    if (pwd == NULL) {
        ERR_EXIT("config username is invalid");
    }

    struct group *grp = getgrnam(g_config->groupname);
    if (pwd == NULL) {
        ERR_EXIT("config groupname is invalid");
    }

    if (setegid(grp->gr_gid) == -1) {
        ERR_EXIT("set groupname is failed");
    }

    if (seteuid(pwd->pw_uid) == -1) {
        ERR_EXIT("set username is failed");
    }
    cfs_log(NOTICE, "Load Configure File Init Success");
}   

    static void     
cfs_disk_init(MYSQL *conn_ptr, cfs_disk_t **disk_buff, unsigned long *len)
{
    MYSQL_ROW result_row = NULL;
    char      sql[256]   = {0};
    long      size;
    int       i;
    long      length;

    MYSQL_RES *data_res = NULL;
    sprintf(sql, "SELECT device, work, priority, id FROM %s ORDER BY priority ASC", T_CDN_DISK);
    data_res = db_select(conn_ptr, sql);

    my_ulonglong rows = mysql_num_rows(data_res);
    length            = (long)rows;

    if (length != 0){
        // cfs_disk_t *disk = *disk_buff;
        cfs_disk_t *disk = (cfs_disk_t *)cfs_malloc(sizeof(cfs_disk_t) * length);
        for (i = 0; i < length; ++i)
        {
            result_row       = mysql_fetch_row(data_res);
            disk[i].priority = (unsigned short)atoi(result_row[2]);
            disk[i].id       = (unsigned int)atoi(result_row[3]);

            size             = strlen(result_row[1]);
            disk[i].work     = (char *)cfs_malloc(size + 1);

            strcpy(disk[i].work, result_row[1]);

            size             = strlen(result_row[0]);
            disk[i].device   = (char *)cfs_malloc(size + 1);

            strcpy(disk[i].device, result_row[0]);
        }
        *disk_buff = disk;
        *len = length;
    }
    db_free_result(data_res);
}

    static void*    
signal_hander(int num)
{
    g_quit = 1;
    cfs_log(NOTICE, "Receive exit signals USR1");
    return (void *)0;
}

    static int
cfs_log(int level, char *fmt, ...)
{
    va_list argptr;
    int     cnt;
    char    buffer[2000]    = {0};
    char    error_str[2048] = {0};
    char    *dir_path       = NULL;
    char    now_time[32]    = {0};
    struct  stat log_info   = {0};
    char    level_str[16]   = {0};
    int     len;

    cfs_get_localtime(now_time);
    len = strlen(g_config->log_path);
    dir_path = (char *)malloc(len + 1);
    if (dir_path == NULL) {
        syslog(LOG_USER|LOG_INFO, "Malloc Log Path Memory Space Error");
        return -1;
    }
    memset(dir_path, 0, len + 1);
    memcpy(dir_path, g_config->log_path, len);

    va_start(argptr, fmt);
    cnt = vsprintf(buffer, fmt, argptr);
    va_end(argptr);

    switch (level) {
        case ERR:
            strcpy(level_str, "ERROR");
            break;
        case WARN:
            strcpy(level_str, "WARN");
            break;
        case NOTICE:
            strcpy(level_str, "NOTICE");
            break;
        default:
            strcpy(level_str, "NOTICE");
            break;
    }

#if 0
    sprintf(error_str, "%s [%s]%d:%s(%s)--%s\n", now_time, level_str, __LINE__, __FUNCTION__, __FILE__, buffer);
#else
    sprintf(error_str, "%s [%s] %s\n", now_time, level_str, buffer);
#endif
    if (access(dirname(dir_path), 0) != 0) {
        if (cfs_mkrdir(dir_path, 0777) == -1) {
            ERR_EXIT("mkdir error");
            return -1;
        }
    }

    if ((stat(g_config->log_path, &log_info)) == 0){
        if (log_info.st_size > (1024000 * 10)){
            char tmp_log_name[1024] = {0};
            struct tm *now_date = NULL;
            time_t t = time(0);
            now_date = localtime(&t);
            sprintf(tmp_log_name, "%s.%04d%02d%02d%02d%02d%02d", g_config->log_path, now_date->tm_year + 1900, now_date->tm_mon + 1, now_date->tm_mday, \
                    now_date->tm_hour, now_date->tm_min, now_date->tm_sec);
            if (rename(g_config->log_path, tmp_log_name) == -1){
                syslog(LOG_USER|LOG_INFO, "log file rename file failed %s", strerror(errno));
            }
        }
    }
    FILE *fd = fopen(g_config->log_path, "a+");
    if (fd != NULL) {
        fwrite(error_str, strlen(error_str), 1, fd);
        fclose(fd);
    }else{
        syslog(LOG_USER|LOG_INFO, "Open Log File %s", strerror(errno));
    }
    cfs_free(dir_path);
    return(cnt);
}

    static int 
cfs_preg_match(char pattern[], char str[])
{
    pcre            *re;
    const char      *error;
    int             erroffset;
    int             res;
    // char            pattern   [] = "(\\d{1,2}|1\\d|2[0-4]\\d|25[0-5])\\.(\\d{1,2}|1\\d\\d|2[0-4]\\d|25[0-5])\\.(\\d{1,2}|1\\d\\d|2[0-4]\\d|25[0-5])\\.(\\d{1,2}|1\\d\\d|2[0-4]\\d|25[0-5])";
    re = pcre_compile(pattern, 0, &error, &erroffset, NULL);
    if (re == NULL) {
        cfs_log(WARN, "PCRE compilation failed at offset %d: %s", erroffset, error);
        return -1;
    }

    res = pcre_exec(re, NULL, str, strlen(str), 0, 0, NULL, 0);
    if (re != NULL) {
        free(re);
        re = NULL;
    }
    return res;
}

    static int 
cfs_get_localtime(char *date_str)
{
    time_t t = time(0);
    struct tm *now = localtime(&t);
    sprintf(date_str, "%04d-%02d-%02d %02d:%02d:%02d", now->tm_year + 1900, \
            now->tm_mon + 1, now->tm_mday, now->tm_hour, now->tm_min, now->tm_sec);
    return 0;
}

    void
cfs_split(const char *str, const char delimiter, char **key, char **val)
{
    int len;
    if (str == NULL)
        return;

    char *idx = strchr(str, delimiter);
    if (idx == NULL) {
        *key = str;
        val = NULL;
        return;
    }
    len = idx - str;
    char *key_ptr = (char *)malloc(len + 1);
    if (key_ptr == NULL) {
        *key = *val = NULL;
        return;
    }
    memset(key_ptr, 0, len + 1);
    strncpy(key_ptr, str, len);
    *key = key_ptr;

    len = strlen(++idx);
    char *val_ptr = (char *)malloc(len + 1);
    if (val_ptr == NULL) {
        *key = *val = NULL;
        return;
    }
    memset(val_ptr, 0, len + 1);
    strcpy(val_ptr, idx);
    *val = val_ptr;
}

    void 
cfs_free(void *ptr)
{
    if (ptr != NULL) {
        free(ptr);
        ptr = NULL;
    }
}

    void*
cfs_malloc(size_t size)
{
    void *ptr = malloc(size);
    if (ptr == NULL) {
        ERR_EXIT("malloc");
    }
    memset(ptr, 0, size);
    return ptr;
}

    static int
cfs_iostats(const int fd, cfs_disk_t *disk, const unsigned long len, cfs_disk_t **result)
{
    int   i, size;
    float io_util;
    char  cmd_buff[64] = {0};
    char  *chres       = NULL;
    char  *idx         = NULL;

    for  (i = 0; i < len; ++i)
    {
        sprintf(cmd_buff, "IOSTAT %s", disk[i].device);
        cfs_writen(fd, cmd_buff, strlen(cmd_buff));
        // send command
        cfs_readn(fd, &size, sizeof(size));

        // recv status
        char *buff = cfs_malloc(size + 1);
        cfs_readn(fd, buff, size);
        chres = cfs_ioerror(buff);
        // get iostat success ?
        if (chres != NULL) {
            cfs_log(WARN, "%s %s", cmd_buff, chres);
            cfs_free(chres);
            continue;
        }
        cfs_free(buff);

        // recv content
        cfs_readn(fd, &size, sizeof(size));
        buff = cfs_malloc(size + 1);
        cfs_readn(fd, buff, size);
        idx = strrchr(buff, ',');
        io_util = atof(++idx);
        cfs_free(buff);
        if (io_util >= g_config->io_utilization) {
            cfs_log(WARN, "'%s' utilization percent gt configure value %lf/%d", disk[i].device, io_util, g_config->io_utilization);
            continue;
        }else{
            cfs_log(NOTICE, "select device %s %lf. %s work directory write", disk[i].device, io_util, disk[i].work);
            *result = &disk[i];
            break;
        }
    }

    return 0;
}

    static char*
cfs_ioerror(char *buff)
{
    // [412, 'command param error']
    // [400, 'command is invalid']
    // [404, 'not found']
    // [301, 'data is null']
    // [300, 'success']
    char *key = NULL;
    char *val = NULL;

    cfs_split(buff, ' ', &key, &val);
    int code = atoi(key);
    cfs_free(key);

    switch (code) {
        case 300:
            cfs_free(val);
            break;
        case 301:
        case 404:
        case 400:
        case 412:
            return val;
            break;
    }

    return NULL;
}

void* cfs_server_run()
{
    int sockfd;
    int err;
    int i;
    int connfd;
    int fd_all[MAX_FD]; //保存所有描述符，用于select调用后，判断哪个可读

    //下面两个备份原因是select调用后，会发生变化，再次调用select前，需要重新赋值
    fd_set fd_read;                      //FD_SET数据备份
    fd_set fd_select;                    //用于select

    struct timeval timeout;           //超时时间备份
    struct timeval timeout_select;  //用于select

    struct sockaddr_in serv_addr;   //服务器地址
    struct sockaddr_in cli_addr;      //客户端地址
    socklen_t serv_len;
    socklen_t cli_len;

    //超时时间设置
    timeout.tv_sec = 10;
    timeout.tv_usec = 0;

    if ((sockfd = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0){
        cfs_log(ERR, "cfs server socket error");
        exit(-1);
    }
    cfs_log(NOTICE, "cfs server socket ok.");

    memset(&serv_addr, 0, sizeof(serv_addr));
    memset(&cli_addr, 0, sizeof(cli_addr));

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(g_config->server_port);
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    serv_len = sizeof(serv_addr);
    err = bind(sockfd, (struct sockaddr *)&serv_addr, serv_len);
    if(err < 0)
    {
        cfs_log(ERR, "fail to bind");
        exit(-1);
    }
    cfs_log(NOTICE, "cfs server bind ok.");

    err = listen(sockfd, LISTEN_NUMS);
    if(err < 0)
    {
        cfs_log(ERR, "fail to listen");
        exit(-1);
    }
    cfs_log(NOTICE, "cfs server listen ok.");

    //初始化fd_all数组
    memset(fd_all, -1, sizeof(fd_all));

    fd_all[0] = sockfd;   //第一个为监听套接字
    FD_ZERO(&fd_read);
    FD_SET(sockfd, &fd_read);  //将监听套接字加入fd_set

    char buf[4096];  //读写缓冲区
    int num;
    int maxfd;
    maxfd = fd_all[0];  //监听的最大套接字
    cfs_log(NOTICE, "cfs server fd=%d", sockfd);
    while(1)
    {
        //每次都需要重新赋值
        fd_select = fd_read;
        timeout_select = timeout;
        //err = select(maxfd+1, &fd_select, NULL, NULL, NULL);
        err = select(maxfd+1, &fd_select, NULL, NULL, (struct timeval *)&timeout_select);
        if(err < 0)
        {
            cfs_log(NOTICE,"fail to select");
            exit(-1);
        }
        else if(err == 0)
        {
            cfs_log(WARN, "select timeout");
            continue;
        }
        cfs_log(NOTICE, "select ,maxfd=%d", maxfd);
        //检测监听套接字是否可读
        if(FD_ISSET(sockfd, &fd_select))
        {
            //可读，证明有新客户端连接服务器      
            cli_len = sizeof(cli_addr);
            connfd = accept(sockfd, (struct sockaddr *)&cli_addr, &cli_len);
            if(connfd < 0)
            {
                cfs_log(ERR, "fail to accept");
                exit(-1);
            }
            cfs_log(NOTICE, "cfs server accept a new client.");

            //将新连接套接字加入fd_all及fd_read
            for(i=0; i<MAX_FD; i++)
            {
                if(fd_all[i] != -1)
                {
                    continue;
                }
                else
                {
                    fd_all[i] = connfd;
                    cfs_log(NOTICE, "cfs client fd_all[%d] join", i);
                    break;
                }
            }
            FD_SET(connfd, &fd_read);
            if(maxfd < connfd)
            {
                maxfd = connfd;  //更新maxfd
            }
        }
        //从1开始查看连接套接字是否可读，因为上面已经处理过0（sockfd）
        for(i=1; i<=maxfd; i++)
        {
            cfs_log(NOTICE, "FD_ISSET fd_all[%d]=%d is test.", i, fd_all[i]);
            if(FD_ISSET(fd_all[i], &fd_select))
            {
                cfs_log(NOTICE, "fd_all[%d] is ok", i);
                num = read(fd_all[i], buf, sizeof(buf));
                if(num > 0)
                {
                    if(strncmp("HEAD", buf, 4) == 0)
                    {
                        cfs_log(NOTICE, "client:fd_all[%d] HEAD CMD", i);
//                        http_cmd_head(fd_all[i], buf);
                        FD_CLR(fd_all[i], &fd_read);
                        close(fd_all[i]);
                        fd_all[i] = -1;
                    }
                    else if(strncmp("GET", buf, 3) == 0)
                    {
                        cfs_log(NOTICE, "client:fd_all[%d] GET CMD", i);
 //                       http_cmd_get(fd_all[i], buf);
                        FD_CLR(fd_all[i], &fd_read);
                        close(fd_all[i]);
                        fd_all[i] = -1;
                    }
                    else
                    {
                        cfs_log(WARN, "client:fd_all[%d] is not GET or HEAD CMD", i);
                    }
                }
            }
        }
    }
    exit(0);
}

//static int http_cmd_head(int sock, char *buf)
//{
//    char   *pos, *epos;
//    char    tmp[256]     = {0};
//    char   *rh           = NULL;
//    char    file_path[256]={0};
//    const char *split    = "\r\n\r\n";
//    rh = (char *)cfs_malloc(1024);
//
//    if( buf != NULL)
//    {
//        char *idx = strstr(buf, split);
//        if (idx != NULL) {
//            pos = strstr(buf, "HEAD");
//            if(pos != NULL)
//            {
//                pos++;
//            }
//            epos = strchr(pos, ' ');
//            if (epos != NULL){
//                strncpy(file_path, pos, epos-pos);
//                file_path[epos-pos] = '\0';
//            }
//        }
//    }
//
//    int file_size = 0;
//    int flag = access(file_path, R_OK); // -1:not exist; 0:exist
//    printf("client cfs request file_path=%s\n",file_path);
//    if(-1 == flag)
//    {
//        sprintf(rh, "HTTP/1.1 404 Not Found\r\n");
//    }
//    else
//    {
//        file_size = get_file_size(file_path);
//        sprintf(rh, "HTTP/1.1 200 OK\r\n");
//    }
//
//    sprintf(tmp, "Server: cfs server\r\n");
//    strcat(rh, tmp);
//    sprintf(tmp, "Accept-Range: bytes\r\n");
//    strcat(rh, tmp);
//    sprintf(tmp, "Content-Length: %d\r\n", file_size);
//    strcat(rh, tmp);
//    sprintf(tmp, "Connection: close\r\n\r\n");
//    strcat(rh, tmp);
//
//    cfs_writen(sock, rh, strlen(rh));
//    cfs_free(rh);
//    return 0;
//}

//static int http_cmd_get(int sock, char *buf)
//{
//    int     nres;
//    char   *pos, *epos;
//    char    tmp[256]     = {0};
//    char   *head_buff    = NULL;
//    char   *rh           = NULL;
//    const char *split    = "\r\n\r\n";
//
//    rh = (char *)cfs_malloc(1024);
//    head_buff = (char *)cfs_malloc(1024);
//
//    while((nres = cfs_readn(sock, head_buff, 1024)) != 0)
//    {
//        char *idx = strstr((char *)head_buff, split);
//        if (idx != NULL) {
//            pos = strstr(head_buff, "GET");
//            if(pos != NULL)
//            {
//                pos++;
//            }
//            epos = strchr(pos, ' ');
//            if (epos != NULL){
//                strncpy(header->file_path, pos, epos-pos);
//            }
//
//            pos = strcasestr(head_buff, "Range:");
//            if (pos != NULL){
//                //		header->accept_ranges = 1; //TODO:
//            }
//        }
//    }
//
//    FILE *filep = fopen(header->file_path, "r");
//    if(NULL == filep)
//    {
//        cfs_log(ERR, "fopen %s error", header->file_path);
//        exit(-1);
//    }
//
//    char rw_buf[1024]={0};
//    char *prw;
//    unsigned long data_size = get_file_size(header->file_path);
//    unsigned long ulres = 0;
//    unsigned long data_rds = 0;
//    while(ulres < data_size)  // send request files 
//    {
//        sprintf(tmp, "HTTP/1.1 200 OK\r\n");
//        strcat(rh, tmp);
//        sprintf(tmp, "Server: cfs server\r\n");
//        strcat(rh, tmp);
//        sprintf(tmp, "Content-Type: text/html\r\n");
//        strcat(rh, tmp);
//        sprintf(tmp, "Accept-Range: bytes\r\n");
//        strcat(rh, tmp);
//
//        prw = rh + strlen(rh);	
//        if((data_rds = fread(rw_buf, sizeof(rh) - strlen(rh), 1, filep)) == -1)
//        {
//            cfs_log(ERR, "read file error");
//        }
//        sprintf(tmp, "Content-length: %lu\r\n",data_rds);
//        strcat(rh, tmp); 
//
//        ulres = ulres + data_rds;
//        if(ulres >= data_size)
//        {
//            sprintf(tmp, "Connection: close\r\n\r\n");
//        }
//        else
//        {
//            sprintf(tmp, "Connection: Keep-Alive\r\n\r\n");
//        }
//        strcat(rh, tmp);
//
//        strncpy(prw, rw_buf, data_rds);
//        cfs_writen(sock, rh, strlen(rh));
//    }
//    cfs_free(head_buff);
//    cfs_free(rh);
//
//    return 0;
//}

//static unsigned long get_file_size(const char *path)
//{
//    unsigned long filesize = -1;	
//    struct stat statbuff;
//    if(stat(path, &statbuff) < 0){
//        return filesize;
//    }else{
//        filesize = statbuff.st_size;
//    }
//    return filesize;
//}
