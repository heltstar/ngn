CC=gcc
CFLAGS=-Wall -g
ALL=cfs
all:$(ALL)
OBJS=dictionary.o strlib.o iniparser.o cfs_mysql.o urlencode.o cfs.o

.c.o: 
	$(CC) $(CFLAGS) -c $<

cfs:$(OBJS)
	$(CC) $(CFLAGS) $^ -o $@ -g -lmysqlclient -lcrypto -lpcre -L /usr/lib/mysql/ -I /usr/include/mysql/ 

cfs.o: cfs.c 
	$(CC) $(CFLAGS) -c cfs.c -lmysqlclient -L /usr/lib/mysql/ -I /usr/include/mysql/ 	

cfs_mysql.o: cfs_mysql.c cfs_mysql.h
	$(CC) $(CFLAGS) -c cfs_mysql.c -lmysqlclient -L /usr/lib/mysql/ -I /usr/include/mysql/ 	

clean:
	rm -f $(ALL) *.o
