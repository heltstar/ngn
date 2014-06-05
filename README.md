
<<<<<<< HEAD
cdn server download app  
sudo apt-get install libmysqld-dev  
   
=======
<<<<<<< HEAD
cdn server download app   
sudo apt-get install libmysqld-dev  

>>>>>>> dev
TODO:  
1、文件大小分块，每块4M  
2、每块对应一个下载线程  
3、每个node节点对应若干下载线程  
4、初始文件大小从origin获取，可借鉴之前存在的http获取文件大小，同时可以判断所请求文件是否存在，减少其它node节点连接开销  
<<<<<<< HEAD
5、node服务器 端可接受多个连接，进行处理。,先完成客户端分块下载功能，多线程下载功能，后再逐渐优化服务器端功能.  
6、服务器端可由目前fork改成线程方式，或者线程池方式（以后有精力时间在来完善).  
  
git learn websit:  
http://www.liaoxuefeng.com/wiki/0013739516305929606dd18361248578c67b8067c8c017b000/00137586810169600f39e17409a4358b1ac0d3621356287000  

=======
5、node服务器 端可接受多个连接，进行处理。,先完成客户端分块下载功能，多线程下载功能，后再逐渐优化服务器端功能.   
6、服务器端可由目前fork改成线程方式，或者线程池方式（以后有精力时间在来完善).   
  
git learn websit:  
http://www.liaoxuefeng.com/wiki/0013739516305929606dd18361248578c67b8067c8c017b000/00137586810169600f39e17409a4358b1ac0d3621356287000  
=======
cdn server download app    
sudo apt-get install libmysqld-dev    
     
TODO:    
1、文件大小分块，每块4M   
2、每块对应一个下载线程    
3、每个node节点对应若干下载线程   
4、初始文件大小从origin获取，可借鉴之前存在的http获取文件大小，同时可以判断所请求文件是否存在，减少其它node节点连接开销    
5、node服务器 端可接受多个连接，进行处理。,先完成客户端分块下载功能，多线程下载功能，后再逐渐优化服务器端功能.    
6、服务器端可由目前fork改成线程方式，或者线程池方式（以后有精力时间在来完善).    
7、每个文件块编号，如果某节点下载该节点失败，则标记该文件块编号对应值为未下载，其它节点可接过来接着下载。  
8、如果第一次发现某个node节点没有所要下载文件，则需要标记该节点，之后不再在此节点尝试下载连接,防止重复浪费。 
     
git learn websit:   
http://www.liaoxuefeng.com/wiki/0013739516305929606dd18361248578c67b8067c8c017b000/00137586810169600f39e17409a4358b1ac0d3621356287000   
>>>>>>> test
>>>>>>> dev
