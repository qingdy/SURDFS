#本文件中的测试案例针对测试文档《Dataserver测试用例报告》,在测试之前重启ns，删除所有的ns_log.
#准备三个数据文件filenotchunk、file32m和file64m，大小分别为33554401B（大小不是16K的整数倍即可）、32M和64M。后续写测试会用到这三个文件。 writefile是写所用到的数据文件，readfile是读出来的block数据文件
#异常测试
#3.1【普通写】功能测试案例1 和测试案例3  DataServer写时网络异常以及宕机的情况 
echo "test case 1.2.2 :读取一个Block，但该Block在DS上不存在\n"
./testwriteblock  /31  #写的中途关掉ds1
./testwriteblock  /32  #写的中途关掉ds2
./testwriteblock  /33  #写的中途关掉ds3

#3.4【安全写】功能测试案例4 和测试案例6  DataServer写时网络异常以及宕机的情况
./testsafewriteblock  /35  #写的中途关掉ds1
./testsafewriteblock  /36  #写的中途关掉ds2
./testsafewriteblock  /37  #写的中途关掉ds3

#3.8  功能测试案例8  DataServer在建立流水线超时后收到建立流水线回复包:修改dataserver端的代码，在流水线的最后一个ds建立流水线（writepipeline）处理逻辑中修改，在向前发回复包的时候sleep一下。
./testwriteblock  /38  #
./testsafewriteblock  /39 #
