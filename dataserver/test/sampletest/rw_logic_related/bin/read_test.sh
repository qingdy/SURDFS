#本文件中的测试案例针对测试文档《Dataserver测试用例报告》,在测试之前重启ns，删除所有的ns_log.
#准备三个数据文件filenotchunk、file32m和file64m，大小分别为33554401B（大小不是16K的整数倍即可）、32M和64M。后续写测试会用到这三个文件。 writefile是写所用到的数据文件，readfile是读出来的block数据文件
#读功能测试
#其中正常情况下的读功能测试已经在写中测试过了，这里只测试异常情况
#1.3.5 功能测试案例  读数据块 测试案例5 :读取一个Block，但该Block在DS上不存在
echo "test case 1.2.2 :读取一个Block，但该Block在DS上不存在\n"
./testreadblock  /1  ;

#1.3.6 功能测试案例  写数据块 测试案例6 :读取一个Block，该Block在DS存在，其元数据文件不存在
echo "\ntest case 1.2.3  32M的数据块\n"
./testreadblock  /2  ;

#1.3.7 功能测试案例  写数据块 测试案例7 

echo "\ntest case 1.2.4  64M的数据块 \n"
./testreadblock  /6  ;

#1.2.5 功能测试案例  写数据块 测试案例5 开始写后人为的断掉client
echo "\ntest case 1.2.4  64M的数据块 \n"
cp  file64m writefile;
./testwriteblock  /7 ;
./testreadblock  /7 ;
md5sum writefile ;
md5sum  readfile;
./testsafewriteblock  /8  ;
./testreadblock  /8  ;
md5sum writefile ;
md5sum  readfile;


