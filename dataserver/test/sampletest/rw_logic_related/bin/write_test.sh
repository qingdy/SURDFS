#本文件中的测试案例针对测试文档《Dataserver测试用例报告》,在测试之前重启ns，删除所有的ns_log.
#准备三个数据文件filenotchunk、file32m和file64m，大小分别为33554401B（大小不是16K的整数倍即可）、32M和64M。后续写测试会用到这三个文件。 writefile是写所用到的数据文件，readfile是读出来的block数据文件
#写功能测试
#1.2.2 功能测试案例  写数据块 测试案例2
echo "test case 1.2.2 非16K整数倍大小的的数据块\n"
cp  filenotchunk  writefile;
./testwriteblock  /1 ;
./testreadblock  /1  ;
md5sum writefile ;
md5sum readfile;
./testsafewriteblock  /2 ;
./testreadblock  /2  ;
md5sum writefile ;
md5sum  readfile;
#1.2.3 功能测试案例  写数据块 测试案例3
echo "\ntest case 1.2.3  32M的数据块\n"
cp  file32m writefile;
./testwriteblock  /3 ;
./testreadblock  /3  ;
md5sum writefile ;
md5sum  readfile;
./testsafewriteblock  /4  ;
./testreadblock  /4 ;
md5sum writefile ;
md5sum  readfile;
#1.2.4 功能测试案例  写数据块 测试案例4
echo "\ntest case 1.2.4  64M的数据块 \n"
cp  file64m writefile;
./testwriteblock  /5 ;
./testreadblock  /5 ;
md5sum writefile ;
md5sum  readfile;
./testsafewriteblock  /6  ;
./testreadblock  /6  ;
md5sum writefile ;
md5sum  readfile;

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


