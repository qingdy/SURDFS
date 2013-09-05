#!/bin/sh
#### 所写的本地文件为writefile，读出来后的数据文件为readfile。file64m为大小为64m的文件，file32m为一个大小为32m的文件。
#### filenotchunk为一个大小不是16k整数倍的文件。这些在测试之前要贮备好。
####  启动一个ns，三个ds。

#【1】 测试正常读写，先写一个block之后读出来，比较MD5值，比对数据是否一致
echo "start test 1:正常写"
./testwriteblock /1
./testreadblock /1
md5sum writefile
md5sum readfile
echo "finish test 1 :正常写!\n\n"

###$$$ testreadpacket的使用：usage:  ./testreadpacket  block_id  block_version  blk_offset  request_len  dataserver(ip:port)

#【2】blk_offset的值不在0-64M范围内（高于64M和低于0M）其中block_id为上一测试案例中的block_id,观察ds的日志显示read request length invalid，并发送相应的错误码给client。
echo "start test 2 :blk_offset的值不在0-64M范围内"
./testreadpacket 3 1 64000000000   1024000   10.10.72.115:8877
echo "finish test 2 :blk_offset的值不在0-64M范围内!\n\n"

#【3】client请求的数据长度不合法，block_offset+request_len >64M 观察ds的日志显示read request length invalid，并发送相应的错误码给client。
echo "start test 3 :request_len  不合法"
./testreadpacket 3 1  0  64000000000     10.10.72.115:8877
echo "finish test 3 :request_len  不合法!\n\n"

#【4】请求读一个DataServer上不存在的Block,观察ds的日志显示ret_code:4003,即block在ds上不存在，并发送相应的错误码给client。
echo "start test 4 :请求读一个DataServer上不存在的Block"
./testreadpacket 4 1  0  1024000     10.10.72.115:8877
echo "finish test 4 :请求读一个DataServer上不存在的Block\n\n"

#【5】请求读一个DataServer上存在的Block, 但是版本不符合，观察ds的日志显示ret_code=4006:RET_READ_VERSION_NOT_MATCH,并发送相应的错误码给client。
echo "start test 5 :请求读一个DataServe存在的Block,但是版本不匹配"
./testreadpacket 3 2  0  1024000     10.10.72.115:8877
echo "finish test 5 :请求读一个DataServer存在的Block,但是版本不匹配"

#【6】请求读一个DataServer上的Block,但是其数据文件和校验文件不匹配观察ds的日志显示crc校验不正确，并向ns上报错误块.并发送相应的错误码给client。
echo "start test 6 :请求读一个DataServer存在的Block,但是其数据文件和校验文件不匹配"
./testreadpacket  3  1  0  1024000     10.10.72.115:8877
echo "finish test 6 :请求读一个DataServer存在的Block,但是其数据文件和校验文件不匹配k\n\n"

#【7】请求读一个DataServer上的Block,但是其数据文件不存在。观察ds的日志显示ret_code：4004：block或者文件不存在，并向ns上报错误块.并发送相应的错误码给client。
echo "start test 7 :请求读一个DataServer存在的Block,但是其数据文件不存在"
./testreadpacket  3  1  0  1024000     10.10.72.115:8877
echo "finish test 7 :请求读一个DataServer存在的Block,但是其数据文件不存在\n\n"

#【8】请求读一个DataServer上的Block,但是其校验文件不存在。观察ds的日志显示ret_code：4004：block或者文件不存在，并向ns上报错误块.并发送相应的错误码给client。
echo "start test 8 :请求读一个DataServer存在的Block,但是其校验文件不存在"
./testreadpacket  3  1  0  1024000     10.10.72.115:8877
echo "finish test 8 :请求读一个DataServer存在的Block,但是其校验文件不存在\n\n"
