#!/bin/sh
###在测试writepacket的整个流程中，不与nameserver做交互，直接和ds建立连接和发包,这个测试中需要比对版本，所以要重新编译ns，让其默认的block_version为5。
###启动一个ns，并清空ns上的元数据信息。启动三个ds。这里启动的ns：10.10.72.116:9000，三个ds的ip依次是10.10.72.115:8877 ；10.10.72:114:8877 ；10.10.72.124:8877
###$$$用法：./testwritepacket [file_id]  [block_id]  [block_version] [block_offset] [data_length] [mode]  [flag] [ds1]  [ds2]  [ds3]
### flag:
###  0:normal case
###  1:data_length not match checksum_len case.
###  2:data_ not match checksum case.
###  3:block_offset is not in [0,64M]

#【1.1】(普通写) 测试正常writepacket,观察三个ds的的日志。将写进去的数据读出来做校验，一致。
echo "start test 1:普通写正常writepacket"
./testwriteblock  /1
./testreadblock  /1
md5sum writefile
md5sum readfile
echo "finish test 1 :普通写正常writepacket!\n\n"

#【1.2】(安全写) 测试正常writepacket,观察三个ds的的日志。将写进去的数据读出来做校验，一致。
echo "start test 1:安全写正常writepacket"
./testsafewriteblock  /2
./testreadblock  /1
md5sum writefile
md5sum readfile
echo "finish test 1 :安全写正常writepacket!\n\n"

#【2.1】普通写checksum_的长度和与所传的data的长度不匹配 ,观察ds 115的的日志。将flag 置为1，人为的使checksum_的长度和与所传的data的长度不匹配，观察流水线第一个ds的日志。
echo "start test 2:普通写checksum_的长度和与所传的data的长度不匹配 "
./testwritepacket 5 5 5  0  1048576 0 1 10.10.72.115:8877  10.10.72.114:8877  10.10.72.124:8877
echo "finish test 2:普通写checksum_的长度和与所传的data的长度不匹配 !\n\n"

#【2.2】安全写checksum_的长度和与所传的data的长度不匹配 ,观察ds 115的的日志。将flag 置为1，人为的使checksum_的长度和与所传的data的长度不匹配,观察流水线第一个ds的日志。
echo "start test 2:安全写checksum_的长度和与所传的data的长度不匹配"
./testwritepacket 6 6 5   0  1048576 1 1 10.10.72.115:8877  10.10.72.114:8877  10.10.72.124:8877
echo "finish test 2:安全写checksum_的长度和与所传的data的长度不匹配!\n\n"


#【3.1】普通写请求包checksum和data数据的校验不匹配 ,观察流水线最后一个ds的的日志。将flag 置为2，人为使的请求包checksum和data数据的校验不匹配。
echo "start test 3:普通写请求包checksum和data数据的校验不匹配"
./testwritepacket 7 7 5  0  1048576 0 2 10.10.72.115:8877  10.10.72.114:8877  10.10.72.124:8877
echo "finish test 3:普通写请求包checksum和data数据的校验不匹配!\n\n"

#【3.2】安全写请求包checksum和data数据的校验不匹配,观察流水线最后一个ds的的日志。将flag 置为2，人为使的请求包checksum和data数据的校验不匹配。
echo "start test 3:安全写请求包checksum和data数据的校验不匹配"
./testwritepacket 8 8  5   0  1048576 1 2 10.10.72.115:8877  10.10.72.114:8877  10.10.72.124:8877
echo "finish test 3:安全写请求包checksum和data数据的校验不匹配!\n\n"

#【4.1】(普通写)请求包里的block_offset不在[0，64M]范围内,观察流水线第一个ds的日志。将flag 置为3，人为使的请求包checksum和data数据的校验不匹配。
echo "start test 3:普通写checksum_的长度和与所传的data的长度不匹配 "
./testwritepacket 9 9  5  0  1048576 0 3 10.10.72.115:8877  10.10.72.114:8877  10.10.72.124:8877
echo "finish test 3:普通写checksum_的长度和与所传的data的长度不匹配 !\n\n"

#【4.2】(安全写)请求包里的block_offset不在[0，64M]范围内,观察流水线第一个ds的日志。将flag 置为3，人为使的请求包checksum和data数据的校验不匹配。
echo "start test 3:安全写请求包checksum和data数据的校验不匹配"
./testwritepacket 10 10  5   0  1048576 1 3 10.10.72.115:8877  10.10.72.114:8877  10.10.72.124:8877
echo "finish test 3:安全写请求包checksum和data数据的校验不匹配!\n\n"

#【5.1】(普通写) 测试正常writepacket,突然挂掉流水线中的一个ds,观察被挂掉ds前一个ds的日志,显示des_id not match error，对后继ds的连接断了，所以peer_id不相等了。
echo "start test 1:测试正常writepacket,突然挂掉流水线中的一个ds"
./testwriteblock  /12
echo "finish test 1:测试正常writepacket,突然挂掉流水线中的一个ds\n\n"

#【5.2】(安全写) 测试正常writepacket,突然挂掉流水线中的一个ds,观察被挂掉ds前一个ds的日志,显示des_id not match error，对后继ds的连接断了，所以peer_id不相等了。
echo "start test 1:普通写正常writepacket"
./testsafewriteblock  /13
echo "finish test 1 :普通写正常writepacket!\n\n"

