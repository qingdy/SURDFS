#!/bin/sh
###在测试writepacketreply的整个流程中，除了写完整的文件，其他操作不与nameserver做交互，直接和ds建立连接和发包,这个测试中需要比对版本，所以要重新编译ns，让其默认的block_version为5。
###启动一个ns，并清空ns上的元数据信息。启动三个ds。这里启动的ns：10.10.72.116:9000，三个ds的ip依次是10.10.72.115:8877 ；10.10.72:114:8877 ；10.10.72.124:8877
###$$$用法：./testwritepacket [file_id]  [block_id]  [block_version] [block_offset] [data_length] [mode]  [flag] [ds1]  [ds2]  [ds3]
### flag:
###  0:normal case
###  1:data_length not match checksum_len case.
###  2:data_ not match checksum case.
###  3:block_offset is not in [0,64M]
#【1.1】(普通写)流水线前继DataServer未写完时，收到了后面的DataServer写成功的回复。然后写超时DataServer在超时之前写完成.观察流水线中倒数第二个节点的日志。
echo "start test 1:普通写先处理后继节点的回复包，再完成本地操作 "
./testwritepacket 5 5 5  0  1048576 0 0 10.10.72.115:8877  10.10.72.114:8877  10.10.72.124:8877
echo "finish test 1:普通写先处理后继节点的回复包，再完成本地操作!\n\n"

#【1.2】(安全写)流水线前继DataServer未写完时，收到了后面的DataServer写成功的回复。然后写超时DataServer在超时之前写完成.观察流水线中倒数第二个节点的日志。
echo "start test 1:安全写先处理后继节点的回复包，再完成本地操作"
./testwritepacket 6 6 5   0  1048576 1 0 10.10.72.115:8877  10.10.72.114:8877  10.10.72.124:8877
echo "finish test 1:安全写先处理后继节点的回复包，再完成本地操作!\n\n"

#【2.0】准备工作 为了超时必须修改代码，在处理writepacketreply的时候往前继节点发包之前sleep（10）。将DS_WRITE_TIMEOUT变量设为5s
#【2.1】(普通写)流水线前继DataServer处理本地写操作完成，但是在超时时间间隔内没有收到后面的DataServer写成功的回复。观察流水线中第一个节点的日志。
echo "start test 2:普通写先处理后继节点的回复包，再完成本地操作 "
./testwritepacket 7 7 5  0  1048576 0 0 10.10.72.115:8877  10.10.72.114:8877  10.10.72.124:8877
echo "finish test 2:普通写先处理后继节点的回复包，再完成本地操作!\n\n"

#【2.2】(安全写)流水线前继DataServer处理本地写操作完成，但是在超时时间间隔内没有收到后面的DataServer写成功的回复。观察流水线中第一个节点的日志。
echo "start test 2:安全写先处理后继节点的回复包，再完成本地操作"
./testwritepacket 8 8  5   0  1048576 1 0 10.10.72.115:8877  10.10.72.114:8877  10.10.72.124:8877
echo "finish test 2:安全写先处理后继节点的回复包，再完成本地操作!\n\n"

