#!/bin/sh
###在测试建立pipeline的整个流程中，不与nameserver做交互，直接和ds建立连接和发包,这个测试中需要比对版本，所以要重新编译ns，让其默认的block_version为5。
###启动一个ns，并清空ns上的元数据信息。启动三个ds。这里启动的ns：10.10.72.116:9000，三个ds的ip依次是10.10.72.115:8877 ；10.10.72:114:8877 ；10.10.72.124:8877
###$$$用法：./testwritepipeline [file_id]  [block_id]  [block_version]  [mode]  [ds1]  [ds2]  [ds3]
#【1.1】 测试正常写pipeline,观察115的日志，可以看到它发pipeline建立成功的包给client了。
echo "start test 1:正常建立pipeline"
./testwritepipeline 4 4 5 0 10.10.72.115:8877  10.10.72.114:8877  10.10.72.124:8877
echo "finish test 1 :正常建立pipeline!\n\n"

#【1.2】 测试安全写pipeline,观察115的日志，可以看到它发pipeline建立成功的包给client了。
echo "start test 1:正常建立pipeline"
./testwritepipeline 4 4 5 1 10.10.72.115:8877  10.10.72.114:8877  10.10.72.124:8877
echo "finish test 1 :正常建立pipeline!\n\n"

#【2.0】为测试用例2,3做准备,会生成一个file_id为3.block_id为3，version为5的block。
echo "start test 2:准备工作，写一个block"
./testwriteblock /1
./testwriteblock /2
./testwriteblock /3
echo "start test 2:准备工作，写一个block\n\n"

#【2.1】DS收到建立"普通写"流水线请求包中，本地存放的有和这个流水线请求中相同block_id的Block，且本地的version 大于等于请求写入本地block的version,日志中会打印相应的错误信息，并返回错误码给client,且不继续建立流水线。
echo "start test 2:普通写pipeline本地的version 大于等于请求写入本地block的version"
./testwritepipeline 3 3 4  0 10.10.72.115:8877  10.10.72.114:8877  10.10.72.124:8877
echo "finish test 2:普通写pipeline本地的version 大于等于请求写入本地block的version!\n\n"

#【2.2】DS收到建立"安全写"流水线请求包中，本地存放的有和这个流水线请求中相同block_id的Block，且本地的version 大于等于请求写入本地block的version,日志中会打印相应的错误信息，并返回错误码给client,且不继续建立流水线。
echo "start test 2:安全写pipeline本地的version 大于等于请求写入本地block的version"
./testwritepipeline 3 3 4  1 10.10.72.115:8877  10.10.72.114:8877  10.10.72.124:8877
echo "finish test 2:安全写pipeline本地的version 大于等于请求写入本地block的version!\n\n"


#【3.1】DS收到建立"普通写"流水线请求包中，本地存放的有和这个流水线请求中相同block_id的Block，且本地的version 小于请求写入本地block的version,删除本地的block，继续建立流水线。
echo "start test 3:普通写pipeline本地的version 小于请求写入本地block的version"
./testwritepipeline 3 3 6  0  10.10.72.115:8877  10.10.72.114:8877  10.10.72.124:8877
echo "finish test 3:普通写pipeline本地的version 小于请求写入本地block的version!\n\n"

#【3.2】DS收到建立"安全写"流水线请求包中，本地存放的有和这个流水线请求中相同block_id的Block，且本地的version 小于请求写入本地block的version,删除本地的block，继续建立流水线。
echo "start test 3:安全写pipeline本地的version 小于请求写入本地block的version"
./testwritepipeline 4 4 6  1 10.10.72.115:8877  10.10.72.114:8877  10.10.72.124:8877
echo "finish test 3:安全写pipeline本地的version 小于请求写入本地block的version!\n\n"

#【4.1】DS收到建立普通写流水线请求包中，收到的ds_id与自己的ds_id不匹配,观察115的日志，可以看到它有错误日志，并发送包含错误码的信息给client。这里修改了代码。将包发给流水线中的第二个ds，观察第二个ds的日志。
echo "start test 4:普通写收到的ds_id与自己的ds_id不匹配"
./testwritepipeline_1 5 5 5  0 10.10.72.115:8877  10.10.72.114:8877  10.10.72.124:8877
echo "finish test 4:普通写收到的ds_id与自己的ds_id不匹配!\n\n"

#【4.2】DS收到建立普通写流水线请求包中，收到的ds_id与自己的ds_id不匹配,观察115的日志，可以看到它有错误日志，并发送包含错误码的信息给client。这里修改了代码。将包发给流水线中的第二个ds，观察第二个ds的日志。
echo "start test 4:安全写收到的ds_id与自己的ds_id不匹配"
./testwritepipeline_1 5 5 5  1 10.10.72.115:8877  10.10.72.114:8877  10.10.72.124:8877
echo "finish test 4 :安全写收到的ds_id与自己的ds_id不匹配"

#【5.0】XXX测试本地操作失败需要修改ds端的代码，人为的使本地的preparewrite返回错误码
#【5.1】该DS处于普通写流水线的最后一个DS，收到正确的建立流水线请求，但在做本地新建block文件等环节(preparewrite())操作失败,修改ds端代码，重编后先部署到流水线中的最后一个ds。观察流水线最后一个ds的日志。
echo "start test 5:last ds普通写本地新建block文件等环节(preparewrite())操作失败"
./testwritepipeline 6 6 5  0 10.10.72.115:8877  10.10.72.114:8877  10.10.72.124:8877
echo "finish test 5:last ds普通写本地新建block文件等环节(preparewrite())操作失败!\n\n"

#【5.2】该DS处于安全写流水线的最后一个DS，收到正确的建立流水线请求，但在做本地新建block文件等环节(preparewrite())操作失败,修改ds端代码，重编后先部署到流水线中的最后一个ds。观察流水线最后一个ds的日志。
echo "start test 5:last ds安全写本地新建block文件等环节(preparewrite())操作失败"
./testwritepipeline 7 7 5  1 10.10.72.115:8877  10.10.72.114:8877  10.10.72.124:8877
echo "finish test 5:last ds安全写本地新建block文件等环节(preparewrite())操作失败!\n\n"

#【6.0】XXX测试本地操作失败需要修改ds端的代码，人为的使本地的preparewrite返回错误码
#【6.1】该DS不处于普通写流水线的最后一个DS，收到正确的建立流水线请求，但在做本地新建block文件等环节(preparewrite())操作失败,修改ds端代码，重编后先部署到流水线中的ds中。观察流水线第一个ds的日志。
echo "start test 6:非last ds普通写本地新建block文件等环节(preparewrite())操作失败"
./testwritepipeline 8 8 5  0 10.10.72.115:8877  10.10.72.114:8877  10.10.72.124:8877
echo "finish test 6 :非last ds普通写本地新建block文件等环节(preparewrite())操作失败!\n\n"

#【6.2】该DS不处于安全写流水线的最后一个DS，收到正确的建立流水线请求，但在做本地新建block文件等环节(preparewrite())操作失败,修改ds端代码，重编后先部署到流水线中的ds中。观察流水线第一个ds的日志。
echo "start test 6:非last ds安全写本地新建block文件等环节(preparewrite())操作失败"
./testwritepipeline 9 9  5  1 10.10.72.115:8877  10.10.72.114:8877  10.10.72.124:8877
echo "finish test 6 :非last ds安全写本地新建block文件等环节(preparewrite())操作失败!\n\n"
