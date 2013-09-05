#!/bin/sh
###在测试建立pipeline的整个流程中，不与nameserver做交互，直接和ds建立连接和发包,正常的流程在测试writepipeline的时候就测试。这里只测试一个异常情况。
###启动一个ns，并清空ns上的元数据信息。启动三个ds。这里启动的ns：10.10.72.116:9000，三个ds的ip依次是10.10.72.115:8877 ；10.10.72:114:8877 ；10.10.72.124:8877
###$$$用法：./testwritepipelinereply [block_id]  [block_version]  [ret_code]  [dataserver] 
#【1】 测试DataServer收到建立流水线返回包，但并没有发出过对应此返回包的建立流水线的包。
echo "start test 1:正常建立pipeline"
./testwritepipelinereply 4 5 1 10.10.72.115:8877 
echo "finish test 1 :正常建立pipeline!\n\n"

