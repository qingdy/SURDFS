#!/bin/sh

ulimit -c unlimited
ulimit -s 20480

exeFile=`ls |grep bladestore_client |grep -v grep`
if [ -z "${exeFile}" ]; then
    echo "exe file ${exeFile} not exist"
    exit 1
fi

## judge if it already exist
isExist=`ps aux |grep ${exeFile} |grep -v grep`
if [ ! -z "${isExist}" ]; then
    echo "process ${exeFile} already exist"
    exit 1
fi 

nohup ./${exeFile} >/dev/null 2>&1 &

## judge if it's successfully started
isExist=`ps aux |grep ${exeFile} |grep -v grep`
if [ -z "${isExist}" ]; then
    echo "start process ${exeFile} failed"
else
    echo "start process ${exeFile} success"
fi 
