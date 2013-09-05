#!/bin/sh


exeFile=`ls |grep bladestore_client |grep -v grep`

###TODO check if it exist
## kill exefile
if [ -z "${exeFile}" ]; then
   echo "${exeFile} not exist!"
   exit 1
else
   killall ${exeFile}
fi

### check if it's been killed
isExist=`ps aux |grep ${exeFile} |grep -v grep`
if [ -z "${isExist}" ]; then
    echo "stop sucess!"
else
    echo "stop failed!"
fi
