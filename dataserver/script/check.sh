#!/bin/sh


exeFile=`ls |grep bladestore_dataserver |grep -v grep`

###TODO check if it exist
## kill exefile
if [ -z "${exeFile}" ]; then
   echo "${exeFile} not exist!"
   exit 1
fi

### check if it's been killed
isExist=`ps aux |grep ${exeFile} |grep -v grep`
if [ -n "${isExist}" ]; then
    echo "dataserver ${exefile} is alive!"
else
    echo "dataserver ${exefile} is dead!"
fi
