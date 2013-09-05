#!/bin/sh

### start dataservices of one group
source ./FuncDef.sh
source ./conf.txt

idx=0
ssh_port=22
while (( ${idx} < ${count}))
do
    ip_ds=${ip[${idx}]}
    echo "${ip_ds}"
    dest_path_ds=${path[${idx}]}
    user_ds=${user[${idx}]}
    passwd_ds=${passwd[${idx}]}

    ./run_remote.sh $ip_ds $ssh_port $user_ds $passwd_ds "cd $dest_path_ds/bin;. check.sh"
    let idx+=1
done
