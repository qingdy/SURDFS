#!/bin/bash

source ./conf.txt
source ./FuncDef.sh
source ./install_fun.sh

if [ $count -gt 0 ]; then
    echo "count: ${count}"
else
    echo "count invalid"
    exit 1
fi

ssh_port=22

idx=0
while (( ${idx} < ${count}))
do
    ip_ds=${ip[${idx}]}
    echo "${ip_ds}"
    ds_num=${ds_num[${idx}]}
    dest_path_ds=${path[${idx}]}
    user_ds=${user[${idx}]}
    passwd_ds=${passwd[${idx}]}

    cp ../test/bladestore_client ../bin/bladestore_client_${ds_num}
    ### rsync to dest machine
    pack_common
    rm ../bin/bladestore_client_${ds_num}

    ./run_remote.sh $ip_ds $ssh_port $user_ds $passwd_ds "mkdir -p $dest_path_ds"
    echo "sending common files for count[${idx}]"
    ./pcp.sh $ip_ds $ssh_port $user_ds $passwd_ds $common_tarball $dest_path_ds
     echo "extracting common files for count[${idx}]"
    ./run_remote.sh $ip_ds $ssh_port $user_ds $passwd_ds "cd $dest_path_ds -rf; tar zxf ${common_tarball}"
    let idx+=1
done
