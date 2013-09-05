#!/bin/sh

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
    port_ds=${port[${idx}]}
    rack_id_ds=${rack_id[${idx}]} 
    echo "rack_id : ${rack_id_ds}"
    ds_num=${ds_num[${idx}]}
    dest_path_ds=${path[${idx}]}
    storage_path_ds=${path[${idx}]}/storage/
    log_path_ds=${path[${idx}]}/log
    bin_path_ds=${path[${idx}]}/bin
    echo "path : ${storage_path_ds}"
    user_ds=${user[${idx}]}
    passwd_ds=${passwd[${idx}]}


    #./run_remote.sh $ip_ds $ssh_port $user_ds $passwd_ds "cd $storage_path_ds && find . -type f -a -name 'blk_*' | xargs rm -f"
    ./run_remote.sh $ip_ds $ssh_port $user_ds $passwd_ds "cd $bin_path_ds &&  rm -f *"
    #./run_remote.sh $ip_ds $ssh_port $user_ds $passwd_ds "cd $log_path_ds && find . -type f -a -name 'dataserver*' | xargs rm -f"
    let idx+=1
done
