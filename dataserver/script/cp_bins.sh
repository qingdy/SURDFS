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
#while (( ${idx} < 1))
do
    ip_ds=${ip[${idx}]}
    echo "${ip_ds}"
    port_ds=${port[${idx}]}
    rack_id_ds=${rack_id[${idx}]} 
    echo "rack_id : ${rack_id_ds}"
    ds_num=${ds_num[${idx}]}
    dest_path_ds=${path[${idx}]}
    storage_path_ds=${path[${idx}]}/storage/data
    temp_path_ds=${path[${idx}]}/storage/temp
    echo "path : ${storage_path_ds}"
    user_ds=${user[${idx}]}
    passwd_ds=${passwd[${idx}]}
	bin_local=../bin/bladestore_dataserver
	bin_remote=${dest_path_ds}/bin/bladestore_dataserver_${ds_num}

    ./run_remote.sh $ip_ds $ssh_port $user_ds $passwd_ds "cd $dest_path_ds -rf; rm  -f  bin/bladestore_dataserver_${ds_num}"
    echo "sending common files for count[${idx}]"
    ./pcp.sh $ip_ds $ssh_port $user_ds $passwd_ds $bin_local $bin_remote
    let idx+=1
done
