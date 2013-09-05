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
    storage_path_ds=${path[${idx}]}/storage/data
    temp_path_ds=${path[${idx}]}/storage/temp
    echo "path : ${storage_path_ds}"
    user_ds=${user[${idx}]}
    passwd_ds=${passwd[${idx}]}
    stat_log=${ip_ds}_${port_ds}.log

    SetValue ../conf/dataserver.conf ds_ip ${ip_ds}
    SetValue ../conf/dataserver.conf ds_port ${port_ds}
    SetValue ../conf/dataserver.conf rack_id ${rack_id_ds}
    SetValue ../conf/dataserver.conf stat_log ${stat_log}
    SetValue ../conf/dataserver.conf block_storage_directory  ${storage_path_ds}
    SetValue ../conf/dataserver.conf temp_directory  ${temp_path_ds}
    cp ../bin/bladestore_dataserver ../bin/bladestore_dataserver_${ds_num}
    ### rsync to dest machine
    pack_common
    rm ../bin/bladestore_dataserver_${ds_num}

    ./run_remote.sh $ip_ds $ssh_port $user_ds $passwd_ds "mkdir -p $dest_path_ds"
    echo "sending common files for count[${idx}]"
    ./pcp.sh $ip_ds $ssh_port $user_ds $passwd_ds $common_tarball $dest_path_ds
     echo "extracting common files for count[${idx}]"
    ./run_remote.sh $ip_ds $ssh_port $user_ds $passwd_ds "cd $dest_path_ds -rf; tar zxf ${common_tarball}; rm bin/bladestore_dataserver"
    let idx+=1
done
