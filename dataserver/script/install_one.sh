#!/bin/bash

source ./conf.txt
source ./FuncDef.sh
source ./install_fun.sh


ssh_port=22

ip_ds=${ip[$1]}
echo "${ip_ds}"
port_ds=${port[$1]}
rack_id_ds=${rack_id[$1]} 
echo "rack_id : ${rack_id_ds}"
ds_num=${ds_num[$1]}
dest_path_ds=${path[$1]}
storage_path_ds=${path[$1]}/storage/data
temp_path_ds=${path[$1]}/storage/temp
echo "path : ${storage_path_ds}"
user_ds=${user[$1]}
passwd_ds=${passwd[$1]}

SetValue ../conf/dataserver.conf ds_ip ${ip_ds}
SetValue ../conf/dataserver.conf ds_port ${port_ds}
SetValue ../conf/dataserver.conf rack_id ${rack_id_ds}
SetValue ../conf/dataserver.conf block_storage_directory  ${storage_path_ds}
SetValue ../conf/dataserver.conf temp_directory  ${temp_path_ds}
cp      ../bin/bladestore_dataserver ../bin/bladestore_dataserver_${ds_num} 
### rsync to dest machine
pack_common
rm    ../bin/bladestore_dataserver_${ds_num}

./run_remote.sh $ip_ds $ssh_port $user_ds $passwd_ds "mkdir -p $dest_path_ds"
echo "sending common files for count[$1]"
./pcp.sh $ip_ds $ssh_port $user_ds $passwd_ds $common_tarball $dest_path_ds
echo "extracting common files for count[$1]"
./run_remote.sh $ip_ds $ssh_port $user_ds $passwd_ds "cd $dest_path_ds -rf; tar zxf ${common_tarball}; rm bin/bladestore_dataserver"
