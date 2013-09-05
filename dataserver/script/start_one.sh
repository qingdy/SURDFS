#!/bin/sh

### start dataservices of one group
source ./FuncDef.sh
source ./conf.txt


ssh_port=22
ip_ds=${ip[$1]}

echo " first para: $1"
echo "${ip_ds}"
port_ds=${port[$1]}
rack_id_ds=${rack_id[$1]} 
echo "rack_id : ${rack_id_ds}"
ds_num=${ds_num[$1]}
dest_path_ds=${path[$1]}
echo "path : ${storage_path_ds}"
user_ds=${user[$1]}
passwd_ds=${passwd[$1]}

./run_remote.sh $ip_ds $ssh_port $user_ds $passwd_ds "cd $dest_path_ds/bin; ./start.sh"
