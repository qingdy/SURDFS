#!/bin/bash

source ./conf.txt
source ./FuncDef.sh
source ./install_fun.sh


ssh_port=22

ip_ds=${ip[$1]}
echo "${ip_ds}"
ds_num=${ds_num[$1]}
dest_path_ds=${path[$1]}
user_ds=${user[$1]}
passwd_ds=${passwd[$1]}

cp ../test/bladestore_client ../bin/bladestore_client_${ds_num}
### rsync to dest machine
pack_common
rm ../bin/bladestore_client_${ds_num}

./run_remote.sh $ip_ds $ssh_port $user_ds $passwd_ds "mkdir -p $dest_path_ds"
echo "sending common files for count[$1]"
./pcp.sh $ip_ds $ssh_port $user_ds $passwd_ds $common_tarball $dest_path_ds
echo "extracting common files for count[$1]"
./run_remote.sh $ip_ds $ssh_port $user_ds $passwd_ds "cd $dest_path_ds -rf; tar zxf ${common_tarball}"
