#收集从各个客户端发送到服务器的统计日志

#
sleep 30
for FILE in $(find ./dstemp/*)
do
    cat $FILE >> ./ds_stat.log
    rm -f $FILE
done


for FILE in $(find ./nstemp/*)
do
    cat $FILE >> ./ns_stat.log
    rm -f $FILE
done


HOSTNAME="10.11.150.198"
USERNAME="root"
PASSWORD="123456"
DBNAME="bladestore"

NSLOG=/opt/bladestore/bladestore_monitor/ns_stat.log
DSLOG=/opt/bladestore/bladestore_monitor/ds_stat.log
NSSQL=ns.sql
DSSQL=ds.sql

outdate_ns=/opt/bladestore/bladestore_monitor/outdate_ns
outdate_ds=/opt/bladestore/bladestore_monitor/outdate_ds

create_db_sql="create database IF NOT EXISTS ${DBNAME}"
/usr/bin/mysql -h${HOSTNAME} -u${USERNAME} -p${PASSWORD} -e "${create_db_sql}"


ns=ns
ns_history=ns_history
ds=ds
ds_history=ds_history

#创建ns表
create_ns_sql="create table IF NOT EXISTS $ns (id bigint(20) primary key auto_increment, ip_port varchar(20), vip varchar(20), status varchar(10), read_queue_length int(10), write_queue_length int(10), read_times int(64), read_error_times int(64), write_times int(64), write_error_times int(64), safe_write_times int(64),  safe_write_error_times int(64), delete_times int(64), delete_error_times int(64), copy_times int(64), copy_error_times int(64), dir_nums int(64), file_nums int(64), block_nums int(64), time datetime);"
/usr/bin/mysql -h${HOSTNAME} -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "${create_ns_sql}"

#创建ns历史表
create_ns_history_sql="create table IF NOT EXISTS $ns_history (id bigint(20) primary key auto_increment, ip_port varchar(20), type varchar(10), read_times int(64), read_error_times int(64), write_times int(64), write_error_times int(64), safe_write_times int(64), safe_write_error_times int(64), delete_times int(64), delete_error_times int(64), copy_times int(64), copy_error_times int(64), dir_nums int(64), file_nums int(64), block_nums int(64), time datetime);"
/usr/bin/mysql -h${HOSTNAME} -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "${create_ns_history_sql}"

#创建ds表
create_ds_sql="create table if not exists $ds (id bigint(20) primary key auto_increment, ip_port varchar(20), total_storage int(64), used_storage int(64), block_nums int(64), request_connection_num int(10), current_connection_num int(10), client_queue_length int(10), ns_queue_length int(10), sync_queue_length int(10), read_bytes int(64), read_packet_times int(10), read_packet_error_times int(10), write_bytes int(64), write_packet_times int(10), write_packet_error_times int(10), normal_write_block_times int(10), normal_write_block_error_times int(10), safe_write_block_times int(10), safe_write_block_error_times int(10), delete_block_times int(10), delete_block_error_times int(10), transfer_block_times int(10), transfer_block_error_times int(10), replicate_packet_times int(10), replicate_packet_error_times int(10), replicate_block_times int(10), replicate_block_error_times int(10), time datetime);"
/usr/bin/mysql -h${HOSTNAME} -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "${create_ds_sql}"

#创建ds历史表
create_ds_history_sql="create table if not exists $ds_history (id bigint(20) primary key auto_increment, ip_port varchar(20), type varchar(10), total_storage int(64), used_storage int(64), block_nums int(64), request_connection_num int(64), read_bytes int(64), read_packet_times int(64), read_packet_error_times int(64), write_bytes int(64), write_packet_times int(64), write_packet_error_times int(64), normal_write_block_times int(64), normal_write_block_error_times int(64), safe_write_block_times int(64), safe_write_block_error_times int(64), delete_block_times int(64), delete_block_error_times int(64), transfer_block_times int(64), transfer_block_error_times int(64), replicate_packet_times int(64), replicate_packet_error_times int(64), replicate_block_times int(64), replicate_block_error_times int(64), time datetime);"
/usr/bin/mysql -h${HOSTNAME} -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "${create_ds_history_sql}"


#执行历史数据统计
/opt/bladestore/bladestore_monitor/monitor_master_modify.php

#插入ns实时数据,注意此处的ns，不能用$ns值。。。待解决
cat $NSLOG | awk -F "/" '{now=strftime("%y-%m-%d %H:%M:%S");  print "insert into ns(ip_port, vip, status, read_queue_length, write_queue_length, read_times, read_error_times, write_times, write_error_times, safe_write_times, safe_write_error_times, delete_times, delete_error_times, copy_times, copy_error_times, dir_nums, file_nums, block_nums, time) value(" $1 "," $2 "," $3 "," $4 "," $5 "," $6 "," $7 "," $8 "," $9 "," $10 "," $11 "," $12 "," $13 "," $14 "," $15 "," $16 "," $17 "," $18 "," "\047" now "\047" ");" }' > $NSSQL 
/usr/bin/mysql -h${HOSTNAME} -u${USERNAME} -p${PASSWORD} ${DBNAME} < $NSSQL

#插入ds实时数据,注意此处的ds，不能用$ds值。。。待解决
cat $DSLOG | awk -F "/" '{now=strftime("%y-%m-%d %H:%M:%S");  print "insert into ds(ip_port, total_storage, used_storage, block_nums, request_connection_num, current_connection_num, client_queue_length, ns_queue_length, sync_queue_length, read_bytes, read_packet_times, read_packet_error_times, write_bytes, write_packet_times, write_packet_error_times, normal_write_block_times, normal_write_block_error_times, safe_write_block_times, safe_write_block_error_times, delete_block_times, delete_block_error_times, transfer_block_times, transfer_block_error_times, replicate_packet_times, replicate_packet_error_times, replicate_block_times, replicate_block_error_times, time) value(" $1 "," $2 "," $3 "," $4 "," $5 "," $6 "," $7 "," $8 "," $9 "," $10 "," $11 "," $12 "," $13 "," $14 "," $15 "," $16 "," $17 "," $18 "," $19 "," $20 "," $21 "," $22 "," $23 "," $24 "," $25 "," $26 "," $27 "," "\047" now "\047" ");" }' > $DSSQL
/usr/bin/mysql -h${HOSTNAME} -u${USERNAME} -p${PASSWORD} ${DBNAME} < $DSSQL


#将文件保存到过期文件夹中
#name=`date  +%Y_%m_%d_%H_%M`
#mv $NSLOG ${outdate_ns}/${name}_ns.log
#mv $NSSQL ${outdate_ns}/${name}_ns.sql

#name=`date  +%Y_%m_%d_%H_%M`
#mv $DSLOG ${outdate_ds}/${name}_ds.log
#mv $DSSQL ${outdate_ds}/${name}_ds.sql
