#!/usr/bin/php
<?php

#以下代码实现对NS和DS的实时数据进行统计，生成历史数据；

$database = "bladestore";
$ns = "ns";
$ns_history = "ns_history";
$ds = "ds";
$ds_history = "ds_history";

$db = mysql_connect("10.11.150.198", "root", "123456");
if(!$db)
{
    echo "connect error!";
}
mysql_select_db($database);

#以下处理NS
#获取IP插入到历史表中，否则后面的插入和更新都不知道NS的IP
$get_ipport = "select ip_port from $ns group by ip_port";
$result = mysql_query($get_ipport);
while($ns_ip = mysql_fetch_row($result))
{
    #如果不存在某NS历史条目，先插入
    $query = mysql_query("select count(*) as num from $ns_history where ip_port='$ns_ip[0]'");
    $row = mysql_fetch_array($query);
    if($row['num'] <= 0)
    {
        $hour_insert = "insert into $ns_history (ip_port, type, read_times, read_error_times, write_times, write_error_times, safe_write_times, safe_write_error_times, delete_times, delete_error_times, copy_times, copy_error_times, dir_nums, file_nums, block_nums, time) value('$ns_ip[0]','hour',0,0,0,0,0,0,0,0,0,0,0,0,0,now());";
        mysql_query($hour_insert);
        $day_insert = "insert into $ns_history (ip_port, type, read_times, read_error_times, write_times, write_error_times, safe_write_times, safe_write_error_times, delete_times, delete_error_times, copy_times, copy_error_times, dir_nums, file_nums, block_nums, time) value('$ns_ip[0]','day',0,0,0,0,0,0,0,0,0,0,0,0,0,now());";
        mysql_query($day_insert);
        $month_insert = "insert into $ns_history (ip_port, type, read_times, read_error_times, write_times, write_error_times, safe_write_times, safe_write_error_times, delete_times, delete_error_times, copy_times, copy_error_times, dir_nums, file_nums, block_nums, time) value('$ns_ip[0]','month',0,0,0,0,0,0,0,0,0,0,0,0,0,now());";
        mysql_query($month_insert);
        $year_insert = "insert into $ns_history (ip_port, type, read_times, read_error_times, write_times, write_error_times, safe_write_times, safe_write_error_times, delete_times, delete_error_times, copy_times, copy_error_times, dir_nums, file_nums, block_nums, time) value('$ns_ip[0]','year',0,0,0,0,0,0,0,0,0,0,0,0,0,now());";
        mysql_query($year_insert);
    }

    #求一小时的和值
    $sql = "select sum(read_times), sum(read_error_times), sum(write_times), sum(write_error_times), sum(safe_write_times), sum(safe_write_error_times), sum(delete_times), sum(delete_error_times), sum(copy_times), sum(copy_error_times)from $ns where ip_port='$ns_ip[0]';";
    $sum_query = mysql_query($sql);
    $sum = mysql_fetch_row($sum_query);
    #echo $sum[0], " " ,$sum[1]," ", $sum[2]," ", $sum[3], " ", $sum[4]," ", $sum[5]," ", $sum[6]," ", $sum[7], " ", $sum[8], " ", $sum[9] ;

    #其他一些值，不能求和，选取最早的一项
    $sql2 = "select dir_nums, file_nums, block_nums, time from $ns where ip_port='$ns_ip[0]';";
    $num_query = mysql_query($sql2);
    $num = mysql_fetch_row($num_query);

    #更新history中的一小时和值
    $hour_up = "update $ns_history set read_times = $sum[0], read_error_times = $sum[1], write_times = $sum[2], write_error_times = $sum[3], safe_write_times = $sum[4], safe_write_error_times = $sum[5], delete_times = $sum[6], delete_error_times = $sum[7], copy_times = $sum[8], copy_error_times = $sum[9], dir_nums = $num[0], file_nums = $num[1], block_nums = $num[2], time = '$num[3]' where ip_port = '$ns_ip[0]' and type = 'hour';";
    mysql_query($hour_up);

    #更新history中的日值，1点清零
    $day_clear = "update $ns_history set read_times = 0, read_error_times = 0, write_times = 0, write_error_times = 0, safe_write_times = 0, safe_write_error_times = 0, delete_times = 0, delete_error_times = 0, copy_times= 0, copy_error_times = 0, time = now() where ip_port = '$ns_ip[0]' and type = 'day' and hour(curtime()) = 1;";
    mysql_query($day_clear);
    $day_up = "update $ns_history set read_times = read_times+$sum[0], read_error_times = read_error_times+$sum[1], write_times = write_times+$sum[2], write_error_times = write_error_times+$sum[3], safe_write_times = safe_write_times+$sum[4], safe_write_error_times = safe_write_error_times+$sum[5], delete_times = delete_times+$sum[6], delete_error_times = delete_error_times+$sum[7], copy_times = copy_times+$sum[8], copy_error_times = copy_error_times+$sum[9] where ip_port = '$ns_ip[0]' and type = 'day'  ;";
    mysql_query($day_up);

    #更新history中的月值，1号清零
    $month_clear = "update $ns_history set read_times = 0, read_error_times = 0, write_times = 0, write_error_times = 0, safe_write_times = 0, safe_writ    e_error_times = 0, delete_times = 0, delete_error_times = 0, copy_times= 0, copy_error_times = 0, time = now()  where ip_port = '$ns_ip[0]' and type = 'month' and dayofmonth(curdate()) = 1;"; 
    mysql_query($month_clear);

    $month_up = "update $ns_history set read_times = read_times+$sum[0], read_error_times = read_error_times+$sum[1], write_times = write_times+$sum[2], write_error_times = write_error_times+$sum[3], safe_write_times = safe_write_times+$sum[4], safe_write_error_times = safe_write_error_times+$sum[5], delete_times = delete_times+$sum[6], delete_error_times = delete_error_times+$sum[7], copy_times = copy_times+$sum[8], copy_error_times = copy_error_times+$sum[9] where ip_port = '$ns_ip[0]' and type = 'month';";
    mysql_query($month_up);

    #更新history中的年值，1月清零
    $year_clear = "update $ns_history set read_times = 0, read_error_times = 0, write_times = 0, write_error_times = 0, safe_write_times = 0, safe_write_error_times = 0, delete_times = 0, delete_error_times = 0, copy_times= 0, copy_error_times = 0, time = now() where ip_port = '$ns_ip[0]' and type = 'year' and month(curdate()) = 1;";
    mysql_query($year_clear);

    $year_up = "update $ns_history set read_times = read_times+$sum[0], read_error_times = read_error_times+$sum[1], write_times = write_times+$sum[2], write_error_times = write_error_times+$sum[3], safe_write_times = safe_write_times+$sum[4], safe_write_error_times = safe_write_error_times+$sum[5], delete_times = delete_times+$sum[6], delete_error_times = delete_error_times+$sum[7], copy_times = copy_times+$sum[8], copy_error_times = copy_error_times+$sum[9] where ip_port = '$ns_ip[0]' and type = 'year';";
    mysql_query($year_up);

    $query = mysql_query("select count(*) as num from $ns where ip_port='$ns_ip[0]'");
    $row = mysql_fetch_array($query);
    #策略：如果实时数据数目大于等于12，将实时数据全部删除
    if($row['num'] >= 12)
    {
        #删除NS所有临时数据，在外层shell脚本中会插入新数据
        $del = "delete from $ns where ip_port = '$ns_ip[0]';"; 
        mysql_query($del);
    }
}



#以下处理DS

#获取IP插入到历史表中，否则后面的插入和更新都不知道NS的IP
$get_ipport = "select ip_port from $ds group by ip_port";
$result = mysql_query($get_ipport);
while($ds_ip = mysql_fetch_row($result))
{
    #如果不存在某DS历史条目，先插入
    $query = mysql_query("select count(*) as num from $ds_history where ip_port='$ds_ip[0]'");
    $row = mysql_fetch_array($query);
    if($row['num'] <= 0)
    {
        $hour_insert = "insert into $ds_history (ip_port, type, total_storage, used_storage, block_nums,  request_connection_num, read_bytes, read_packet_times, read_packet_error_times, write_bytes, write_packet_times, write_packet_error_times, normal_write_block_times, normal_write_block_error_times, safe_write_block_times, safe_write_block_error_times, delete_block_times, delete_block_error_times, transfer_block_times, transfer_block_error_times, replicate_packet_times, replicate_packet_error_times, replicate_block_times, replicate_block_error_times, time) value('$ds_ip[0]','hour',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,now());";
        mysql_query($hour_insert);
        $day_insert = "insert into $ds_history (ip_port, type, total_storage, used_storage, block_nums, request_connection_num, read_bytes, read_packet_times, read_packet_error_times, write_bytes, write_packet_times, write_packet_error_times, normal_write_block_times, normal_write_block_error_times, safe_write_block_times, safe_write_block_error_times, delete_block_times, delete_block_error_times, transfer_block_times, transfer_block_error_times, replicate_packet_times, replicate_packet_error_times, replicate_block_times, replicate_block_error_times, time) value('$ds_ip[0]','day',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,now());";
        mysql_query($day_insert);
        $month_insert = "insert into $ds_history (ip_port, type, total_storage, used_storage, block_nums, request_connection_num, read_bytes, read_packet_times, read_packet_error_times, write_bytes, write_packet_times, write_packet_error_times, normal_write_block_times, normal_write_block_error_times, safe_write_block_times, safe_write_block_error_times, delete_block_times, delete_block_error_times, transfer_block_times, transfer_block_error_times, replicate_packet_times, replicate_packet_error_times, replicate_block_times, replicate_block_error_times, time) value('$ds_ip[0]','month',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,now());";
        mysql_query($month_insert);
        $year_insert = "insert into $ds_history (ip_port, type, total_storage, used_storage, block_nums, request_connection_num, read_bytes, read_packet_times, read_packet_error_times, write_bytes, write_packet_times, write_packet_error_times, normal_write_block_times, normal_write_block_error_times, safe_write_block_times, safe_write_block_error_times, delete_block_times, delete_block_error_times, transfer_block_times, transfer_block_error_times, replicate_packet_times, replicate_packet_error_times, replicate_block_times, replicate_block_error_times, time) value('$ds_ip[0]','year',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,now());";
        mysql_query($year_insert);
    }
    
    #求一小时的和值
    $sql = "select sum(request_connection_num), sum(read_bytes), sum(read_packet_times), sum(read_packet_error_times), sum(write_bytes), sum(write_packet_times), sum(write_packet_error_times), sum(normal_write_block_times), sum(normal_write_block_error_times), sum(safe_write_block_times), sum(safe_write_block_error_times), sum(delete_block_times), sum(delete_block_error_times), sum(transfer_block_times), sum(transfer_block_error_times), sum(replicate_packet_times), sum(replicate_packet_error_times), sum(replicate_block_times), sum(replicate_block_error_times) from $ds where ip_port='$ds_ip[0]';";
    $sum_query = mysql_query($sql);
    $sum = mysql_fetch_row($sum_query);
    #echo $sum[0], " " ,$sum[1]," ", $sum[2]," ", $sum[3], " ", $sum[4]," ", $sum[5]," ", $sum[6]," ", $sum[7], " ", $sum[8], " ", $sum[9] ;

    #其他一些值，不能求和，选取最早的一项
    $sql2 = "select total_storage, used_storage, block_nums, time from $ds where ip_port='$ds_ip[0]';";
    $num_query = mysql_query($sql2);
    $num = mysql_fetch_row($num_query);
    #echo $num[0]," ",$num[1]," ",$num[2]," ",$num[3];

    #更新history中的一小时和值
    $hour_up = "update $ds_history set total_storage = $num[0], used_storage = $num[1], block_nums = $num[2], time = '$num[3]', request_connection_num = $sum[0], read_bytes = $sum[1], read_packet_times = $sum[2], read_packet_error_times = $sum[3], write_bytes = $sum[4], write_packet_times = $sum[5], write_packet_error_times = $sum[6], normal_write_block_times = $sum[7], normal_write_block_error_times = $sum[8], safe_write_block_times = $sum[9], safe_write_block_error_times = $sum[10], delete_block_times = $sum[11], delete_block_error_times = $sum[12], transfer_block_times = $sum[13], transfer_block_error_times = $sum[14], replicate_packet_times = $sum[15], replicate_packet_error_times = $sum[16], replicate_block_times = $sum[17], replicate_block_error_times = $sum[18] where ip_port = '$ds_ip[0]' and type = 'hour';";
    mysql_query($hour_up);

    #更新history中的日值，1点清零
    $day_clear = "update $ds_history set request_connection_num = 0, read_bytes = 0, read_packet_times = 0, read_packet_error_times = 0, write_bytes = 0, write_packet_times = 0, write_packet_error_times = 0, normal_write_block_times = 0, normal_write_block_error_times = 0, safe_write_block_times = 0, safe_write_block_error_times = 0, delete_block_times = 0, delete_block_error_times = 0, transfer_block_times = 0, transfer_block_error_times = 0, replicate_packet_times = 0, replicate_packet_error_times = 0, replicate_block_times = 0, replicate_block_error_times = 0, time = now()  where ip_port = '$ds_ip[0]' and type = 'day' and hour(curtime()) = 1;";
    mysql_query($day_clear);
    $day_up = "update $ds_history set request_connection_num = request_connection_num + $sum[0], read_bytes = read_bytes + $sum[1], read_packet_times = read_packet_times + $sum[2], read_packet_error_times = read_packet_error_times + $sum[3], write_bytes = write_bytes + $sum[4], write_packet_times = write_packet_times + $sum[5], write_packet_error_times = write_packet_error_times+$sum[6], normal_write_block_times = normal_write_block_times+$sum[7], normal_write_block_error_times = normal_write_block_error_times+$sum[8], safe_write_block_times = safe_write_block_times+$sum[9], safe_write_block_error_times = safe_write_block_error_times+$sum[10], delete_block_times = delete_block_times+$sum[11], delete_block_error_times = delete_block_error_times+$sum[12], transfer_block_times = transfer_block_times+$sum[13], transfer_block_error_times = transfer_block_error_times+$sum[14], replicate_packet_times = replicate_packet_times+$sum[15], replicate_packet_error_times = replicate_packet_error_times+$sum[16], replicate_block_times = replicate_block_times+$sum[17], replicate_block_error_times = replicate_block_error_times+$sum[18] where ip_port = '$ds_ip[0]' and type = 'day'  ;";
    mysql_query($day_up);
    #echo $day_up;

    #更新history中的月值，1号清零
    $month_clear = "update $ds_history set request_connection_num = 0, read_bytes = 0, read_packet_times = 0, read_packet_error_times = 0, write_bytes = 0, write_packet_times = 0, write_packet_error_times = 0, normal_write_block_times = 0, normal_write_block_error_times = 0, safe_write_block_times = 0, safe_write_block_error_times = 0, delete_block_times = 0, delete_block_error_times = 0, transfer_block_times = 0, transfer_block_error_times = 0, replicate_packet_times = 0, replicate_packet_error_times = 0, replicate_block_times = 0, replicate_block_error_times = 0, time = now() where ip_port = '$ds_ip[0]' and type = 'month' and dayofmonth(curdate()) = 1;"; 
    mysql_query($month_clear);

    $month_up = "update $ds_history set request_connection_num = request_connection_num + $sum[0], read_bytes = read_bytes + $sum[1], read_packet_times = read_packet_times +$sum[2], read_packet_error_times = read_packet_error_times + $sum[3], write_bytes = write_bytes + $sum[4], write_packet_times = write_packet_times + $sum[5], write_packet_error_times = write_packet_error_times+$sum[6], normal_write_block_times = normal_write_block_times+$sum[7], normal_write_block_error_times = normal_write_block_error_times+$sum[8], safe_write_block_times = safe_write_block_times+$sum[9], safe_write_block_error_times = safe_write_block_error_times+$sum[10], delete_block_times = delete_block_times+$sum[11], delete_block_error_times = delete_block_error_times+$sum[12], transfer_block_times = transfer_block_times+$sum[13], transfer_block_error_times = transfer_block_error_times+$sum[14], replicate_packet_times = replicate_packet_times+$sum[15], replicate_packet_error_times = replicate_packet_error_times+$sum[16], replicate_block_times = replicate_block_times+$sum[17], replicate_block_error_times = replicate_block_error_times+$sum[18] where ip_port = '$ds_ip[0]' and type = 'month';";
    mysql_query($month_up);

    #更新history中的年值，1月清零
    $year_clear = "update $ds_history set request_connection_num = 0, read_bytes = 0, read_packet_times = 0, read_packet_error_times = 0, write_bytes = 0, write_packet_times = 0, write_packet_error_times = 0, normal_write_block_times = 0, normal_write_block_error_times = 0, safe_write_block_times = 0, safe_write_block_error_times = 0, delete_block_times = 0, delete_block_error_times = 0, transfer_block_times = 0, transfer_block_error_times = 0, replicate_packet_times = 0, replicate_packet_error_times = 0, replicate_block_times = 0, replicate_block_error_times = 0, time = now() where ip_port = '$ds_ip[0]' and type = 'year' and month(curdate()) = 1;";
    mysql_query($year_clear);

    $year_up = "update $ds_history set request_connection_num = request_connection_num + $sum[0], read_bytes = read_bytes + $sum[1], read_packet_times = read_packet_times +$sum[2], read_packet_error_times = read_packet_error_times + $sum[3], write_bytes = write_bytes + $sum[4], write_packet_times = write_packet_times + $sum[5], write_packet_error_times = write_packet_error_times+$sum[6], normal_write_block_times = normal_write_block_times+$sum[7], normal_write_block_error_times = normal_write_block_error_times+$sum[8], safe_write_block_times = safe_write_block_times+$sum[9], safe_write_block_error_times = safe_write_block_error_times+$sum[10], delete_block_times = delete_block_times+$sum[11], delete_block_error_times = delete_block_error_times+$sum[12], transfer_block_times = transfer_block_times+$sum[13], transfer_block_error_times = transfer_block_error_times+$sum[14], replicate_packet_times = replicate_packet_times+$sum[15], replicate_packet_error_times = replicate_packet_error_times+$sum[16], replicate_block_times = replicate_block_times+$sum[17], replicate_block_error_times = replicate_block_error_times+$sum[18] where ip_port = '$ds_ip[0]' and type = 'year';";
    mysql_query($year_up);

    $query = mysql_query("select count(*) as num from $ds where ip_port='$ds_ip[0]'");
    $row = mysql_fetch_array($query);
    #策略：如果实时数据数目大于等于12，将实时数据全部删除
    if($row['num'] >= 12)
    {
        #删除DS所有临时数据，在外层shell脚本中会插入新数据
        $del = "delete from $ds where ip_port = '$ds_ip[0]';"; 
        mysql_query($del);
    }
}




mysql_close($db);
?>
