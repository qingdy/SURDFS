#!/bin/sh
#$1表示文件名
#替换参数$2=后面的值为$3 
function SetValue
{
		if [[ $# != 3 ]]
		then
			echo 'input: $1: filename, $2: keyname, $3:keyvalue'
			exit 1
		fi
        filename=$1
        keyname=$2
        keyvalue=$3     # 可以为空
	
        # 确保目标存在
        targetLine=`grep "^${keyname}=" ${filename}`

        if [ "$targetLine" ]
        then
            # 这里用%作为sed分隔符,因为keyvalue可能含有/
            sed --in-place "s%^${keyname}=.*%${keyname}=${keyvalue}%" ${filename}
        else
            echo "${keyname}=${keyvalue}" >>${filename}
        fi

}


function StartProc
{
	if [[ $# != 4 ]]
	then
		echo 'input: $1: ip, $2: port, $3: user, $4: progname'
		exit 1
	fi
	dest_ip=$1
	dest_port=$2
	dest_user=$3
	progname=$4
	rslt=`ssh $dest_user@$dest_ip -p$dest_port $progname`
	echo $rslt
							
}

#$1: ip, $2: port, $3: user, $4: progname
#打印远程机器上指定的进程数目
function GetProcNum
{
	if [[ $# != 4 ]]
	then
		echo 'input: $1: ip, $2: port, $3: user, $4: progname'
		exit 1
	fi
	dest_ip=$1
	dest_port=$2
	dest_user=$3
	progname=$4
	
	checkcmd="ps aux |grep $progname |grep -v grep|wc -l"
	rslt=`ssh $dest_user@$dest_ip -p$dest_port $checkcmd`
	echo $rslt
}

#$1: ip, $2: port, $3: user, $4: progname, $5: prognum
#检查远程机器上进程数是否等于$prognum，如果等于，打印==信息，如果不等，打印!=信息
function checkStart
{
	if [[ $# != 5 ]]
	then
		echo 'input: $1: ip, $2: port, $3: user, $4: progname, $5: prongum'
		exit 1	
	fi
	dest_ip=$1
	dest_port=$2
	dest_user=$3
	progname=$4
	prognum=$5
	rslt=`GetProcNum $dest_ip $dest_port $dest_user $progname`
	if [[ $rslt != $prognum  ]]
	then 
		echo "$progname exist $rslt != expect $prognum, failed"
	else
		echo "$progname exist $rslt == expect $prognum, successful"		
	fi
}

#$1: ip, $2: port, $3: user, $4: progname
#检查远程机器上进程是否全部不存在了,如果进程数==0，打印==信息，如果不等，打印!=信息
function checkStop
{
	
	if [[ $# != 4 ]]
    then
	    echo 'input: $1: ip, $2: port, $3: user, $4: progname'
        exit 1
    fi 
	dest_ip=$1
	dest_port=$2
	dest_user=$3
	progname=$4
	prognum=0
	checkStart $dest_ip $dest_port $dest_user $progname $prognum
}

#$1: ip, $2: port, $3: user, $4: progname
#如果进程不存在，打印sucessful,否则打印failed
function checkFin
{
	if [[ $# != 4 ]]
	then
		echo 'input: $1: ip, $2: port, $3: user, $4: progname'
		exit 1
	fi 
    dest_ip=$1
    dest_port=$2
    dest_user=$3
    progname=$4
	prognum=0
	rslt=`GetProcNum $dest_ip $dest_port $dest_user $progname`
	if [[ $rslt != $prognum  ]]
	then
		echo "failed"
	else
		echo "successful"
	fi
}

#$1: ip, $2: port, $3: user, $4: progname
#阻塞检查进程完成
function WaitFin
{
	if [[ $# != 4 ]]
    then
		echo 'input: $1: ip, $2: port, $3: user, $4: progname'
	exit 1
	fi
	dest_ip=$1
	dest_port=$2
	dest_user=$3
	progname=$4
	

	while [ 1 ]
	do
		rslt=`checkFin $dest_ip $dest_port $dest_user $progname`
		if [[ $rslt == "successful"  ]]
		then 
			echo stop $progname $rslt
			break
		else
			echo stop $progname $rslt
			sleep 10
		fi
	done
}

function WARNING
{
	echo "warning are you sure (yes/no)"
	read answer
	if [ $answer != "yes" ]
	then
    	echo "answer is $answer, exit!"
	    exit 0
	else
	    echo "answer is $answer, continue..."
	fi
}
