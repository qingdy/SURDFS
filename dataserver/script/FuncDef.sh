#!/bin/sh
#$1��ʾ�ļ���
#�滻����$2=�����ֵΪ$3 
function SetValue
{
		if [[ $# != 3 ]]
		then
			echo 'input: $1: filename, $2: keyname, $3:keyvalue'
			exit 1
		fi
        filename=$1
        keyname=$2
        keyvalue=$3     # ����Ϊ��
	
        # ȷ��Ŀ�����
        targetLine=`grep "^${keyname}=" ${filename}`

        if [ "$targetLine" ]
        then
            # ������%��Ϊsed�ָ���,��Ϊkeyvalue���ܺ���/
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
#��ӡԶ�̻�����ָ���Ľ�����Ŀ
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
#���Զ�̻����Ͻ������Ƿ����$prognum��������ڣ���ӡ==��Ϣ��������ȣ���ӡ!=��Ϣ
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
#���Զ�̻����Ͻ����Ƿ�ȫ����������,���������==0����ӡ==��Ϣ��������ȣ���ӡ!=��Ϣ
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
#������̲����ڣ���ӡsucessful,�����ӡfailed
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
#�������������
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
