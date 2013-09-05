#!/bin/sh

source conf.txt

if [ $# -lt 1 ]; then
	echo "$0 filename"
	exit
fi

FILE=$1

echo "rsync ${FILE}"

./pcp.sh 10.1.80.60 22 liuxiaoyun liuxiaoyun ${FILE} /home/liuxiaoyun/trunk/dataserver/src/
./pcp.sh 10.1.80.164 22 liuxiaoyun liuxiaoyun ${FILE} /home/liuxiaoyun/trunk/src/
./pcp.sh 10.1.80.168 22 liuxiaoyun liuxiaoyun ${FILE} /home/liuxiaoyun/trunk/dataserver/src/

