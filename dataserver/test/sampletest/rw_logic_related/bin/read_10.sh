#!/bin/sh
./deletedir /
./testwriteblock   /4 
declare -i idx=1
cd ../concurrencyTest
while (( ${idx} < 11))
do
    cd test${idx}
    . start.sh  /4
    echo "start read /4 from ${idx}"
    let idx+=1
    cd ..
done
