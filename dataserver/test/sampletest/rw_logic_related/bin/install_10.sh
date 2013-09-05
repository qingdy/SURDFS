#!/bin/bash
declare -i i=1
declare -i count=11
rm  -r ../concurrencyTest
mkdir ../concurrencyTest
cp -r  ../conf  ../concurrencyTest/
while (($i<11))
do
    mkdir -p ../concurrencyTest/test${i}
    cp   ./start.sh  ./testreadblock  ../concurrencyTest/test${i}
    i=i+1
done
