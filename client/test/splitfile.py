#!/usr/bin/env python
#coding:utf-8
input = open('tempdata', 'rb')
output = open('100Mfile', 'wb')
for i in range(0, 100):
    filedata = input.read(1024*1024)
    output.write(filedata)
output.close()
input.close()
