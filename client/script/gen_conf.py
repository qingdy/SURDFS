#!/bin/env python

ip = [x.strip() for x in file('ip_list.txt') if len(x.strip()) != 0]
d = {}
for x in range(len(ip)):
   d[x] = ip[x]
default_port = 12331
port = 0

l1 = []
l2 = []
for x in range(len(d)*12):
    l2.append((d[x%len(d)], port))
    if len(l2) == 3:
        l1.append(l2)
        l2 = []
    if x % len(d) == len(d) - 1:
        port += 1;

f = file('conf.txt', 'w')
groupid = 0
for l in l1:
    print >>f, "##Group", groupid
    print >>f, "GROUP[%d]=%d" % (groupid, groupid+1)
    memberid = 0
    groupid += 1
    for x in l:
        print >>f, "ip%d[%d]=%s" % (memberid, groupid, x[0])
        print >>f, "port%d[%d]=%d" % (memberid, groupid, x[1] + default_port)
        print >>f, "path%d[%d]=/data%d/dataservice_%d" % (memberid, groupid, x[1] + 1, groupid)
        print >>f, "user%d[%d]=bladefs" % (memberid, groupid)
        print >>f, "passwd%d[%d]=bladefs.123" % (memberid, groupid)
        print >>f, ""
        memberid += 1
f.close()
