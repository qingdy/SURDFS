#!/bin/sh
sleep 30
/opt/bladestore/bladestore_monitor/pcp.sh 10.11.150.198 22 bladestore bladestore  /opt/bladestore/dataserver/log/DS_STATUS.log /opt/bladestore/bladestore_monitor/dstemp/10.11.150.198_8000
rm -f /opt/bladestore/dataserver/log/DS_STATUS.log


