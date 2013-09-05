/*
 *version  :  1.0
 *author   :  funing
 *date     :  2012-5-28
 *
 */
#ifndef BLADESTORE_NAMESERVER_MONITOR_MANAGER_H
#define BLADESTORE_NAMESERVER_MONITOR_MANAGER_H

#include<sys/time.h>
#include <stdint.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "atomic.h"
#include "log.h"

#include "cthread.h"
#include "thread_mutex.h"
#include "blade_rwlock.h"

#include "blade_net_util.h"
#include "blade_common_define.h"
#include "blade_common_data.h"
#include "layout_manager.h"

using namespace pandora; 
using namespace bladestore::common; 
using namespace std;

namespace bladestore
{
namespace nameserver
{

class NameServerImpl; 

class MonitorManager : public Runnable
{
public:
	static const int MONITOR_INTERVAL_SECS = 10;//can be moved to another place
    static const int LAST_REPORT_INTERVAL = 86400;

    MonitorManager(BladeLayoutManager &);    
    ~MonitorManager();

    void Init();
    void Start();    
	void Run(CThread * thread, void * arg);

    void monitor_ns();
    void monitor_ns_racks_map();
    void monitor_ns_server_map();
    void monitor_ns_blocks_map();

public:
    CThread ns_monitor_;

private:
    DISALLOW_COPY_AND_ASSIGN(MonitorManager);
    NameServerImpl * ns_;
    BladeLayoutManager & layout_ ;
    Log* log_;
};

}//end of namespace nameserver
}//end of namespace bladestore
#endif

