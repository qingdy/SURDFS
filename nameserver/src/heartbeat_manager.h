/*
 *version  : 1.0
 *authro   : chen yunyun, funing
 *date     : 2012-4-20
 *
 */

#ifndef BLADESTORE_NAMESERVER_HEARTBEAT_MANAGER_H
#define BLADESTORE_NAMESERVER_HEARTBEAT_MANAGER_H
#include <deque>

#include "proactor_pipe.h"
#include "blade_packet.h"
#include "blade_dataserver_info.h"
#include "heartbeat_packet.h"

#ifdef GTEST
#define private public
#define protected public
#endif


using namespace bladestore::common;
using namespace bladestore::message;

namespace bladestore
{
namespace nameserver
{

class MetaManager;

//处理来自ds端的心跳和请求等, HeartbeatManager需要实现execute方法
class HeartbeatManager : public ProactorDataPipe<std::deque<BladePacket *>, HeartbeatManager >
{
public: 
	typedef ProactorDataPipe<std::deque<BladePacket *>, HeartbeatManager > base_type;

public:
	HeartbeatManager(MetaManager & meta_manager);
	~HeartbeatManager();
	void Initialize(int32_t thread_count, int32_t max_queue_size);

	//ProactorDataPipe的接口	
	int Execute(BladePacket * packet, void * args);
	int Push(BladePacket * packet);

private:
    int update_ds(HeartbeatPacket *);

private:
	MetaManager & meta_manager_;
	int32_t max_queue_size_;
};

}//end of namespace nameserver
}//end of namespace bladestore
#endif
