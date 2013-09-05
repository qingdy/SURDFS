/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-6-6
 *
 */

#ifndef BLADESTORE_NAMESERVER_TASK_PROCESS_H
#define BLADESTORE_NAMESERVER_TASK_PROCESS_H

#include "blade_packet_queue_thread.h"
#include "blade_thread_buffer.h"

namespace bladestore
{
namespace ha
{

class BladeNsLogManager;
class BladeSlaveMgr;

}
}

using namespace bladestore::ha;

namespace bladestore
{
namespace nameserver
{

class NameServerImpl;
class MetaManager;

class NameServerTaskProcess : public IPacketQueueHandler
{
public:
	NameServerTaskProcess(NameServerImpl* nameserver, MetaManager & meta_manager);
	~NameServerTaskProcess();

	//used for BladePacketQueueThread
	bool handle_packet_queue(BladePacket *packet, void *args);
	void Init(BladeNsLogManager * blade_ns_log_manager, BladeSlaveMgr * blade_slave_manager);

private:
    //与Client交互的函数
	int32_t blade_create(BladePacket * packet);
    int32_t blade_mkdir(BladePacket * packet);
    int32_t blade_add_block(BladePacket * packet);
    int32_t blade_complete(BladePacket * packet);
    int32_t blade_get_block_locations(BladePacket * packet);
    int32_t blade_is_valid_path(BladePacket * packet);
    int32_t blade_get_listing(BladePacket * packet);
    int32_t blade_get_file_info(BladePacket * packet);
    int32_t blade_abandon_block(BladePacket * packet);
    int32_t blade_rename(BladePacket * packet);
    int32_t blade_delete_file(BladePacket * packet);
    int32_t blade_renew_lease(BladePacket * packet);

    //与dataserver的交互
    int32_t blade_register(BladePacket * packet);
    int32_t blade_bad_block_report(BladePacket * packet);
    int32_t blade_block_received(BladePacket * packet);
    int32_t blade_block_report(BladePacket *packet);

	//与Slave NameServer的log交互
	int make_checkpoint();
	int ns_slave_write_log(BladePacket * packet);
	int ns_slave_register(BladePacket *packet);
	int ns_renew_lease(BladePacket * packet);
    int32_t blade_expire_lease(BladePacket * blade_packet);
    int blade_leave_ds(BladePacket * blade_packet);
				
private:
	bool init_;

	//TLS 接收缓冲区
	ThreadSpecificBuffer log_receive_packet_buffer_;

	NameServerImpl * nameserver_;	
	MetaManager & meta_manager_;
	BladeNsLogManager * blade_ns_log_manager_;
	BladeSlaveMgr * blade_slave_manager_;
};

}//end of namespace nameserver
}//end of namespace bladestore
#endif
