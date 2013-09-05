#include "heartbeat_manager.h"
#include "blade_net_util.h"
#include "heartbeat_packet.h"
#include "blade_dataserver_info.h"
#include "blade_common_define.h"
#include "blade_meta_manager.h"
#include "log.h"

using namespace pandora;

namespace bladestore
{
namespace nameserver
{

HeartbeatManager::HeartbeatManager(MetaManager & meta_manager) : meta_manager_(meta_manager)
{
    Initialize(1,1000);
    //set_thread_parameter(this, 1, NULL);
}

HeartbeatManager::~HeartbeatManager()
{

}

void HeartbeatManager::Initialize(int32_t thread_count, int32_t max_queue_size)
{
    max_queue_size_ = max_queue_size;
	base_type::set_thread_parameter(this, thread_count, NULL);
	base_type::Start();
}

int HeartbeatManager::Push(BladePacket * blade_packet)
{
	//@notice cannt blocking here
	bool handled = base_type::Push(blade_packet, max_queue_size_, false);
    if (handled)
		return BLADE_SUCCESS;
	if (NULL != blade_packet)
	{
		delete blade_packet;
	}
	blade_packet = NULL;
	return BLADE_ERROR;
}

int HeartbeatManager::Execute(BladePacket * blade_packet, void * args)
{
	HeartbeatPacket * heartbeat_packet = static_cast<HeartbeatPacket *>(blade_packet);
	if (NULL == heartbeat_packet)
    {
		return BLADE_ERROR;	
    }
	return update_ds(heartbeat_packet);
}

int HeartbeatManager::update_ds(HeartbeatPacket *packet)
{
    HeartbeatReplyPacket *reply = new HeartbeatReplyPacket(BLADE_SUCCESS);
    reply->set_channel_id(packet->channel_id());  
    if (BLADE_ERROR == meta_manager_.update_ds(packet, reply))
    {
        reply->set_ret_code(RET_NEED_REGISTER);
        int err = packet->Reply(reply);
        if (err != 0 && reply) {
            delete reply;
            reply = NULL;
        }
        delete packet;
    	LOGV(MSG_INFO, "dataserver need re_register!");
        return BLADE_SUCCESS;
    }

    if (reply->blocks_to_remove().size() > 0 || reply->blocks_to_replicate().size() >0)
    {
        reply->set_ret_code(RET_NEED_OP);
        int err = packet->Reply(reply);
        if (err != 0 && reply) {
            delete reply;
            reply = NULL;
        }
    	LOGV(LL_INFO, "send cmd to dataserver!");
        delete packet;
        return BLADE_SUCCESS;
    }
    if (NULL != reply)
        delete reply;
    reply = NULL;
    if (NULL != packet)
        delete packet;
    packet = NULL;
    return BLADE_SUCCESS;
}

}//end of namespace nameserver
}//end of namespace bladestore
