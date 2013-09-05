#include "nameserver_task_process.h"
#include "blade_common_define.h"
#include "status_packet.h"
#include "blade_log_packet.h"
#include "singleton.h"
#include "checkpoint_packet.h"
#include "lease_expired_packet.h"
#include "block_report_packet.h"
#include "blade_ns_log_manager.h"
#include "nameserver_impl.h"
#include "blade_slave_register_packet.h"
#include "block_to_get_length_packet.h"
#include "blade_ns_lease_packet.h"
#include "blade_net_util.h"
#include "blade_slave_mgr.h"
#include "blade_meta_manager.h"
#include "complete_packet.h"
#include "stat_manager.h"

using namespace pandora;
using namespace bladestore::common;
using namespace bladestore::message;

namespace bladestore
{
namespace nameserver
{

NameServerTaskProcess::NameServerTaskProcess(NameServerImpl * nameserver, MetaManager & meta_manager) : nameserver_(nameserver),meta_manager_(meta_manager)
{
	init_ = false;
}

NameServerTaskProcess::~NameServerTaskProcess()
{

}

void NameServerTaskProcess::Init(BladeNsLogManager * blade_ns_log_manager, BladeSlaveMgr * blade_slave_manager)
{
	blade_slave_manager_ = blade_slave_manager;
	blade_ns_log_manager_ = blade_ns_log_manager;
	init_ = true;
}

//主要处理3种逻辑入口,读队列，写队列，log队列, 对于写需要先同步到slave机器上
bool NameServerTaskProcess::handle_packet_queue(BladePacket *packet, void *args)
{
	bool ret = true;
	int return_code = BLADE_SUCCESS;
    LOGV(LL_DEBUG, "in handle!");
	
	assert(true == init_);

	int operation= packet->get_operation();
	//uint32_t channel_id = packet->channel_id();

	if ((void*)WRITE_THREAD_FLAG == args)
	{
		//write stuff
		LOGV(LL_DEBUG, "handle write packet, packe code is %d", operation);
		switch(operation)
		{
            case OP_REGISTER:
                return_code = blade_register(packet);
                break;
            case OP_ABANDONBLOCK:
                return_code = blade_abandon_block(packet);
                break;
            case OP_BAD_BLOCK_REPORT :
                return_code = blade_bad_block_report(packet);
                break;
            case OP_BLOCK_RECEIVED :
                return_code = blade_block_received(packet);
                break;
            case OP_BLOCK_REPORT :
                return_code = blade_block_report(packet);
                break;
            case OP_RENAME:
                return_code = blade_rename(packet);
                break;
            case OP_DELETEFILE:
                return_code = blade_delete_file(packet);
                break;
            case OP_MKDIR:
                return_code = blade_mkdir(packet);
                break;
            case OP_ADDBLOCK:
                return_code = blade_add_block(packet);
                break;
            case OP_COMPLETE:
                return_code = blade_complete(packet);
                break;
			case OP_CREATE:
				return_code = blade_create(packet);
				break;
            case OP_LEASEEXPIRED:
                return_code = blade_expire_lease(packet);
                break;
			case OP_LEAVE_DS:
				return_code = blade_leave_ds(packet);
				break;
			case OP_CHECKPOINT:
				nameserver_->get_check_mutex().Lock();
				return_code = make_checkpoint();
				nameserver_->get_check_mutex().Unlock();
				delete packet;
				packet = NULL;
				break;

			default:
				return_code = BLADE_ERROR;
				break;
		}
	}
	else if ((void*)LOG_THREAD_FLAG == args)
	{
		
		switch(operation)
		{
			case OP_NS_LOG:
				return_code = ns_slave_write_log(packet);
				break;
			default:
				LOGV(LL_ERROR, "unknow packet code %d in read queue", operation);
				return_code = BLADE_ERROR;
				break;
				
		}
	}
	else if ((void*)READ_THREAD_FLAG == args)
	{
		LOGV(LL_DEBUG, "handle read packet, packe code is %d", operation);
		switch(operation)
		{
            case OP_GETLISTING:
                return_code = blade_get_listing(packet);
                break;
            case OP_RENEWLEASE:
                return_code = blade_renew_lease(packet);
                break;
            case OP_GETFILEINFO:
                return_code = blade_get_file_info(packet);
                break;
            case OP_GETBLOCKLOCATIONS:
                return_code = blade_get_block_locations(packet);
                break;
            case OP_ISVALIDPATH:
                return_code = blade_is_valid_path(packet);
                break;
			case OP_NS_SLAVE_REGISTER:
				return_code = ns_slave_register(packet);
				break;
            default:
				LOGV(LL_ERROR, "unknow packet code %d in read queue", operation);
				return_code = BLADE_ERROR;
				break;
		}
	}
	else
	{
		LOGV(LL_ERROR, "we should not reach here, thread_flag = %ld", (long)(args));
	}

	LOGV(LL_DEBUG, "return code : %d", return_code);
	if (BLADE_SUCCESS != return_code)
	{	
		ret = false;
	}

	return ret;
}

int32_t NameServerTaskProcess::blade_register(BladePacket *packet)
{
    int16_t ret_code;  
    int32_t return_code = BLADE_ERROR;
    BladePacket *reply = NULL;
    if (NULL != packet)
	{
        return_code = meta_manager_.register_ds(packet, reply);
        if (BLADE_SUCCESS == return_code)
        {
            ret_code = RET_SUCCESS;
        }
        else
        {
            ret_code = RET_REGISTER_INVALID;
        }
        reply = new RegisterReplyPacket(ret_code);
        int err = -1;
        if (NULL != reply)
        {
            reply->set_channel_id(packet->channel_id()); 
            err = packet->Reply(reply);
        }
        delete packet;
        if (err != 0 && reply) {
            delete reply;
            reply = NULL;
        }
    }
    return return_code;
}

int32_t NameServerTaskProcess::blade_bad_block_report(BladePacket *packet)
{
    int32_t return_code = BLADE_ERROR;
    if (NULL != packet)
    {  
        return_code = meta_manager_.report_bad_block(packet);
        delete packet;
    }
    return return_code;
}

int32_t NameServerTaskProcess::blade_block_received(BladePacket *packet)
{
	int32_t return_code = BLADE_SUCCESS;
	if (NULL != packet)
	{
        return_code = meta_manager_.block_received(packet);
        delete packet;
	}
	else
	{
		return_code = BLADE_ERROR;
	}
	return return_code;
}

int32_t NameServerTaskProcess::blade_block_report(BladePacket *packet)
{
    int32_t return_code = BLADE_ERROR;
    int16_t ret_code = 0;
    BlockReportReplyPacket *reply_packet = NULL;
    if (NULL != packet)
    {
        return_code = meta_manager_.report_blocks(packet);
        if (BLADE_NEED_REGISTER == return_code)
        {
            ret_code = RET_NEED_REGISTER;
        }
        else
        {
            ret_code = RET_SUCCESS;
        }
        reply_packet = new BlockReportReplyPacket(ret_code);
        reply_packet->set_channel_id(packet->channel_id());  
        int err = packet->Reply(reply_packet);
        delete packet;
        if (err != 0 && reply_packet) {
            delete reply_packet;
            reply_packet = NULL;
        }
    }
    return return_code;
}

int32_t NameServerTaskProcess::blade_expire_lease(BladePacket * blade_packet)
{
    int return_code = BLADE_ERROR;
    if (NULL != blade_packet)
    {
		LeaseExpiredPacket * lease_packet = static_cast<LeaseExpiredPacket*>(blade_packet);
		if (NULL == lease_packet)
		{
			if (NULL != blade_packet)
			{
				delete blade_packet;
			}
			return BLADE_ERROR;			
		}

		if (!(lease_packet->is_safe_write_))
		{
        	return_code = meta_manager_.meta_blade_expire_lease(blade_packet);
		}
		else
		{
			meta_manager_.get_layout_manager().blocks_mutex_.rlock()->lock();
			BLOCKS_MAP_ITER iter = meta_manager_.get_layout_manager().blocks_map_.find(lease_packet->block_id());	
			set<uint64_t> ds_list;
			if (meta_manager_.get_layout_manager().blocks_map_.end() != iter)
			{
				ds_list = iter->second->dataserver_set_;
			}
			else
			{
				meta_manager_.get_layout_manager().blocks_mutex_.rlock()->unlock();
				return BLADE_ERROR;
			}
			meta_manager_.get_layout_manager().blocks_mutex_.rlock()->unlock();
			
			std::vector<BladePacket *> response;
			response.clear();
			BlockToGetLengthPacket  * get_length_packet = new BlockToGetLengthPacket(lease_packet->block_id(), lease_packet->block_version());
			nameserver_->get_blade_client_manager()->send_get_length_packet(ds_list, get_length_packet, 5000, response);
			if (0 == response.size())
			{
        		return_code = meta_manager_.meta_blade_expire_lease(blade_packet);
			}
			else
			{
				int32_t length = BLADE_BLOCK_SIZE + 1;
				int valid = 0;
				BlockToGetLengthReplyPacket * packet = NULL;
				for (int i = 0; i < response.size(); i++)
				{
					packet = static_cast<BlockToGetLengthReplyPacket *>(response[i]);
					if (NULL == packet)
					{
						continue;
					}
					valid ++;
                    LOGV(LL_DEBUG, "length : %ld", packet->block_length());
					if (packet->block_length() < length)
					{
						length = packet->block_length();
					}
				}

				if (valid > 0 && length <= BLADE_BLOCK_SIZE && length >= 0)
				{
					BladePacket * reply_packet = NULL;
					CompletePacket * complete_packet = new CompletePacket(lease_packet->file_id(), lease_packet->block_id(), length);	
        			return_code = meta_manager_.meta_blade_complete(complete_packet, reply_packet);
					if (NULL != reply_packet)
					{
						delete reply_packet;
					}
				}
				else
				{
        			return_code = meta_manager_.meta_blade_expire_lease(blade_packet);
				}
			}
		}
        delete blade_packet;
    }
    return return_code;
}

int32_t NameServerTaskProcess::blade_abandon_block(BladePacket * blade_packet)
{
    int return_code = BLADE_ERROR;
    BladePacket *reply_packet = NULL;
    if (NULL != blade_packet)
    {
        return_code = meta_manager_.meta_blade_abandon_block(blade_packet, reply_packet);

        int err = -1;
        if (NULL != reply_packet){
            err = blade_packet->Reply(reply_packet);
            if (err != 0 && reply_packet) {
                delete reply_packet;
                reply_packet = NULL;
            }
        }
        delete blade_packet;
    }
    return return_code;
}

int32_t NameServerTaskProcess::blade_rename(BladePacket * blade_packet)
{
    int return_code = BLADE_ERROR;
    BladePacket *reply_packet = NULL;
    if (NULL != blade_packet)
    {
        return_code = meta_manager_.meta_blade_rename(blade_packet, reply_packet);

        int err = -1;
        if (NULL != reply_packet) {
            err = blade_packet->Reply(reply_packet);
            if (err != 0 && reply_packet) {
                delete reply_packet;
                reply_packet = NULL;
            }
        }
        delete blade_packet;
    }
    return return_code;
}

int32_t NameServerTaskProcess::blade_delete_file(BladePacket * blade_packet)
{
    int return_code = BLADE_ERROR;
    AtomicInc64(&Singleton<StatManager>::Instance().write_times_);
    AtomicInc64(&Singleton<StatManager>::Instance().delete_times_);
    BladePacket *reply_packet = NULL;
    if (NULL != blade_packet)
    {
        return_code = meta_manager_.meta_blade_delete_file(blade_packet, reply_packet);

        int err = -1;
        if (NULL != reply_packet) {
            err = blade_packet->Reply(reply_packet);
            if (err != 0 && reply_packet) {
                delete reply_packet;
                reply_packet = NULL;
            }
        }
        delete blade_packet;
    }
    return return_code;
}

int32_t NameServerTaskProcess::blade_renew_lease(BladePacket * blade_packet)
{
    int return_code = BLADE_ERROR;
    BladePacket *reply_packet = NULL;
    if (NULL != blade_packet)
    {
        return_code = meta_manager_.meta_blade_renew_lease(blade_packet, reply_packet);

        int err = -1;
        if (NULL != reply_packet) {
            err = blade_packet->Reply(reply_packet);
            if (err != 0 && reply_packet) {
                delete reply_packet;
                reply_packet = NULL;
            }
        }
        delete blade_packet;
    }
    return return_code;
}

int32_t NameServerTaskProcess::blade_get_listing(BladePacket * blade_packet)
{
    int return_code = BLADE_ERROR;
    AtomicInc64(&Singleton<StatManager>::Instance().read_times_);
    BladePacket *reply_packet = NULL;
    if (NULL != blade_packet)
    {
        return_code = meta_manager_.meta_blade_get_listing(blade_packet, reply_packet);

        int err = -1;
        if (NULL != reply_packet) {
            err = blade_packet->Reply(reply_packet);
            if (err != 0 && reply_packet) {
                delete reply_packet;
                reply_packet = NULL;
            }
        }
        delete blade_packet;
    }
    return return_code;
}

int32_t NameServerTaskProcess::blade_get_file_info(BladePacket * blade_packet)
{
    int return_code = BLADE_ERROR;
    AtomicInc64(&Singleton<StatManager>::Instance().read_times_);
    BladePacket *reply_packet = NULL;
    if (NULL != blade_packet)
    {
        return_code = meta_manager_.meta_blade_get_file_info(blade_packet, reply_packet);

        int err = -1;
        if (NULL != reply_packet) {
            err = blade_packet->Reply(reply_packet);
            if (err != 0 && reply_packet) {
                delete reply_packet;
                reply_packet = NULL;
            }
        }
        delete blade_packet;
    }
    return return_code;
}

int32_t NameServerTaskProcess::blade_get_block_locations(BladePacket * blade_packet)
{
    int return_code = BLADE_ERROR;
    AtomicInc64(&Singleton<StatManager>::Instance().read_times_);
    BladePacket *reply_packet = NULL;
    if (NULL != blade_packet)
    {
        return_code = meta_manager_.meta_blade_get_block_locations(blade_packet, reply_packet);

        int err = -1;
        if (NULL != reply_packet) {
            err = blade_packet->Reply(reply_packet);
            if (err != 0 && reply_packet) {
                delete reply_packet;
                reply_packet = NULL;
            }
        }
        delete blade_packet;
    }
    return return_code;
}

int32_t NameServerTaskProcess::blade_is_valid_path(BladePacket * blade_packet)
{
    int return_code = BLADE_ERROR;
    AtomicInc64(&Singleton<StatManager>::Instance().read_times_);
    BladePacket *reply_packet = NULL;
    if (NULL != blade_packet)
    {
        return_code = meta_manager_.meta_blade_is_valid_path(blade_packet, reply_packet);

        int err = -1;
        if (NULL != reply_packet) {
            err = blade_packet->Reply(reply_packet);
            if (err != 0 && reply_packet) {
                delete reply_packet;
                reply_packet = NULL;
            }
        }
        delete blade_packet;
    }
    return return_code;
}

int32_t NameServerTaskProcess::blade_mkdir(BladePacket * blade_packet)
{
    int return_code = BLADE_ERROR;
    AtomicInc64(&Singleton<StatManager>::Instance().write_times_);
    BladePacket *reply_packet = NULL;
    if (NULL != blade_packet)
    {
        return_code = meta_manager_.meta_blade_mkdir(blade_packet, reply_packet);

        int err = -1;
        if (NULL != reply_packet) {
            err = blade_packet->Reply(reply_packet);
            if (err != 0 && reply_packet) {
                delete reply_packet;
                reply_packet = NULL;
            }
        }
        delete blade_packet;
    }
    return return_code;
}

int32_t NameServerTaskProcess::blade_add_block(BladePacket * blade_packet)
{
    int return_code = BLADE_ERROR;
    AtomicInc64(&Singleton<StatManager>::Instance().write_times_);
    BladePacket *reply_packet = NULL;
    if (NULL != blade_packet)
    {
        return_code = meta_manager_.meta_blade_add_block(blade_packet, reply_packet);

        int err = -1;
        if (NULL != reply_packet) {
            err = blade_packet->Reply(reply_packet);
            if (err != 0 && reply_packet) {
                delete reply_packet;
                reply_packet = NULL;
            }
        }
        delete blade_packet;
    }
    return return_code;
}

int32_t NameServerTaskProcess::blade_complete(BladePacket * blade_packet)
{
    int return_code = BLADE_ERROR;
    BladePacket *reply_packet = NULL;
    if (NULL != blade_packet)
    {
        return_code = meta_manager_.meta_blade_complete(blade_packet, reply_packet);

        int err = -1;
        if (NULL != reply_packet) {
            err = blade_packet->Reply(reply_packet);
            if (err != 0 && reply_packet) {
                delete reply_packet;
                reply_packet = NULL;
            }
        }
        delete blade_packet;
    }
    return return_code;
}

int32_t NameServerTaskProcess::blade_create(BladePacket * blade_packet)
{
	int return_code = BLADE_ERROR;
    AtomicInc64(&Singleton<StatManager>::Instance().write_times_);
    BladePacket *reply_packet = NULL;
    if (NULL != blade_packet)
    {
	    return_code = meta_manager_.meta_blade_create(blade_packet, reply_packet);

        int err = -1;
        if (NULL != reply_packet) {
	        err = blade_packet->Reply(reply_packet);
            if (err != 0 && reply_packet) {
                delete reply_packet;
                reply_packet = NULL;
            }
        }
        delete blade_packet;
    }
	return return_code;
}

int NameServerTaskProcess::ns_slave_register(BladePacket * packet)
{
	int ret = BLADE_SUCCESS;
	
	LOGV(LL_DEBUG, "IN SLAVE REGISTER");
	
	BladeSlaveRegisterPacket * register_packet = static_cast<BladeSlaveRegisterPacket * >(packet);
	
	if (NULL == register_packet)
	{
		LOGV(LL_DEBUG, "register packet is NULL");
		return BLADE_ERROR;
	}

	uint64_t new_log_file_id = 0; 

	uint64_t rt_slave = register_packet->slave_id();

	if (BLADE_SUCCESS == ret)
	{    
		ret = blade_ns_log_manager_->AddSlave(rt_slave, new_log_file_id);
		if (BLADE_SUCCESS != ret) 
		{
			LOGV(LL_WARN, "add_slave error, err=%d", ret);
		}
		else 
		{
			LOGV(LL_INFO, "add slave, slave_addr=%s, new_log_file_id=%ld, ckpt_id=%lu, err=%d", BladeNetUtil::AddrToString(rt_slave).c_str(), new_log_file_id, blade_ns_log_manager_->GetCheckPoint(), ret);
		}
	}

	if (BLADE_SUCCESS == ret)
	{
		int64_t lease_on = Singleton<BladeNameServerParam>::Instance().get_lease_on();
		if (1 == lease_on)
		{
			BladeLease lease;
			ret = blade_slave_manager_->ExtendLease(rt_slave, lease);
			if (ret != BLADE_SUCCESS)
			{
				LOGV(LL_WARN, "failed to extends lease, ret=%d", ret);
			}
		}
	}

	BladeFetchParam fetch_param;
	if (BLADE_SUCCESS == ret)
	{
		fetch_param.fetch_log_ = true;
		fetch_param.min_log_id_ = blade_ns_log_manager_->GetReplayPoint();
		fetch_param.max_log_id_ = new_log_file_id - 1;

		if (blade_ns_log_manager_->GetCheckPoint() > 0)
		{
			LOGV(LL_INFO, "master has check point, tell slave fetch check point files, id: %ld", blade_ns_log_manager_->GetCheckPoint());
			fetch_param.fetch_ckpt_ = true;
			fetch_param.ckpt_id_ = blade_ns_log_manager_->GetCheckPoint();
			fetch_param.min_log_id_ = fetch_param.ckpt_id_ + 1;
		}
		else
		{
			fetch_param.fetch_ckpt_ = false;
			fetch_param.ckpt_id_ = 0;
		}
	}

	BladeSlaveRegisterReplyPacket * reply_packet = new BladeSlaveRegisterReplyPacket();
	reply_packet->fetch_param_ = fetch_param;
	reply_packet->set_channel_id(register_packet->channel_id());
	
	LOGV(LL_INFO, "fetch check = %d, min_log :%d , max_log: %d, ckpt : %d ", fetch_param.fetch_ckpt_, fetch_param.min_log_id_, fetch_param.max_log_id_, fetch_param.ckpt_id_);
	ret = register_packet->Reply(reply_packet);
	if (0 == ret)
	{
		ret = BLADE_SUCCESS;
	}
	else
	{
		LOGV(LL_DEBUG, "SLAVE REGISTER RESPONSE ERROR");
		ret = BLADE_ERROR;
	}

	if (NULL != register_packet)
	{
		delete register_packet;
	}
	register_packet = NULL;
	
	return ret;
} 

//用在server端接收更新lease的请求
int NameServerTaskProcess::ns_renew_lease(BladePacket * packet)
{
	int ret = BLADE_SUCCESS;
	BladeRenewLeasePacket * renew_lease_packet = static_cast<BladeRenewLeasePacket *>(packet);
	uint64_t rt_slave = renew_lease_packet->ds_id();

	BladeLease lease;
	ret = blade_slave_manager_->ExtendLease(rt_slave, lease);
	if (BLADE_SUCCESS != ret)
	{
		LOGV(LL_WARN, "failed to extend lease, ret=%d", ret);
	}
	
	if (BLADE_SUCCESS == ret)
	{
		BladeGrantLeasePacket * grant_lease_packet = new BladeGrantLeasePacket();
		grant_lease_packet->blade_lease_ = lease;
		renew_lease_packet->Reply(grant_lease_packet);		
	}
	return ret;
}

int NameServerTaskProcess::ns_slave_write_log(BladePacket * packet)
{
	int ret = BLADE_SUCCESS;
	BladeDataBuffer in_buffer;

	BladeLogPacket * log_packet = static_cast<BladeLogPacket *>(packet);
	if (NULL == log_packet)
	{
		return BLADE_ERROR;
	}

	in_buffer.set_data(log_packet->data_buf(), log_packet->data_length());
	in_buffer.get_limit() = log_packet->data_length();
	in_buffer.get_position() = 0;

	LOGV(LL_DEBUG, "SOME DATA POSITION: %d , LIMIT: %d", in_buffer.get_position(), in_buffer.get_limit());

	int64_t in_buff_begin = in_buffer.get_position();
	bool switch_log_flag = false;
	uint64_t log_id;
	static bool is_first_log = true;

	StatusPacket * ret_packet = new StatusPacket();
	ret_packet->set_status(STATUS_OK);
	ret_packet->set_channel_id(log_packet->channel_id());
	ret_packet->Pack();

	// send response to master NameServer
	ret = packet->Reply(ret_packet);

	if (BLADE_SUCCESS == ret)
	{
		if (is_first_log)
		{
			is_first_log = false;

			BladeLogEntry log_ent;
			ret = log_ent.Deserialize(in_buffer.get_data(), in_buffer.get_limit(), in_buffer.get_position());
			if (ret != BLADE_SUCCESS)
			{
				hex_dump(in_buffer.get_data(), in_buffer.get_limit(), LL_INFO);
				LOGV(LL_WARN, "BladeLogEntry deserialize error, error code: %d, position: %ld, limit: %ld", ret, in_buffer.get_position(), in_buffer.get_limit());
			}
			else
			{
				if (BLADE_LOG_SWITCH_LOG != log_ent.cmd_)
				{
					LOGV(LL_WARN, "the first log of slave should be switch_log, cmd_=%d", log_ent.cmd_);
					ret = BLADE_ERROR;
				}
				else
				{
					ret = serialization::decode_i64(in_buffer.get_data(), in_buffer.get_limit(), in_buffer.get_position(), (int64_t*)&log_id);
					if (BLADE_SUCCESS != ret)
					{
						LOGV(LL_WARN, "decode_i64 log_id error, err=%d", ret);
					}
					else
					{
						ret = blade_ns_log_manager_->StartLog(log_id);
						if (BLADE_SUCCESS != ret)
						{
							LOGV(LL_WARN, "start_log error, log_id=%lu err=%d", log_id, ret);
						}
						else
						{
							in_buffer.get_position() = in_buffer.get_limit();
						}
					}
				}
			}
		}
	} // end of first log

	if (BLADE_SUCCESS == ret)
	{
		int64_t data_begin = in_buffer.get_position();
		while (BLADE_SUCCESS == ret && in_buffer.get_position() < in_buffer.get_limit())
		{
			BladeLogEntry log_ent;
			ret = log_ent.Deserialize(in_buffer.get_data(), in_buffer.get_limit(), in_buffer.get_position());
			if (BLADE_SUCCESS != ret)
			{
				LOGV(LL_WARN, "BladeLogEntry deserialize error, err=%d", ret);
				ret = BLADE_ERROR;
			}
			else
			{
				// check switch_log
				if (BLADE_LOG_SWITCH_LOG == log_ent.cmd_)
				{
					if (data_begin != in_buff_begin || ((in_buffer.get_position() + log_ent.GetLogDataLen()) != in_buffer.get_limit()))
					{
						LOGV(LL_ERROR, "swith_log is not single, this should not happen, in_buff.pos=%ld log_data_len=%d in_buff.limit=%ld", in_buffer.get_position(), log_ent.GetLogDataLen(), in_buffer.get_limit());
						ret = BLADE_ERROR;
					}
					else
					{
						ret = serialization::decode_i64(in_buffer.get_data(), in_buffer.get_limit(), in_buffer.get_position(), (int64_t*)&log_id);
						if (BLADE_SUCCESS != ret)
						{
							LOGV(LL_WARN, "decode_i64 log_id error, err=%d", ret);
						}
						else
						{
							switch_log_flag = true;
						}
					}
				}
				else
				{
					if (log_ent.seq_ != (blade_ns_log_manager_->GetCurLogSeq() + 1) && (blade_ns_log_manager_->GetCurLogSeq() != 0))	
					{
						LOGV(LL_DEBUG, "not continous in log mgr : %ld  in seq : %ld", blade_ns_log_manager_->GetCurLogSeq(), log_ent.seq_);
						ret = BLADE_ERROR;			
					}
					else
					{
						blade_ns_log_manager_->SetCurLogSeq(log_ent.seq_);
					}
				}

				in_buffer.get_position() += log_ent.GetLogDataLen();
			}
		}

		if (BLADE_SUCCESS == ret && in_buffer.get_limit() - data_begin > 0)
		{
			ret = blade_ns_log_manager_->StoreLog(in_buffer.get_data() + data_begin, in_buffer.get_limit() - data_begin);
			if (BLADE_SUCCESS != ret)
			{
				LOGV(LL_ERROR, "nameserver store_log error, err=%d", ret);
			}
		}

		if (switch_log_flag)
		{
			ret = blade_ns_log_manager_->SwitchToLogFile(log_id + 1);
			if (BLADE_SUCCESS != ret)
			{
				LOGV(LL_WARN, "switch_to_log_file error, log_id=%lu err=%d", log_id, ret);
			}
		}
	}

	if (NULL != packet)
	{
		delete packet;	
	}
	packet = NULL;
	return ret;
}

int NameServerTaskProcess::blade_leave_ds(BladePacket * blade_packet)
{
	int return_code = BLADE_ERROR;
	if (NULL != blade_packet)
	{
		return_code = meta_manager_.leave_ds(blade_packet);
		delete blade_packet;
	}
	return return_code;
}

int NameServerTaskProcess::make_checkpoint()
{
	int ret = blade_ns_log_manager_->DoCheckPoint();
	if (ret != BLADE_SUCCESS)
	{
		LOGV(LL_ERROR, "failed to make checkpointing, err=%d", ret);
		exit(0);
	}
	else
	{
		LOGV(LL_INFO, "make checkpointing success");
	}
	return ret;
}

}//end of namespace nameserver
}//end of namespace bladestore
