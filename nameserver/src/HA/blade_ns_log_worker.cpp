#include "blade_ns_log_worker.h"
#include "blade_log_entry.h"
#include "nameserver_impl.h"
#include "blade_ns_log_manager.h"
#include "thread_mutex.h"
#include "blade_meta_manager.h"
#include "log.h"

using namespace bladestore::nameserver;
using namespace pandora;

namespace 
{
  int SYNC_WAIT_US = 10;
}

namespace bladestore
{
namespace ha
{

BladeNsLogWorker::BladeNsLogWorker()
{
	packet_factory_ = new BladePacketFactory();
}

BladeNsLogWorker::~BladeNsLogWorker()
{
	if(NULL != packet_factory_)
	{
		delete packet_factory_;
	}
	packet_factory_ = NULL;
}

void BladeNsLogWorker::SetLogManager(BladeNsLogManager* log_manager)
{
	log_manager_ = log_manager;
}

void BladeNsLogWorker::SetMetaManager(MetaManager * meta_manager)
{
	meta_manager_ = meta_manager;
}

int BladeNsLogWorker::Apply(LogCommand cmd, char* log_data, const int64_t& data_len)
{
	int ret = BLADE_SUCCESS;

	LOGV(LL_INFO, "start replay log, cmd type: %d", cmd);
	BladePacket * blade_packet = NULL;
    BladePacket * blade_reply_packet = NULL;
	unsigned char * data_data =(unsigned char *)(log_data);
	switch (cmd)
	{
		case BLADE_LOG_CREATE: 
			blade_packet = packet_factory_->create_packet(OP_CREATE);
			assert(blade_packet);
			blade_packet->get_net_data()->set_read_data(data_data, data_len);
			blade_packet->Unpack();
            ret = meta_manager_->meta_blade_create(blade_packet, blade_reply_packet);
            break;
        case BLADE_LOG_MKDIR:
            blade_packet = packet_factory_->create_packet(OP_MKDIR);
            assert(blade_packet);
            blade_packet->get_net_data()->set_read_data(data_data, data_len);
            blade_packet->Unpack();
            ret = meta_manager_->meta_blade_mkdir(blade_packet, blade_reply_packet);
            break;
        case BLADE_LOG_ADDBLOCK:
            blade_packet = packet_factory_->create_packet(OP_NS_ADD_BLOCK);
            assert(blade_packet);
            blade_packet->get_net_data()->set_read_data(data_data, data_len);
            blade_packet->Unpack();
            ret = meta_manager_->do_add_block(blade_packet);
            break;
        case BLADE_LOG_COMPLETE:
            blade_packet = packet_factory_->create_packet(OP_COMPLETE);
            assert(blade_packet);
            blade_packet->get_net_data()->set_read_data(data_data, data_len);
            blade_packet->Unpack();
            ret = meta_manager_->meta_blade_complete(blade_packet, blade_reply_packet);
            break;
        case BLADE_LOG_ABANDONBLOCK:
            blade_packet = packet_factory_->create_packet(OP_ABANDONBLOCK);
            assert(blade_packet);
            blade_packet->get_net_data()->set_read_data(data_data, data_len);
            blade_packet->Unpack();
            ret = meta_manager_->meta_blade_abandon_block(blade_packet, blade_reply_packet);
            break;
        case BLADE_LOG_RENAME:
            blade_packet = packet_factory_->create_packet(OP_RENAME);
            assert(blade_packet);
            blade_packet->get_net_data()->set_read_data(data_data, data_len);
            blade_packet->Unpack();
            ret = meta_manager_->meta_blade_rename(blade_packet, blade_reply_packet);
            break;
        case BLADE_LOG_DELETEFILE:
            blade_packet = packet_factory_->create_packet(OP_DELETEFILE);
            assert(blade_packet);
            blade_packet->get_net_data()->set_read_data(data_data, data_len);
            blade_packet->Unpack();
            ret = meta_manager_->do_delete_file(blade_packet);
            break;
        case BLADE_LOG_REGISTER:
            blade_packet = packet_factory_->create_packet(OP_REGISTER);
            assert(blade_packet);
            blade_packet->get_net_data()->set_read_data(data_data, data_len);
            blade_packet->Unpack();
            ret = meta_manager_->register_ds(blade_packet, blade_reply_packet);
            break;
        case BLADE_LOG_BLOCK_REPORT:
            blade_packet = packet_factory_->create_packet(OP_BLOCK_REPORT);
            assert(blade_packet);
            blade_packet->get_net_data()->set_read_data(data_data, data_len);
            blade_packet->Unpack();
            ret = meta_manager_->report_blocks(blade_packet);
            break;
        case BLADE_LOG_BAD_BLOCK_REPORT:
            blade_packet = packet_factory_->create_packet(OP_BAD_BLOCK_REPORT);
            assert(blade_packet);
            blade_packet->get_net_data()->set_read_data(data_data, data_len);
            blade_packet->Unpack();
            ret = meta_manager_->report_bad_block(blade_packet);
            break;
        case BLADE_LOG_BLOCK_RECEIVED:
            blade_packet = packet_factory_->create_packet(OP_BLOCK_RECEIVED);
            assert(blade_packet);
            blade_packet->get_net_data()->set_read_data(data_data, data_len);
            blade_packet->Unpack();
            ret = meta_manager_->block_received(blade_packet);
            break;
        case BLADE_LOG_EXPIRE_LEASE:
            blade_packet = packet_factory_->create_packet(OP_LEASEEXPIRED);
            assert(blade_packet);
            blade_packet->get_net_data()->set_read_data(data_data, data_len);
            blade_packet->Unpack();
            ret = meta_manager_->meta_blade_expire_lease(blade_packet);
            break;
        case BLADE_LOG_LEAVE_DS:
            blade_packet = packet_factory_->create_packet(OP_LEAVE_DS);
            assert(blade_packet);
            blade_packet->get_net_data()->set_read_data(data_data, data_len);
            blade_packet->Unpack();
            ret = meta_manager_->leave_ds(blade_packet);
            break;
        case BLADE_LOG_CHECKPOINT:
			blade_packet =  NULL;
			ret = DoCheckPoint(log_data, data_len);	
            break;
        default:
			LOGV(LL_WARN, "unknow log command [%d]", cmd);
			ret = BLADE_INVALID_ARGUMENT;
			break;
	}

    if(NULL != blade_packet)
	{
        delete blade_packet;
	}
    blade_packet = NULL;
	if(NULL != blade_reply_packet)
    {
        delete blade_reply_packet;
    }
    blade_reply_packet = NULL;
    if(BLADE_ERROR != ret)
	{
		LOGV(LL_DEBUG, "ret code is : %d", ret);
		ret = BLADE_SUCCESS;
	}
	return ret;
}

int BladeNsLogWorker::DoCheckPoint(const char* log_data, const int64_t& log_length)
{
	int ret = BLADE_SUCCESS;

	int64_t pos = 0;
	int64_t ckpt_id;

	ret = serialization::decode_i64(log_data, log_length, pos, &ckpt_id);

	if (BLADE_SUCCESS == ret)
	{
		ret = log_manager_->DoCheckPoint(ckpt_id);
	}

	return ret;
}

int BladeNsLogWorker::FlushLog(const LogCommand cmd, const char* log_data, const int64_t& serialize_size)
{
	int ret = BLADE_SUCCESS;

	LOGV(LL_DEBUG, "flush update log, cmd type: %d", cmd);

	CThreadGuard guard(log_manager_->GetLogSyncMutex());

	ret = log_manager_->WriteAndFlushLog(cmd, log_data, serialize_size);

	return ret; 
}

int BladeNsLogWorker::BladeExpireLease(LogCommand cmd, char * data, int length)
{
    FlushLog(cmd, data, length);
    return 0;
}

int BladeNsLogWorker::BladeBadBlockReport(LogCommand cmd, char * data, int length)
{
    FlushLog(cmd, data, length);
    return 0;
}

int BladeNsLogWorker::BladeBlockReceived(LogCommand cmd, char * data, int length)
{
    FlushLog(cmd, data, length);
    return 0;
}

int BladeNsLogWorker::BladeBlockReport(LogCommand cmd, char * data, int length)
{
    FlushLog(cmd, data, length);
    return 0;
}

int BladeNsLogWorker::BladeRegister(LogCommand cmd, char * data, int length)
{
    FlushLog(cmd, data, length);
    return 0;
}

int BladeNsLogWorker::BladeAbandonBlock(LogCommand cmd, char * data, int length)
{
	FlushLog(cmd, data, length);
    return 0;
}

int BladeNsLogWorker::BladeRename(LogCommand cmd, char * data, int length)
{
	FlushLog(cmd, data, length);
    return 0;
}

int BladeNsLogWorker::BladeDeleteFile(LogCommand cmd, char * data, int length)
{
	FlushLog(cmd, data, length);
    return 0;
}


int BladeNsLogWorker::BladeComplete(LogCommand cmd, char * data, int length)
{
	FlushLog(cmd, data, length);
    return 0;
}

int BladeNsLogWorker::BladeCreate(LogCommand cmd, char * data, int length)
{
	FlushLog(cmd, data, length);
    return 0;
}

int BladeNsLogWorker::BladeMkdir(LogCommand cmd, char * data, int length)
{
	FlushLog(cmd, data, length);
    return 0;
}

int BladeNsLogWorker::BladeAddBlock(LogCommand cmd, char * data, int length)
{
	FlushLog(cmd, data, length);
    return 0;
}

int BladeNsLogWorker::BladeLeaveDs(LogCommand cmd, char * data, int length)
{
	FlushLog(cmd, data, length);
    return 0;
}

void BladeNsLogWorker::Exit()
{
	//no op now, do something later
}

}//end of namespace ha
}//end of namespace bladestore

