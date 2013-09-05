#include "blade_nameserver_handler.h"
#include "blade_packet.h"
#include "nameserver_impl.h"
#include "blade_net_util.h"
#include "socket_context.h"
#include "stat_manager.h"

using namespace bladestore::message;

namespace bladestore
{
namespace nameserver
{

BladeNsStreamHandler::BladeNsStreamHandler(NameServerImpl & nameserver) : BladeStreamSocketHandler(BladeCommonData::amframe_), nameserver_(nameserver) 
{
	packet_factory_ = new BladePacketFactory();
}

BladeNsStreamHandler::~BladeNsStreamHandler()
{
	if (NULL != packet_factory_)
	{
		delete packet_factory_;
	}
	packet_factory_ = NULL;
}


void BladeNsStreamHandler::OnReceived(const Packet & packet, void * args)
{
    int16_t operation;
    unsigned char * data_data = const_cast<unsigned char *>(packet.content());
    if (4 == sizeof(size_t))
    {   
        uint16_t my_data = (unsigned char)data_data[4];
        my_data <<= 8;
        my_data |=(unsigned char) data_data[5];
        operation = my_data;
    }   
    else if (8 == sizeof(size_t))
    {   
        uint16_t my_data = (unsigned char)data_data[8];
        my_data <<= 8;
        my_data |=(unsigned char) data_data[9];
        operation = my_data;
    }   
    else 
    {
        LOGV(LL_ERROR, "sizeof(size_t) != 4 && 8");
        return ;
    }

    BladePacket * blade_packet = packet_factory_->create_packet(operation);
    assert(blade_packet);
    blade_packet->get_net_data()->set_read_data(packet.content(), packet.length());
    blade_packet->Unpack();
    blade_packet->endpoint_ = BladeStreamSocketHandler::end_point();
    blade_packet->set_channel_id(packet.channel_id());
    blade_packet->set_peer_id(BladeNetUtil::GetPeerID(blade_packet->endpoint_.GetFd()));
//  LOGV(LL_INFO, "client or ds ip : %s", BladeNetUtil::AddrToString(BladeNetUtil::get_peer_id(blade_packet->endpoint_.socket_context()->GetFd())).c_str());
    switch(operation)
    {
        case OP_HEARTBEAT:
            nameserver_.get_heartbeat_manager_thread().Push(blade_packet);
            break;
        case OP_GETLISTING : 
        case OP_ISVALIDPATH :
        case OP_GETBLOCKLOCATIONS :
        case OP_GETFILEINFO :
        case OP_STATUS :
        case OP_RENEWLEASE :
		case OP_NS_SLAVE_REGISTER:
            LOGV(LL_DEBUG, "********read operation:%d", blade_packet->get_operation());
            nameserver_.get_read_packet_queue_thread().Push(blade_packet);
            AtomicSet64(&Singleton<StatManager>::Instance().read_queue_length_, nameserver_.get_read_packet_queue_thread().get_queue().Size());
            LOGV(LL_DEBUG, "******Read_packet_queue size : %ld", AtomicGet64(&Singleton<StatManager>::Instance().read_queue_length_));
            break;
        case OP_REGISTER :
        case OP_MKDIR :
        case OP_ABANDONBLOCK :  
        case OP_ADDBLOCK :
        case OP_BLOCK_REPORT :
        case OP_CREATE :
        case OP_RENAME :
        case OP_DELETEFILE :
        case OP_COMPLETE :
        case OP_BLOCK_RECEIVED:
        case OP_BAD_BLOCK_REPORT:
            LOGV(LL_DEBUG, "*********write operation:%d", blade_packet->get_operation());
            nameserver_.get_write_packet_queue_thread().Push(blade_packet);
            AtomicSet64(&Singleton<StatManager>::Instance().write_queue_length_, nameserver_.get_write_packet_queue_thread().get_queue().Size());
            LOGV(LL_DEBUG, "*****Write_packet_queue size : %ld", AtomicGet64(&Singleton<StatManager>::Instance().write_queue_length_));
            break;
		case OP_NS_LOG :
            LOGV(LL_DEBUG, "*********log operation:%d\n", blade_packet->get_operation());
            nameserver_.get_log_packet_queue_thread().Push(blade_packet);
            LOGV(LL_DEBUG, "*****Log_packet_queue size : %d", nameserver_.get_log_packet_queue_thread().get_queue().Size());
        default:
            break;
    }
}

void BladeNsStreamHandler::OnTimeout(void * args)
{


}

BladeNsListenHandler::BladeNsListenHandler(NameServerImpl & nameserver) : ListenSocketHandler(BladeCommonData::amframe_), nameserver_(nameserver)
{

}

BladeNsListenHandler::~BladeNsListenHandler()
{

}

StreamSocketHandler* BladeNsListenHandler::OnAccepted(SocketId id)
{
	return new BladeNsStreamHandler(nameserver_);
}

}//end of namespace nameserve
}//end of namespace bladestore
