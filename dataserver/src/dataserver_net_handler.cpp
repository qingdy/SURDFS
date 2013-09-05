#include "log.h"
#include "amframe.h"
#include "socket_context.h"
#include "read_block_packet.h"
#include "blade_packet.h"
#include "bladestore_ops.h"
#include "blade_net_util.cpp"
#include "dataserver_impl.h"
#include "dataserver_response.h"
#include "dataserver_net_server.h"
#include "dataserver_heartbeat.h"
#include "dataserver_stat_manager.h"
#include "dataserver_net_handler.h"
//#include "write_packet_packet.h"

namespace bladestore
{
namespace dataserver
{
using bladestore::message::BladePacket;

//----------------------- DataServerstreamsockethandler------------------------//
DataServerStreamSocketHandler::DataServerStreamSocketHandler(AmFrame& amframe, 
                                                            DataServerImpl *impl)
    :BladeStreamSocketHandler(amframe)
{
    ds_impl_ = impl;
}

void DataServerStreamSocketHandler::OnConnected()
{
    ds_impl_->NumConnectionAdd();
    LOGV(LL_INFO, "OnConnected");
}

void DataServerStreamSocketHandler::OnReceived(const Packet& packet, 
                                                void *client_data)
{
    uint16_t packet_type = 0;
    if (4 == sizeof(size_t)) 
    {
        packet_type =(unsigned char)(packet.content())[4];
        packet_type <<= 8;
        packet_type |= (unsigned char)(packet.content())[5];
    } 
    else if (8 == sizeof(size_t)) 
    {
        packet_type =(unsigned char) (packet.content())[8];
        packet_type <<= 8;
        packet_type |= (unsigned char)(packet.content())[9];
    }

    int16_t operation = (int16_t) packet_type;
    LOGV(LL_INFO, "packet received, type:%d", operation);

    bool ret = true;
    BladePacket * blade_packet = CreatePacket(operation, packet); 
    if (NULL == blade_packet)
        return;
    switch (operation)
    {
        case OP_WRITEPIPELINE_REPLY :  
        case OP_WRITEPACKET_REPLY : 
        {
            if (NULL ==client_data)
            {
                LOGV(LL_DEBUG, "write reply client data == NULL");
                delete blade_packet;
                return;
            }
        }
        case OP_READBLOCK :  
        {
            if (OP_READBLOCK == operation) 
            {
                ReadBlockPacket * read_packet = static_cast<ReadBlockPacket *>(blade_packet);
                LOGV(LL_DEBUG, "read blk_id:%ld, offset:%ld,channel_id:%u,FD:%d", 
                        read_packet->block_id(), read_packet->block_offset(), 
                        read_packet->channel_id(), end_point().GetFd());
            }
        }
        case OP_WRITEPIPELINE :  
        case OP_WRITEPACKET :  
        case OP_REPLICATEBLOCK :  
        {
            //for these for cases, client_data == NULL
            ClientTask * task = new ClientTask(blade_packet, 
                                    static_cast<WriteResponse *>(client_data));
            ret = ds_impl_->PushClientQueue(task); 

            if (true == ret)
                LOGV(LL_INFO, "into client queue, type:%d", operation);
            else
            {
                delete task;
                LOGV(LL_ERROR, "error into client queue, type:%d", operation);
            }

            LOGV(LL_DEBUG, "client_queue_size: %d", ds_impl_->ClientQueueSize());
            break;
        }
        case OP_REPLICATEBLOCK_REPLY :
        case OP_REGISTER_REPLY :
        {
            if (NULL == client_data)
                break;
            SyncResponse * sync_client_data = static_cast<SyncResponse *>(client_data);
            NSTask * task = new NSTask(blade_packet, sync_client_data);
                                    
            ret = ds_impl_->PushNameServerQueue(task);

            if (true == ret)
                LOGV(LL_INFO, "into nameserver queue, type:%d", operation);
            else
            {
                delete sync_client_data;
                delete blade_packet;
                LOGV(LL_ERROR, "error into nameserver queue, type:%d", operation);
            }
            LOGV(LL_DEBUG, "nameserer_queue_size: %d", ds_impl_->NameServerQueueSize());
            break;
        }
        case OP_HEARTBEAT_REPLY :  
        case OP_BLOCK_TO_GET_LENGTH :  
        case OP_BLOCK_REPORT_REPLY :
        {
            //client_data is not used within these three situations
            NSTask * task = new NSTask(blade_packet, NULL);

            ret = ds_impl_->PushNameServerQueue(task);
            if (true == ret)
                LOGV(LL_INFO, "into nameserver queue, type:%d", operation);
            else
            {
                delete blade_packet;
                LOGV(LL_ERROR, "error into nameserver queue, type:%d", operation);
            }
            LOGV(LL_DEBUG, "ns_queue_size:%d", ds_impl_->NameServerQueueSize());
            break;
        }
        default:
        {
            LOGV(LL_ERROR, "invalid operation type:%d", operation);
            if (NULL != blade_packet)
                delete blade_packet;
            break;
        }
    }
}

void DataServerStreamSocketHandler::OnTimeout(void *client_data)
{
    LOGV(LL_DEBUG, "OnTimeout.");
    if (NULL != client_data) 
    {
        DataServerResponse * client_data_temp = (DataServerResponse *)(client_data);
        switch (client_data_temp->operation()) 
        {
            case OP_WRITEPIPELINE:
            {
                WriteResponse * client_data_local =
                                static_cast<WriteResponse *>(client_data_temp);
                if (NULL == client_data_local)
                    break;
                //TODO  log statement  below for debug use
                LOGV(LL_DEBUG, "timeout for op:%d,", client_data_local->operation(), 
                     client_data_local->channel_id()); 
                delete client_data_local;
                        break;
            }
            case OP_WRITEPACKET:
            {
                WriteResponse * client_data_local =
                                static_cast<WriteResponse *>(client_data_temp);
                if (NULL == client_data_local)
                    break;
                //TODO  log statement  below for debug use
                LOGV(LL_DEBUG, "timeout for op:%d,", client_data_local->operation()); 

                client_data_local->set_error(1);
                int32_t response_count = client_data_local->AddResponseCount();
                if (2 == response_count) 
                {
                    delete client_data_local;
                    LOGV(LL_DEBUG, "delete client data for op:%d,",
                         client_data_local->operation()); 
                }
                break;
            }
            case OP_REGISTER:
            {
                SyncResponse * client_data_local =
                                    static_cast<SyncResponse *>(client_data_temp);
                if (NULL == client_data_local)
                    break;
                LOGV(LL_DEBUG, "timeout of register");
                client_data_local->cond().Lock();
                client_data_local->set_wait(false);
                client_data_local->cond().Unlock();
                client_data_local->cond().Signal();
                break;
            }
            case OP_REPLICATEBLOCK:
            {
                SyncResponse * client_data_local = static_cast<SyncResponse *>(client_data_temp);
                if (NULL == client_data_local)
                    break;
                LOGV(LL_DEBUG, "timeout of transferblock");
                client_data_local->cond().Lock();
                int32_t response_count = client_data_local->AddResponseCount();
                int32_t base_count = client_data_local->base_count();
                client_data_local->set_error(1);
                if (response_count == base_count) 
                {
                    client_data_local->set_wait(false);
                    client_data_local->cond().Unlock();
                    client_data_local->cond().Signal();
                    LOGV(LL_DEBUG, "timeout of OP_REPLICATEBLOCK signal");
                } 
                else 
                {
                    client_data_local->cond().Unlock();
                    LOGV(LL_DEBUG, "timeout of OP_REPLICATEBLOCK unlock");
                }
                break;
            }
            default:
            {
                LOGV(LL_INFO, "timeout of other operation:%d");
                //there is no client data here 
                break;
            }
        }
    }
}

void DataServerStreamSocketHandler::OnClose(int error_code)
{
    if (0 < ds_impl_->num_connection()) 
    {
        ds_impl_->NumConnectionSub();
    }
    LOGV(LL_INFO, "OnClose error code:%d", error_code);
}

BladePacket * DataServerStreamSocketHandler::CreatePacket(int16_t operation,
                                                           const Packet & packet)

{
    //unpack the packet and create a new packet to put in the task queue
    BladePacketFactory *packetfactory = packet_factory();
    BladePacket * blade_packet = packetfactory->create_packet(operation); 
    if (NULL == blade_packet) 
    {
        LOGV(LL_ERROR, "create_packet error"); 
        return blade_packet;
    }
    blade_packet->get_net_data()->set_read_data(packet.content(),
                                                packet.length());
    int32_t ret = blade_packet->Unpack();
    if (BLADE_SUCCESS != ret)
    {
        LOGV(LL_ERROR, "packet unpack error,operation:%d", 
                               operation);
        delete blade_packet;
        return NULL;
    }
    blade_packet->set_endpoint(&(end_point()));
    blade_packet->set_channel_id(packet.channel_id());
    blade_packet->set_peer_id(BladeNetUtil::GetPeerID(
                              blade_packet->endpoint_.GetFd()));
    LOGV(LL_DEBUG, "packet create & unpack.type:%d,len:%d", 
                           operation, blade_packet->length());
    return blade_packet;
}

//----------------------DataServerlistensockethandler------------------------//
DataServerListenSocketHandler::DataServerListenSocketHandler(AmFrame& amframe,
                                                            DataServerImpl *impl)
:ListenSocketHandler(amframe)
{
    ds_impl_ = impl;
}

StreamSocketHandler* DataServerListenSocketHandler::OnAccepted(SocketId id)
{
    LOGV(LL_DEBUG, "On Accepted"); 
    //XXX for monitor 
    AtomicInc64(&Singleton<DataServerStatManager>::Instance().request_connection_num_);

    return new DataServerStreamSocketHandler(amframe(), ds_impl_);
}       

void DataServerListenSocketHandler::OnClose(int error_code)
{
    if (0 < ds_impl_->num_connection()) 
    {
        ds_impl_->NumConnectionSub();
    }
    LOGV(LL_DEBUG, "On Close"); 
}

}//end of namespace dataserver
}//end of namespace bladestore
