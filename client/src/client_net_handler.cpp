#include "client_net_handler.h"
#include "blade_packet_factory.h"
#include "bladestore_ops.h"
#include "write_packet_packet.h"
#include "read_block_packet.h"

namespace bladestore
{
namespace client
{

ClientStreamSocketHandler::ClientStreamSocketHandler(AmFrame& amframe):
                           BladeStreamSocketHandler(amframe)
{
}

void ClientStreamSocketHandler::OnReceived(const Packet& packet, void* client_data)
{
    uint16_t packet_type = 0;
    if (4 == sizeof(size_t)) {
        packet_type = (unsigned char)(packet.content())[4];
        packet_type <<= 8;
        packet_type |= (unsigned char)(packet.content())[5];
    } else if (8 == sizeof(size_t)) {
        packet_type = (unsigned char)(packet.content())[8];
        packet_type <<= 8;
        packet_type |= (unsigned char)(packet.content())[9];
    }
    int16_t operation = (int16_t) packet_type;
    LOGV(LL_INFO, "packet OnReceived, type:%d.", operation);
    BladePacket *blade_packet = CreatePacket(operation, packet);
    if (!blade_packet)
        return;
    
    switch (operation)
    {
        case OP_READBLOCK_REPLY :
        {
            if (!client_data)
                break;
            ResponseSync *response = static_cast<ResponseSync*>(client_data);
            if (!response->is_sync_packet()) {
                ReadBlockReplyPacket *packet = static_cast<ReadBlockReplyPacket*>(blade_packet);
                response->cond().Lock();
                int64_t seq = packet->sequence();
                LOGV(LL_DEBUG, "recieve read block seq:%ld.", seq);
                response->AddResponsePacket(seq, (BladePacket*)packet);
                int32_t response_count = response->AddResponseCount();
                int32_t unit_total_count = response->unit_total_count();
                if (response_count == unit_total_count) {
                    response->set_wait(false);
                    response->cond().Unlock();
                    response->cond().Signal();
                    LOGV(LL_DEBUG, "read block signal success");
                } else {
                    response->cond().Unlock();
                }
            } else {
                ResponseSync* response = static_cast<ResponseSync*>(client_data);
                response->cond().Lock();
                response->set_response_packet(blade_packet);
                response->set_response_error(false);
                response->set_wait(false);
                response->cond().Unlock();
                response->cond().Signal();
            }
            break;
        }
        case OP_WRITEPACKET_REPLY :
        {
            if (!client_data)
                break;
            ResponseSync *response = static_cast<ResponseSync*>(client_data);
            if (!response->is_sync_packet()) {
                WritePacketReplyPacket *packet = static_cast<WritePacketReplyPacket*>(blade_packet);
                assert(packet);
                response->cond().Lock();
                int64_t seq = packet->sequence();
                LOGV(LL_DEBUG, "receive write packet seq:%ld.", seq);
                response->AddResponsePacket(seq, (BladePacket*)packet);
                int32_t response_count = response->AddResponseCount();
                int32_t unit_total_count = response->unit_total_count();
                if (response_count == unit_total_count) {
                    response->set_wait(false);
                    response->cond().Unlock();
                    response->cond().Signal();
                    LOGV(LL_DEBUG, "write packet signal success");
                } else {
                    response->cond().Unlock();
                }
            } else {
                ResponseSync* response = static_cast<ResponseSync*>(client_data);
                response->cond().Lock();
                response->set_response_packet(blade_packet);
                response->set_response_error(false);
                response->set_wait(false);
                response->cond().Unlock();
                response->cond().Signal();
            }
            break;
        }
        default:
        {
            if (!client_data)
                break;
            ResponseSync* response = static_cast<ResponseSync*>(client_data);
            response->cond().Lock();
            response->set_response_packet(blade_packet);
            response->set_response_error(false);
            response->set_wait(false);
            response->cond().Unlock();
            response->cond().Signal();
            break;
        }
    }
}

void ClientStreamSocketHandler::OnClose(int error_code)
{
    LOGV(LL_INFO, "OnClosed, error code: %d.", error_code);
}

void ClientStreamSocketHandler::OnTimeout(void *client_data)
{
    ClientResponse *response = static_cast<ClientResponse *>(client_data);
    LOGV(LL_INFO, "OnTimeout of operation %d.",response->operation());
    switch (response->operation())
    {
        case OP_WRITEPACKET:
        case OP_READBLOCK:
        {
            ResponseSync *response = static_cast<ResponseSync *>(client_data);
            LOGV(LL_DEBUG, "timeout of RW, response count:%d", response->response_count());
            if (!response->is_sync_packet()) {
                response->cond().Lock();
                int32_t response_count = response->AddResponseCount();
                int32_t unit_total_count = response->unit_total_count();
                if (response_count == unit_total_count) {
                    response->set_response_error(true);
                    response->set_wait(false);
                    response->cond().Unlock();
                    response->cond().Signal();
                    LOGV(LL_DEBUG, "timeout of RW signal");
                } else {
                    response->cond().Unlock();
                    LOGV(LL_DEBUG, "timeout of RW unlock");
                }
                break;
            }
        }
        default:
        {
            if (!client_data)
                break;
            ResponseSync* response = static_cast<ResponseSync*>(client_data);
            response->cond().Lock();
            response->set_response_error(true);
            response->set_wait(false);
            response->cond().Unlock();
            response->cond().Signal();
            break;
        }
    }
}

void ClientStreamSocketHandler::OnConnected()
{
    LOGV(LL_INFO, "OnConnected.");
}

BladePacket * ClientStreamSocketHandler::CreatePacket(int16_t operation, const Packet & packet)
{
    BladePacketFactory *packetfactory = packet_factory();
    BladePacket *blade_packet = packetfactory->create_packet(operation); 
    if (!blade_packet) {
        LOGV(LL_ERROR, "create packet error"); 
        return NULL;
    }
    blade_packet->get_net_data()->set_read_data(packet.content(), packet.length());
    int32_t ret = blade_packet->Unpack();
    if (BLADE_SUCCESS != ret){
        LOGV(LL_ERROR, "packet unpack error,operation:%d", operation);
        delete blade_packet;
        return NULL;
    }
    LOGV(LL_DEBUG, "packet create & unpack.type:%d,len:%d", operation, blade_packet->length());
    return blade_packet;
}

}// end of namespace client
}// end of namespace bladestore
