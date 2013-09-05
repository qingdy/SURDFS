#ifndef BLADESTORE_CLIENT_CLIENT_NET_HANDLER_H
#define BLADESTORE_CLIENT_CLIENT_NET_HANDLER_H

#include "blade_socket_handler.h"
#include "log.h"
#include "client_response.h"
#include "blade_common_define.h"

namespace bladestore
{
namespace client
{

class ClientStreamSocketHandler : public BladeStreamSocketHandler
{
public:
    ClientStreamSocketHandler(AmFrame& amframe);
    void OnReceived(const Packet& packet, void* client_data);
    void OnClose(int error_code);
    void OnTimeout(void* client_data);
    void OnConnected();

private:
    BladePacket *CreatePacket(int16_t operation, const Packet & packet);
    DISALLOW_COPY_AND_ASSIGN(ClientStreamSocketHandler);
};

//模版 template
template <typename Sent>
BladePacket *RequestToNameServer(Sent *sent, ResponseSync *response,
                                 ClientStreamSocketHandler *handler)
{
    if (BLADE_SUCCESS != sent->Pack()) {
        LOGV(LL_ERROR, "pack the sendpacket error.");
        return NULL;
    }
    response->set_request_packet(sent);
    int ret = BladeCommonData::amframe_.SendPacket(
                                            handler->end_point(),
                                            response->request_packet(),
                                            true,
                                            (void *)response,
                                            CLIENT_REQUEST_TO_NS_TIME_OUT);
    if (ret == 0) {
        LOGV(LL_DEBUG, "packet already sent.");
        response->cond().Lock();
        while (response->wait() == true) {
            response->cond().Wait();
        }
        response->cond().Unlock();
        if (!response->response_error()) {
            response->set_request_packet(NULL);
            return response->response_packet();
        } else {
            response->set_request_packet(NULL);
            LOGV(LL_ERROR, "Packet already sent, but response not ok.");
        }
    } else {
        LOGV(LL_ERROR, "Send packet error, error: %d.", ret);
    }
    return NULL;
}

template <typename Sent>
BladePacket *RequestToDataServer(Sent *sent, ResponseSync *response,
                                 ClientStreamSocketHandler *handler)
{
    // resend when failed to send packet to dataserver
    for (int32_t i = 0; i < CLIENT_MAX_RESEND_TIMES; ++i) {
        Sent *actually_sent = new Sent(*sent);
        assert(actually_sent);
        response->set_request_packet(sent);
        LOGV(LL_DEBUG, "set wait.");
        response->set_wait(true);
        int ret = BladeCommonData::amframe_.SendPacket(
                                        handler->end_point(),
                                        actually_sent,
                                        true,
                                        (void*)response,
                                        CLIENT_REQUEST_TO_DS_TIME_OUT);

        if (ret == 0) {
            LOGV(LL_DEBUG, "packet already sent.");
            response->cond().Lock();
            while (response->wait() == true) {
                response->cond().Wait();
            }
            response->cond().Unlock();

            if (!response->response_error()) {
                return response->response_packet();
            } else {
                LOGV(LL_ERROR, "Packet already sent, but response not ok");
            }
        } else {
            delete actually_sent;
            LOGV(LL_ERROR, "Send packet error, error: %d", ret);
        }
    }
    return NULL;
}

template <typename Sent>
bool RWRequestToDataServer(Sent *sent, int64_t seq, ResponseSync *response,
                           ClientStreamSocketHandler *handler)
{
    // resend when failed to send packet to dataserver
    for (int32_t i = 0; i < CLIENT_MAX_RESEND_TIMES; ++i) {
        Sent *actually_sent = new Sent(*sent);
        assert(actually_sent);
        int ret = BladeCommonData::amframe_.SendPacket(
                                        handler->end_point(),
                                        actually_sent,
                                        true,
                                        (void*)response,
                                        CLIENT_RW_REQUEST_TO_DS_TIME_OUT);
        if (ret == 0) {
            LOGV(LL_DEBUG, "packet already sent.");
            LOGV(LL_DEBUG, "end_point.GetFd(): %d.", handler->end_point().GetFd());
            response->AddRequestPacket(seq, sent);
            return true;
        } else {
            LOGV(LL_ERROR, "end_point.GetFd(): %d.", handler->end_point().GetFd());
            delete actually_sent;
            LOGV(LL_ERROR, "Send packet error, error: %d", ret);
        }
    }
    return false;
}

}//end of namespace client
}//end of namespace bladestore
#endif
