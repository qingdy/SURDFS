/*
 * Sohu R&D  2012
 *
 * File Description:
 *      The defination  class of net handlers
 *
 * Author   : @YYY
 * Version  : 1.0
 * Date     : 2012-06-04
 */

#ifndef BLADESTORE_DATASERVER_NET_HANDLER_H
#define BLADESTORE_DATASERVER_NET_HANDLER_H

#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include "atomic.h"
#include "blade_common_define.h"
#include "blade_socket_handler.h"

#ifdef GTEST 
#define private public
#define protected public
#endif

namespace bladestore
{
namespace dataserver
{
using pandora::AmFrame;
using pandora::Packet;
using pandora::atomic_t;
using namespace bladestore::common;
using namespace bladestore::message;

class DataServerImpl;
class message::BladePacket;

class DataServerStreamSocketHandler : public BladeStreamSocketHandler
{
public:
    DataServerStreamSocketHandler(AmFrame &amframe, DataServerImpl *impl);
    virtual ~DataServerStreamSocketHandler() { }

    void OnConnected();
    void OnReceived(const Packet& packet, void *client_data);
    void OnTimeout(void *client_data);
    void OnClose(int error_code);

private:
    BladePacket * CreatePacket(int16_t operation, const Packet &packet);
    DataServerImpl * ds_impl_;

    DISALLOW_COPY_AND_ASSIGN(DataServerStreamSocketHandler);
};

class DataServerListenSocketHandler : public ListenSocketHandler
{
public:
    DataServerListenSocketHandler(AmFrame& amframe, DataServerImpl *impl);
    virtual ~DataServerListenSocketHandler(){}
    StreamSocketHandler* OnAccepted(SocketId id);
    void OnClose(int error_code);

private:
    DataServerImpl * ds_impl_;

    DISALLOW_COPY_AND_ASSIGN(DataServerListenSocketHandler);
};

}//end of namespace dataserver
}//end of namespace bladestore

#endif

