/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-5-1
 *
 */
#ifndef BLADESTORE_NAMESERVER_STREAM_SOCKET_HANDLER_H
#define BLADESTORE_NAMESERVER_STREAM_SOCKET_HANDLER_H

#include "blade_socket_handler.h"
#include "packet.h"
#include "blade_packet_factory.h"

using namespace bladestore::message;
using namespace pandora;

namespace bladestore
{
namespace nameserver
{

class NameServerImpl;

class BladeNsStreamHandler : public BladeStreamSocketHandler
{
public:
	BladeNsStreamHandler(NameServerImpl & nameserver);
	~BladeNsStreamHandler();

	//StreamSockteHandler需要实现的接口
	void OnReceived(const Packet & packet, void * args);
	void OnTimeout(void * args);

public :
	NameServerImpl & nameserver_;
	BladePacketFactory * packet_factory_;
};

class BladeNsListenHandler : public ListenSocketHandler 
{
public:
	BladeNsListenHandler(NameServerImpl & nameserver);
	~BladeNsListenHandler();

	//ListenSocketHandler需要实现的接口
	StreamSocketHandler* OnAccepted(SocketId id);

	void OnClose(int error_code)
	{

	}

public :
	NameServerImpl & nameserver_;
};

}//end of namespace nameserver
}//end of namespace bladestore
#endif
