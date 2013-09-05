/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2010-3-30
 *
 */
#ifndef BLADESTORE_MESSAGE_STREAM_SOCKET_HANDLER_H
#define BLADESTORE_MESSAGE_STREAM_SOCKET_HANDLER_H
#include "socket_handler.h"
#include "blade_common_data.h"
#include "blade_packet_factory.h"

using namespace pandora;

namespace bladestore
{
namespace message
{

class BladeStreamSocketHandler : public StreamSocketHandler
{
public:
	BladeStreamSocketHandler(AmFrame & amframe);
	~BladeStreamSocketHandler();
	void OnConnected()
	{

	}
	void OnClose(int)
	{

	}
	void OnTimeout(void *)
	{

	}
	int DetectPacketSize(const void * data, size_t length);	
    void set_packet_factory(BladePacketFactory *packet_factory);
    BladePacketFactory * packet_factory();

protected:
	BladePacketFactory * packet_factory_;
};

}//end of namespace common
}//end of namespace bladestore
#endif
