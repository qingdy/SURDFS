/*
 *version  : 1.0
 *author   : chen yunyun
 *date     : 2012-4-28
 */
#ifndef BLADESTORE_NAMESERVER_CONNECTION_MGR_H
#define BLADESTORE_NAMESERVER_CONNECTION_MGR_H

#include <ext/hash_map>

#include "thread_mutex.h"
#include "blade_socket_handler.h"
#include "socket_handler.h"
#include "socket_handler.h"
#include "blade_packet.h"
#include "amframe.h"

using namespace pandora;
using namespace bladestore::message;

namespace bladestore
{
namespace nameserver
{

class StreamPointStruct
{
public:
	AmFrame::StreamEndPoint stream_endpoint_;
	uint64_t peer_id_;
};

typedef __gnu_cxx::hash_map<uint64_t, StreamPointStruct * , __gnu_cxx::hash<uint64_t> > BLADE_CONN_MAP;

class ConnectionManager 
{
public:
	ConnectionManager();

	virtual ~ConnectionManager();

	int Connect(uint64_t server_id);

	void Disconnect(uint64_t server_id);

	bool send_packet(uint64_t server_id, BladePacket *packet, void *args = NULL, int64_t timeout = 50000);

	void CleanUp();

	int get_connection(uint64_t server_id, AmFrame::StreamEndPoint & stream_end_point);

	static bool is_alive(uint64_t server_id);
	
	virtual StreamSocketHandler * create_stream_socket_handler() = 0; 

private:
	int status_;

	BLADE_CONN_MAP connect_map_;

	CThreadMutex mutex_;
};

}//end of namespace nameserver
}//end of namespace bladestore

#endif
