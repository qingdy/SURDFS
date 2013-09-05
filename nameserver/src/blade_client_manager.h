/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-4-28
 *
 */
#ifndef BLADESTORE_NAMESERVER_CLIENT_MGR_H
#define BLADESTORE_NAMESERVER_CLIENT_MGR_H

#include <set>
#include <vector>

#include "blade_packet.h"
#include "blade_socket_handler.h"
#include "socket_handler.h"
#include "blade_connection_manager.h"
#include "blade_data_buffer.h"
#include "blade_fetch_param.h"

using namespace bladestore::common;
using namespace bladestore::message;
using namespace pandora;
using namespace std;


namespace bladestore
{ 
namespace nameserver
{

class WaitObjectManager;

class BladeClientManager 
{
public:
	BladeClientManager();
	virtual ~BladeClientManager();

public:
	int Initialize(const int64_t max_request_timeout = 5000000);

	//异步接口
	int post_packet(const uint64_t server,  BladePacket* packet) const;

	//同步接口
	int send_packet(const uint64_t server, BladePacket* packet, const int64_t timeout, BladePacket* & response) const;

	int send_get_length_packet(const set<uint64_t> servers, BladePacket* packet, const int64_t timeout, std::vector<BladePacket *> & response);

	int send_log(const uint64_t server, BladeDataBuffer & data_buffer, int64_t log_sync_timeout, BladePacket * & response_packet);

	int slave_register(const uint64_t ns_master, const uint64_t self_addr, BladeFetchParam & fetch_param, int64_t network_timeout);	
	
	int RenewLease(uint64_t server, uint64_t slave_addr, int64_t renew_lease_timeout, BladePacket * & response);

private:

	void Destroy();

private:
	int32_t inited_;

	mutable int64_t max_request_timeout_;

	ConnectionManager* connmgr_;

	WaitObjectManager* waitmgr_;
};

}//end of namespace nameserver
}//end of namespace bladestore

#endif

