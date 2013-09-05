#include "blade_connection_manager.h"
#include "blade_common_define.h"
#include "blade_common_data.h"
#include "log.h"
#include "blade_net_util.h"

namespace bladestore
{
namespace nameserver
{

ConnectionManager::ConnectionManager() 
{
	status_ = 0;
}

ConnectionManager::~ConnectionManager() 
{

}

void ConnectionManager::CleanUp() 
{
	status_ = 1;
	mutex_.Lock();
	connect_map_.clear();
	mutex_.Unlock();
}

int ConnectionManager::get_connection(uint64_t server_id, AmFrame::StreamEndPoint & stream_end_point) 
{
	int return_code = BLADE_SUCCESS;
	if (1 == status_) 
	{
		return BLADE_ERROR;
	}

	if (0 == server_id) 
	{
		return BLADE_ERROR;
	}

	CThreadGuard guard(&mutex_);

	BLADE_CONN_MAP::iterator it ;
	it = connect_map_.find(server_id);
	if (it != connect_map_.end() && (BladeNetUtil::GetPeerID((it->second->stream_endpoint_).GetFd()) == it->second->peer_id_))
	{
		stream_end_point = it->second->stream_endpoint_;
		return return_code;
	}
	else if (it != connect_map_.end())
	{
		delete it->second;
		it->second = NULL;
		connect_map_.erase(it);
	}

	char spec[64];
	sprintf(spec, "%s", BladeNetUtil::AddrToString(server_id).c_str());
	StreamSocketHandler * packet_handler =  create_stream_socket_handler();
	int64_t socket_fd = (BladeCommonData::amframe_).AsyncConnect(spec, packet_handler);

	if (socket_fd <= 0)
	{
		return_code = BLADE_CONNECT_ERROR;
		LOGV(LL_WARN, "connect to server fail (server_id: %s)", spec);
	}
	else 
	{
		StreamPointStruct * stream_point_struct = new StreamPointStruct();
		stream_point_struct->stream_endpoint_= packet_handler->end_point();	
		stream_end_point= packet_handler->end_point();	
		stream_point_struct->peer_id_ = server_id;	
		connect_map_[server_id] = stream_point_struct;
	}

	return return_code;
}

int  ConnectionManager::Connect(uint64_t server_id) 
{
	int return_code = BLADE_SUCCESS;
	AmFrame::StreamEndPoint stream_end_point;
	return_code = get_connection(server_id, stream_end_point);
	return return_code;
}

void ConnectionManager::Disconnect(uint64_t server_id) 
{
	mutex_.Lock();
	BLADE_CONN_MAP::iterator it = connect_map_.find(server_id);
	if (it != connect_map_.end() && (BladeNetUtil::GetPeerID((it->second->stream_endpoint_).GetFd()) == it->second->peer_id_))
	{
		BladeCommonData::amframe_.CloseEndPoint(it->second->stream_endpoint_);
		connect_map_.erase(it);
	}
	else if(it != connect_map_.end())
	{
		connect_map_.erase(it);
	}

	mutex_.Unlock();
}

bool ConnectionManager::send_packet(uint64_t server_id, BladePacket * packet, void *args, int64_t timeout) 
{
	AmFrame::StreamEndPoint end_point;
	int return_code = get_connection(server_id, end_point);
	if (BLADE_SUCCESS == return_code)
	{
		int ret = BladeCommonData::amframe_.SendPacket(end_point, packet, true, args, timeout);
        LOGV(LL_DEBUG, "SEND PACKET, fd : %d, ret : %d", end_point.GetFd(), ret);
        return ret == 0;
	}
	return false;
}


//测试一台机器是否活着,进行简单的连接
bool ConnectionManager::is_alive(uint64_t server_id) 
{
//	Socket socket;
//	char ip[32];
//	strcpy(ip, BladeNetUtil::addr_to_string((serverId&0xffffffff)).c_str());
//	int port = ((serverId >> 32) & 0xffff);
//	if (socket.setAddress(ip, port) == false) 
//	{
//		return false;
//	}
//	socket.setTimeOption(SO_RCVTIMEO, 1000);
//	socket.setTimeOption(SO_SNDTIMEO, 1000);
//	if (socket.connect()) 
//	{
//		return true;
//	}
	return false;
}

}//end of namespace nameserver
}//end of namespace bladestore

