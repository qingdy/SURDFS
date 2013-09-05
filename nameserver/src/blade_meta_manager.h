/*
 *version : 1.0
 *author  : chen yunyun, funing
 *date    : 2012-4-12
 *
 */
#ifndef BLADESTORE_NAMESERVER_META_MANAGER_H
#define BLADESTORE_NAMESERVER_META_MANAGER_H
#include <ext/hash_map>
#include <set>
#include <string>

#include "layout_manager.h"
#include "lease_manager.h"
#include "blade_block_collect.h"
#include "btree_meta.h"
#include "blade_rwlock.h"
#include "blade_common_data.h"
#include "blade_dataserver_info.h"
#include "heartbeat_packet.h"

namespace bladestore
{
namespace nameserver
{

using namespace bladestore::common;
using namespace bladestore::message;
using namespace bladestore::btree;

class NameServerImpl;
class FileSystemImage;
class BtreeCheckPoint; 

//manage the metadata
class MetaManager
{
public:
	typedef __gnu_cxx::hash_map<uint64_t, std::set<int64_t> > EXPIRE_BLOCK_LIST;

public:
	MetaManager(NameServerImpl * nameserver);
	virtual ~MetaManager();

	void Init();

	//为HeartbeatManager服务
	int32_t register_ds(BladePacket *packet, BladePacket * &);
	int32_t update_ds(HeartbeatPacket* ,HeartbeatReplyPacket * &);
	int join_ds(DataServerInfo * ds_info, bool & is_new);
	int leave_ds(BladePacket * blade_packet);

	//判断nameserver_impl_是不是master, 为HA提供调用接口
	virtual bool is_master();

	//读取和创建block
	int get_block_info(int64_t block_id, std::set<uint64_t> & ds_list);
	int report_blocks(BladePacket *packet);
	int32_t report_bad_block(BladePacket *packet);
	int32_t block_received(BladePacket *packet);
	int add_replicate_info(int64_t block_id, int32_t version, uint64_t source_id, uint64_t dest_id);

	//与Client交互函数
	int32_t meta_blade_create(BladePacket *packet, BladePacket * & reply_packet);
	int32_t meta_blade_mkdir(BladePacket *packet, BladePacket * & reply_packet);
	int32_t meta_blade_add_block(BladePacket *packet, BladePacket * & reply_packet);
	int32_t meta_blade_complete(BladePacket *packet, BladePacket * & reply_packet);
	int32_t meta_blade_get_block_locations(BladePacket *packet, BladePacket * & reply_packet);
	int32_t meta_blade_is_valid_path(BladePacket *packet, BladePacket * & reply_packet);
	int32_t meta_blade_get_file_info(BladePacket *packet, BladePacket * & reply_packet);
	int32_t meta_blade_get_listing(BladePacket *packet, BladePacket *& reply_packet);
	int32_t meta_blade_abandon_block(BladePacket *packet, BladePacket * & reply_packet);
	int32_t meta_blade_rename(BladePacket *packet, BladePacket * & reply_packet);
	int32_t meta_blade_delete_file(BladePacket *packet, BladePacket * & reply_packet);
	int32_t meta_blade_renew_lease(BladePacket *packet, BladePacket * & reply_packet);

	int32_t meta_blade_expire_lease(BladePacket *blade_packet);
	int32_t do_add_block(BladePacket *blade_packet);
	int32_t do_delete_file(BladePacket *packet);

	bool has_valid_lease(int64_t block_id);
	bool has_file_valid_lease(int64_t file_id);

	//本地序列化(checkpoint)需要调用的接口
	FileSystemImage * get_fs_image();
	BtreeCheckPoint * get_btree_checkpoint();

	BladeLayoutManager & get_layout_manager()
	{
		return layout_manager_;
	}

	MetaTree &get_meta_tree()
	{
		return meta_tree_;
	}

private:
	NameServerImpl * nameserver_;

	BladeLayoutManager layout_manager_;

	//对文件系统的目录结构进行管理(操作B+树)
	MetaTree meta_tree_;

	FileSystemImage * fs_image_;

	BtreeCheckPoint * btree_checkpoint_;	
};

class ServerMatch
{
	public:
		ServerMatch(uint64_t ds_id)
		{
			ds_id_ = ds_id;
		}

		bool operator() (DataServerInfo * dataserver_info)
		{
			return dataserver_info->dataserver_id_ == ds_id_;
		}

		uint64_t ds_id_;
};

}//end of namespace nameserver
}//end of namespace bladestore
#endif
