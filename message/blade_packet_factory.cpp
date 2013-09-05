#include "blade_packet_factory.h"
#include "blade_slave_register_packet.h"
#include "status_packet.h"
#include "blade_log_packet.h"
#include "get_listing_packet.h"
#include "read_block_packet.h"
#include "write_pipeline_packet.h"
#include "abandon_block_packet.h"
#include "add_block_packet.h"
#include "complete_packet.h"
#include "create_packet.h"
#include "delete_file_packet.h"
#include "get_block_locations_packet.h"
#include "get_file_info_packet.h"
#include "get_listing_packet.h"
#include "is_valid_path_packet.h"
#include "mkdir_packet.h"
#include "rename_packet.h"
#include "renew_lease_packet.h"

#include "register_packet.h"
#include "block_report_packet.h"
#include "block_received_packet.h"
#include "bad_block_report_packet.h"
#include "heartbeat_packet.h"
#include "block_to_get_length_packet.h"

#include "read_block_packet.h"
#include "write_pipeline_packet.h"
#include "write_packet_packet.h"
#include "replicate_block_packet.h"

#include "blade_ns_lease_packet.h"
#include "leave_ds_packet.h"
#include "lease_expired_packet.h"

using namespace bladestore::message;

namespace bladestore
{
namespace message
{

BladePacketFactory::BladePacketFactory()
{

}

BladePacketFactory::~BladePacketFactory()
{

}

BladePacket * BladePacketFactory::create_packet(int16_t operation)
{
	BladePacket * packet;
	switch(operation)
	{
        case OP_ABANDONBLOCK :  packet = new AbandonBlockPacket(); break;
        case OP_ABANDONBLOCK_REPLY :  packet = new AbandonBlockReplyPacket(); break;
        case OP_ADDBLOCK   :    packet = new AddBlockPacket(); break;
        case OP_ADDBLOCK_REPLY   :    packet = new AddBlockReplyPacket(); break;
        case OP_BAD_BLOCK_REPORT:   packet = new BadBlockReportPacket();break;
        case OP_BLOCK_RECEIVED: packet = new BlockReceivedPacket();break; 
        case OP_BLOCK_REPORT :  packet = new BlockReportPacket();break;
        case OP_BLOCK_TO_GET_LENGTH :    packet = new BlockToGetLengthPacket();break;
        case OP_COMPLETE   :    packet = new CompletePacket(); break;
        case OP_COMPLETE_REPLY   :    packet = new CompleteReplyPacket(); break;
        case OP_CREATE     :    packet = new CreatePacket(); break;
        case OP_CREATE_REPLY     :    packet = new CreateReplyPacket(); break;
        case OP_DELETEFILE :    packet = new DeleteFilePacket(); break;
        case OP_DELETEFILE_REPLY :    packet = new DeleteFileReplyPacket(); break;
        case OP_GETBLOCKLOCATIONS : packet = new GetBlockLocationsPacket(); break;
        case OP_GETBLOCKLOCATIONS_REPLY : packet = new GetBlockLocationsReplyPacket(); break;
        case OP_GETFILEINFO :   packet = new GetFileInfoPacket(); break;
        case OP_GETFILEINFO_REPLY :   packet = new GetFileInfoReplyPacket(); break;
        case OP_GETLISTING :    packet = new GetListingPacket();break;
        case OP_GETLISTING_REPLY :    packet = new GetListingReplyPacket();break;
        case OP_HEARTBEAT  :    packet = new HeartbeatPacket(); break;
        case OP_ISVALIDPATH :   packet = new IsValidPathPacket(); break;
        case OP_ISVALIDPATH_REPLY :   packet = new IsValidPathReplyPacket(); break;
        case OP_MKDIR      :    packet = new MkdirPacket(); break; 
        case OP_MKDIR_REPLY      :    packet = new MkdirReplyPacket(); break; 
        case OP_REGISTER   :    packet = new RegisterPacket();break;
        case OP_RENAME     :    packet = new RenamePacket(); break;
        case OP_RENAME_REPLY     :    packet = new RenameReplyPacket(); break;
        case OP_RENEWLEASE :    packet = new RenewLeasePacket(); break;
        case OP_RENEWLEASE_REPLY :    packet = new RenewLeaseReplyPacket(); break;

        case OP_READBLOCK  :    packet = new ReadBlockPacket();break;
        case OP_READBLOCK_REPLY  :    packet = new ReadBlockReplyPacket();break;
        case OP_WRITEPIPELINE  :    packet = new WritePipelinePacket();break;
        case OP_WRITEPIPELINE_REPLY  :    packet = new WritePipelineReplyPacket();break;
        case OP_WRITEPACKET  :    packet = new WritePacketPacket();break;
        case OP_WRITEPACKET_REPLY  :    packet = new WritePacketReplyPacket();break;
        case OP_REPLICATEBLOCK  :    packet = new ReplicateBlockPacket();break;
        case OP_REPLICATEBLOCK_REPLY  :    packet = new ReplicateBlockReplyPacket();break;

   
        case OP_BLOCK_REPORT_REPLY :    packet = new BlockReportReplyPacket(); break;
        case OP_BLOCK_TO_GET_LENGTH_REPLY : packet = new BlockToGetLengthReplyPacket();break;
        case OP_HEARTBEAT_REPLY :       packet = new HeartbeatReplyPacket(); break;
        case OP_REGISTER_REPLY :        packet = new RegisterReplyPacket(); break;

        case OP_NS_RENEW_LEASE :        packet = new BladeRenewLeasePacket();break;
        case OP_NS_GRANT_LEASE :        packet = new BladeGrantLeasePacket();break;
		case OP_NS_SLAVE_REGISTER:		packet = new BladeSlaveRegisterPacket();break;
		case OP_NS_SLAVE_REGISTER_REPLY:		packet = new BladeSlaveRegisterReplyPacket();break;
		case OP_NS_LOG : 						packet = new BladeLogPacket(); break;
		case OP_STATUS : 						packet = new StatusPacket(); break;
        case OP_NS_ADD_BLOCK :                  packet = new LogAddBlockPacket(); break;
        case OP_LEAVE_DS:                  packet = new LeaveDsPacket(); break;
        case OP_LEASEEXPIRED :          packet = new LeaseExpiredPacket(); break;
        default :               packet = NULL;
	}
	return packet;
}

}//end of namespace message
}//end of namespace bladestore
