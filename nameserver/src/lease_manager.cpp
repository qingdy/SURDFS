#include "lease_manager.h"
#include "nameserver_impl.h"
#include "time_util.h"
#include "blade_common_define.h"
#include "lease_expired_packet.h"

namespace bladestore
{
namespace nameserver
{
using namespace std;
using namespace pandora; 
using namespace bladestore::common; 
using namespace bladestore::btree;

int64_t LeaseManager::lease_ID_ = 0; 

LeaseManager::LeaseManager(NameServerImpl * nameserver) 
{	
	nameserver_ = nameserver;
}

LeaseManager::~LeaseManager()
{
    nameserver_ = NULL;
}


void LeaseManager::Start()
{
    lease_check_.Start(this,NULL);
    LOGV(LL_DEBUG, "Lease manager start!");
}


void LeaseManager::Run(CThread * thread,void * arg)
{
    while(1)
    {
	    lease_mutex_.rlock()->lock();
	    LEASE_MAP_ITER iter = lease_map_.begin();
	    for ( ; iter != lease_map_.end(); ++iter)
	    {
		    if ( iter->second.expired_time_ < TimeUtil::GetTime())
		    {
                LeaseExpiredPacket *lease_packet = new LeaseExpiredPacket(iter->second.file_id_, iter->first, iter->second.block_version_);
				lease_packet->is_safe_write_ =  iter->second.is_safe_write_;
                lease_packet->Pack();
                nameserver_->get_write_packet_queue_thread().Push(lease_packet);
            }
	    }
        lease_mutex_.rlock()->unlock();
		usleep(LEASE_CHECK_SLEEP_USECS);//???
    }
}
	
	
int32_t LeaseManager::RenewLease(int64_t blockID)
{
	LEASE_MAP_ITER iter;
	lease_mutex_.wlock()->lock();
	iter = lease_map_.find(blockID);
	if (lease_map_.end() == iter)
	{
		lease_mutex_.wlock() -> unlock();
		LOGV(LL_ERROR, "The BlockID:%d doesnt have a lease. ", blockID);
		return BLADE_ERROR;
	}

	iter->second.last_update_time_ = TimeUtil::GetTime();
	iter->second.expired_time_ = iter->second.last_update_time_ + LEASE_EXPIRE_WINDOW_USECS;
	lease_mutex_.wlock()->unlock();
	return BLADE_SUCCESS;
}

int32_t LeaseManager::register_lease(int64_t fileid, int64_t blockID, int32_t block_version,  bool is_safe_write)
{
	LEASE_MAP_ITER iter;
	lease_mutex_.wlock()->lock();
	iter = lease_map_.find(blockID);
	if (lease_map_.end() != iter)
	{
		lease_mutex_.wlock()->unlock();
		LOGV(LL_ERROR, "The BLockID:%d has a lease!\n", blockID);
		return BLADE_ERROR;
	}

	LeaseInfo temp_lease_info;
	temp_lease_info.lease_ID_ = ++lease_ID_;
	temp_lease_info.file_id_ = fileid;
	temp_lease_info.block_version_ = block_version;
	temp_lease_info.is_safe_write_ = is_safe_write;
	temp_lease_info.last_update_time_ = TimeUtil::GetTime();
	temp_lease_info.expired_time_ = temp_lease_info.last_update_time_ + LEASE_EXPIRE_WINDOW_USECS;

	lease_map_.insert(make_pair(blockID,temp_lease_info));
	lease_mutex_.wlock()->unlock();
    return BLADE_SUCCESS;
}

//正常情况撤销
int32_t LeaseManager::relinquish_lease(int64_t blockID)
{
    LEASE_MAP_ITER iter;
    lease_mutex_.wlock()->lock();
    iter = lease_map_.find(blockID);
    if (lease_map_.end() == iter)
    {
        lease_mutex_.wlock()->unlock();
        LOGV(LL_ERROR, "The BLockID:%d has no lease!", blockID);
        return BLADE_ERROR;
    }
    lease_map_.erase(iter);
    lease_mutex_.wlock()->unlock();
    return BLADE_SUCCESS;
}

bool LeaseManager::has_valid_lease(int64_t blockID)
{
    LEASE_MAP_ITER iter;
    lease_mutex_.rlock()->lock();
    iter = lease_map_.find(blockID);
    if (lease_map_.end() == iter || iter->second.expired_time_ < TimeUtil::GetTime())
    {
        lease_mutex_.rlock()->unlock();
        return false;
    }

    lease_mutex_.rlock()->unlock();
    return true;   
}

bool LeaseManager::has_file_valid_lease(int64_t fileID)
{
	lease_mutex_.rlock()->lock();
	LEASE_MAP_ITER iter = lease_map_.begin();
	for ( ; iter != lease_map_.end(); ++iter)
	{
		if (fileID ==  iter->second.file_id_)
		{
            lease_mutex_.rlock()->unlock();
            return true;
	    }
    }
    lease_mutex_.rlock()->unlock();
    return false;
}

CRWLock & LeaseManager::get_lease_mutex()
{
	return lease_mutex_;
}

}//end of namespace nameserver
}//end of namespace bladestore
