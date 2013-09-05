/*
 *version  :  1.0
 *author   :  funing
 *date     :  2012-4-19
 *
 */
#ifndef BLADESTORE_NAMESERVER_LEASE_MANAGER_H
#define BLADESTORE_NAMESERVER_LEASE_MANAGER_H

#include<ext/hash_map>
#include<stdint.h>

#include "blade_packet.h"
#include "cthread.h"
#include "thread_mutex.h"
#include "blade_rwlock.h"
#include "meta.h"

#ifdef GTEST
#define private public
#define protected public
#endif


namespace bladestore
{
namespace nameserver
{
using namespace pandora;

class NameServerImpl;

struct LeaseInfo
{
	int64_t lease_ID_;
	int64_t file_id_;
	int32_t block_version_;
	int64_t last_update_time_;
	int64_t expired_time_;
	bool is_safe_write_;
};

typedef __gnu_cxx::hash_map<int64_t, LeaseInfo , __gnu_cxx::hash<int64_t> > LEASE_MAP;
typedef __gnu_cxx::hash_map<int64_t, LeaseInfo , __gnu_cxx::hash<int64_t> >::iterator LEASE_MAP_ITER;

class LeaseManager : public Runnable
{
public:
	LeaseManager(NameServerImpl * nameserver);
	~LeaseManager();
	void Start();    
	void Run(CThread * thread,void * arg);	

	int32_t register_lease(int64_t file_id, int64_t blockID, int32_t block_version = 0, bool is_safe_write = false);    
	bool has_valid_lease(int64_t blockID);
	bool has_file_valid_lease(int64_t fileID);
	int32_t RenewLease(int64_t blockID);
	int32_t relinquish_lease(int64_t blockID);//normal abandon lease 
	int32_t abandon_lease(int64_t blockID);//abnormal abandon lease

	CRWLock & get_lease_mutex();

public:
	LEASE_MAP lease_map_;

private:
	DISALLOW_COPY_AND_ASSIGN(LeaseManager);
	//inc forever
	static int64_t lease_ID_;
	NameServerImpl * nameserver_;

	CRWLock lease_mutex_;
	CThread lease_check_;
};

}//end of namespace nameserver
}//end of namespace bladestore
#endif

