/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-5-8
 *
 */

#ifndef BLADESTORE_HA_ROLE_MANAGER_H
#define BLADESTORE_HA_ROLE_MANAGER_H
#include <stdint.h>

#include "blade_common_define.h"

using namespace bladestore::common;

namespace bladestore
{
namespace ha
{

/// BladeRoleMgr管理了进程的角色和状态
class BladeRoleMgr
{
public:

	enum Role
	{
		MASTER = 1,
		SLAVE = 2,
		STANDALONE = 3, // used for test
	};

	/// Master和Slave的状态位
	/// ERROR状态: 错误状态
	/// INIT状态: 初始化
	/// ACTIVE状态: 提供正常服务
	/// SWITCHING: Slave切换为Master过程中的状态
	/// STOP:
	/// Slave切换为Master过程的状态转换顺序是：
	///     (SLAVE, ACTIVE) -> (MASTER, SWITCHING) -> (MASTER, ACTIVE)

	enum State
	{
		ERROR = 1,
		INIT = 2,
		ACTIVE = 3,
		SWITCHING = 4,
		STOP = 5,
		HOLD = 6,
	};

public:

	BladeRoleMgr()
	{
		role_ = MASTER;
		state_ = INIT;
	}
	virtual ~BladeRoleMgr() 
	{ 
	
	}

	inline Role GetRole() const 
	{
		return role_;
	}

	inline void SetRole(const Role role) 
	{
		RoleAtomicExchange(reinterpret_cast<uint32_t*>(&role_), role);
		LOGV(LL_INFO, "set_role=%d state=%d", role_, state_);
	}

	inline State GetState() const 
	{
		return state_;
	}

	inline void SetState(const State state)
	{
		RoleAtomicExchange(reinterpret_cast<uint32_t*>(&state_), state);
		LOGV(LL_INFO, "set_state=%d role=%d", state_, role_);
	}

	static __inline__ uint32_t RoleAtomicExchange(volatile uint32_t * pv, const uint32_t nv) 
    {
		uint32_t res = __sync_lock_test_and_set(pv, nv);
		return res;
    }

private:

	Role role_;
	State state_;
};

}// end of namespace ha
}// end of namespace bladestore

#endif
