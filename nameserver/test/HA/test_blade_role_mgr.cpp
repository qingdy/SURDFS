/*
 *version  : 1.0
 *author   : chen yunyun
 *date     : 2012-6-8
 */

#include <gtest/gtest.h>
#include <string>

#include "blade_role_mgr.h"

using namespace bladestore::ha;
using namespace std;

namespace test
{
namespace ha
{

class TestBladeRoleMgr: public ::testing::Test
{

};

TEST_F(TestBladeRoleMgr, test_set)
{
	BladeRoleMgr role;
	ASSERT_EQ(BladeRoleMgr::MASTER, role.get_role());
	ASSERT_EQ(BladeRoleMgr::INIT, role.get_state());
	role.set_role(BladeRoleMgr::SLAVE);
	ASSERT_EQ(BladeRoleMgr::SLAVE, role.get_role());
	role.set_state(BladeRoleMgr::SWITCHING);
	ASSERT_EQ(BladeRoleMgr::SWITCHING, role.get_state());
	role.set_role(BladeRoleMgr::MASTER);
	ASSERT_EQ(BladeRoleMgr::MASTER, role.get_role());
	role.set_state(BladeRoleMgr::ACTIVE);
	ASSERT_EQ(BladeRoleMgr::ACTIVE, role.get_state());
}

}//end of namespace ha
}//end of namespace test

int main(int argc, char** argv)
{
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
