#include <limits.h>
#include <gtest/gtest.h>
#include <btree_read_handle_new.h>
#include <btree_base.h>

using namespace bladestore::btree;

/**
 * 测试BtreeReadHandle
 */
namespace test
{
namespace btree
{

TEST(BtreeReadHandleTest, construct)
{
	BtreeBaseHandle handle;
	BtreeReadHandle handle1;
	handle1.set_from_key_range(const_cast<char*>(BtreeBase::MIN_KEY), 0, 0);
	handle1.set_to_key_range(const_cast<char*>(BtreeBase::MAX_KEY), 0, 0);
	int32_t size = CONST_KEY_MAX_LENGTH + 1;
	char buffer[size];
	handle1.set_from_key_range(buffer, size, 0);
	handle1.set_to_key_range(buffer, size, 0);
}

}//end of namespace btree
}//end of namespace test

