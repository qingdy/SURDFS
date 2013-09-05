/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-6-8
 *
 */
#include <gtest/gtest.h>

#include "blade_log_entry.h"
#include "blade_common_define.h"


using namespace bladestore::ha;
using namespace bladestore::common;

namespace test
{
namespace ha
{

class TestBladeLogEntry: public ::testing::Test
{
public:
	virtual void SetUp()
	{
	}

	virtual void TearDown()
	{

	}
};

TEST_F(TestBladeLogEntry, test_fill_header)
{
	BladeLogEntry entry;
	char data[BUFSIZ];
	const int64_t buf_len = BUFSIZ + BUFSIZ;
	char buf[buf_len];
	int64_t buf_pos = 0;

	entry.seq_ = 123123;
	entry.cmd_ = BLADE_LOG_SWITCH_LOG;
	EXPECT_EQ(BLADE_INVALID_ARGUMENT, entry.fill_header(NULL, BUFSIZ));
	EXPECT_EQ(BLADE_INVALID_ARGUMENT, entry.fill_header(data, 0));
	EXPECT_EQ(BLADE_INVALID_ARGUMENT, entry.fill_header(data, -1));

	EXPECT_EQ(BLADE_SUCCESS, entry.fill_header(data, BUFSIZ));
	EXPECT_EQ(BUFSIZ, entry.get_log_data_len());
	EXPECT_EQ(BLADE_SUCCESS, entry.serialize(buf, buf_len, buf_pos));
	EXPECT_EQ(buf_pos, entry.get_serialize_size());

	BladeLogEntry entry2;
	int64_t new_pos = 0;
	EXPECT_EQ(BLADE_SUCCESS, entry2.deserialize(buf, buf_len, new_pos));
	EXPECT_EQ(new_pos, buf_pos);
	EXPECT_EQ(BUFSIZ, entry2.get_log_data_len());
	EXPECT_EQ(BLADE_SUCCESS, entry2.check_data_integrity(data));
	data[2] ^= 0xff;
	EXPECT_EQ(BLADE_ERROR, entry2.check_data_integrity(data));
	data[2] ^= 0xff;
	EXPECT_EQ(BLADE_SUCCESS, entry2.check_data_integrity(data));
}

}//end of namespace ha
}//end of namespace test

int main(int argc, char** argv)
{
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
