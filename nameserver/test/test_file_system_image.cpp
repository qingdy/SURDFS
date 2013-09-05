/*
 *version : 1.0
 *author  : chen yun yun
 *date    : 2012-6-8
 *
 */

#include <gtest/gtest.h>

#include <fcntl.h>
#include <string>
#include <errno.h>
#include <assert.h>

namespace test
{
namespace nameserver
{

class TestFileSystemImage: public ::testing::Test
{
protected:
    virtual void SetUp()
    {
    }
    virtual void TearDown()
    {
    }

};

TEST_F(TestFileSystemImage, test_read_write)
{
	struct ImageHeader
	{
		int32_t x;
		int32_t y;
		int32_t z;
	}image_header;

	image_header.x = 1;
	image_header.y = 2;
	image_header.z = 3;
	char * filename = "file_test.txt";	
	int32_t write_fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	int32_t version = 100;
	EXPECT_EQ(write(write_fd, &image_header, sizeof(ImageHeader)) , sizeof(ImageHeader));
	EXPECT_EQ(write(write_fd, &version, sizeof(int32_t)) , sizeof(int32_t));
	close(write_fd);

	image_header.x = 0;
	image_header.y = 0;
	image_header.z = 0;

	int32_t read_fd = ::open(filename, O_RDONLY);
	version = 0;
	EXPECT_EQ(read(read_fd, &image_header, sizeof(ImageHeader)) , sizeof(ImageHeader));
	EXPECT_EQ(read(read_fd, &version, sizeof(int32_t)) , sizeof(int32_t));

	EXPECT_EQ(1, image_header.x);
	EXPECT_EQ(2, image_header.y);
	EXPECT_EQ(3, image_header.z);
	EXPECT_EQ(100, version);
}

}//end of namespace nameserver
}//end of namespace test

int main(int argc, char** argv)
{
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
