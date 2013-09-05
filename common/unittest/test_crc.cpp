#include <fcntl.h>
#include <gtest/gtest.h>
#include <stdlib.h>
#include <unistd.h>
#include "blade_crc.h"


namespace bladestore
{
namespace common
{
class CrcTest : public ::testing::Test
{
protected:
    virtual void SetUp()
    {
    }
    virtual void TearDown()
    {
    }
};

TEST_F(CrcTest, test_crc_generate)
{
    int32_t read_fd = ::open("./blk_3", O_RDONLY);
    ASSERT_NE(-1, read_fd);

    char * data = (char *)malloc(4*1024*1024);
    char * checksum = (char *)malloc(16*1024);
    ASSERT_EQ(2*1024*1024, ::read(read_fd, data, 2*1024*1024));

    bool ret = Func::crc_generate(data, checksum, 2*1024*1024, 16*1024);
    EXPECT_TRUE(ret);

    ret = Func::crc_generate(data, checksum, -1, 16*1024);
    EXPECT_FALSE(ret);

    ret = Func::crc_generate(data, checksum, 12*1024, 16*1024);
    EXPECT_FALSE(ret);

    ret = Func::crc_generate(data, checksum, 2*1024*1024, 0);
    EXPECT_TRUE(ret);

//    ret = Func::crc_generate(data, checksum, 5*1024*1024, 0); //core
//    EXPECT_TRUE(ret);
//
//    ret = Func::crc_generate(data, checksum, 5*1024*1024, 5*1024*1024); //最后一个参数过大导致段错误
//    EXPECT_TRUE(ret);
    ASSERT_EQ(0,::close(read_fd));
}

TEST_F(CrcTest, test_crc_check)
{
    int32_t read_fd = ::open("./blk_3", O_RDONLY);
    ASSERT_NE(-1, read_fd);

    char * data = (char *)malloc(4*1024*1024);
    char * checksum = (char *)malloc(16*1024);
    ASSERT_EQ(2*1024*1024, ::read(read_fd, data, 2*1024*1024));
    
    bool ret = Func::crc_generate(data, checksum, 2*1024*1024, 0);
    EXPECT_TRUE(ret);

    bool ret_1 = Func::crc_check(data, checksum, 2*1024*1024, 0);
    EXPECT_TRUE(ret_1);

    ret_1 = Func::crc_check(data, checksum, 1*1024*1024, 0);
    EXPECT_TRUE(ret_1);

    ret = Func::crc_generate(data, checksum, 2*1024*1024, 16*1024);
    EXPECT_TRUE(ret);
    ret_1 = Func::crc_check(data, checksum, 2*1024*1024, 16*1024);
    EXPECT_TRUE(ret_1);

    ret_1 = Func::crc_check(data, checksum, 2*1024*1024, 0);
    EXPECT_TRUE(ret_1);

    ret_1 = Func::crc_check(data, checksum, 2*1024*1024, 13*1024);
    EXPECT_FALSE(ret_1);

    ASSERT_NE(-1, ::lseek(read_fd, 2*1024*1024, SEEK_SET));
    ASSERT_EQ(2*1024*1024, ::read(read_fd, data, 2*1024*1024));
    ret_1 = Func::crc_check(data, checksum, 2*1024*1024, 16*1024);
    EXPECT_FALSE(ret_1);
    printf("\nret : %d\n", ret_1);

    ASSERT_EQ(0,::close(read_fd));
}

TEST_F(CrcTest, test_crc_update)
{
    int32_t read_fd = ::open("./blk_3", O_RDONLY);
    ASSERT_NE(-1, read_fd);

    char * data = (char *)malloc(4*1024*1024);
    char * checksum = (char *)malloc(16*1024);
    ASSERT_NE(-1, ::lseek(read_fd, 0, SEEK_SET));
    ASSERT_EQ(2*1024*1024, ::read(read_fd, data, 2*1024*1024));

    bool ret = Func::crc_generate(data, checksum, 2*1024*1024, 0);
    EXPECT_TRUE(ret);
    
//    int32_t write_fd = open("./before", O_WRONLY | O_CREAT | O_TRUNC, 0600);
//    ASSERT_NE(-1, write_fd);
//    ASSERT_EQ(16*1024, write(write_fd, checksum, 16*1024));
//    ASSERT_EQ(0, ::close(write_fd));

    ASSERT_NE(-1, ::lseek(read_fd, 2*1024*1024, SEEK_SET));
    ASSERT_EQ(2*1024*1024, ::read(read_fd, data, 2*1024*1024));

    char * checksum1 = (char *)malloc(16*1024);
    EXPECT_STREQ(checksum, strncpy(checksum1, checksum, 16*1024));

    Func::crc_update(checksum, 0, data, 1*1024*1024, 1*1024*1024);
//    write_fd = open("./after", O_WRONLY | O_CREAT | O_TRUNC, 0600);
//    ASSERT_NE(-1, write_fd);
//    ASSERT_EQ(16*1024, write(write_fd, checksum, 16*1024));
//    ASSERT_EQ(0, ::close(write_fd));
    EXPECT_STRNE(checksum1, checksum);

//    Func::crc_update(checksum, 2*1024*1024, data, 1*1024*1024, 1*1024*1024);

    ASSERT_EQ(0,::close(read_fd));

}

}
}
