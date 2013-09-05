/*
 * funing
 */

#include "string.h"
#include "gtest/gtest.h"

#include "bladestore_ops.h"
#include "blade_common_define.h"

#include "blade_net_util.h"
#include "utility.h"

namespace bladestore
{

namespace common
{
using namespace bladestore::message;
using namespace bladestore::common;
class CommonTest : public ::testing::Test
{
protected:
    virtual void SetUp()
    {
    }
    virtual void TearDown()
    {
    }
    
};

TEST_F(CommonTest, BladeNetUtilTest)
{
    //string to addr
    {
        uint64_t ipport = BladeNetUtil::string_to_addr("202.114.0.242:3000");
        string addr = BladeNetUtil::addr_to_string(ipport); 
        EXPECT_EQ("202.114.0.242:3000", addr);
    }
    {
        uint64_t ipport = BladeNetUtil::string_to_addr("202.114.0.242:1");
        string addr = BladeNetUtil::addr_to_string(ipport); 
        EXPECT_EQ("202.114.0.242:1", addr);
    }
    {
        uint64_t ipport = BladeNetUtil::string_to_addr("0.0.0.1:1");
        string addr = BladeNetUtil::addr_to_string(ipport); 
        EXPECT_EQ("0.0.0.1:1", addr);
    }
    {
        uint64_t ipport = BladeNetUtil::string_to_addr("0.0.0.1:100");
        string addr = BladeNetUtil::addr_to_string(ipport); 
        EXPECT_EQ("0.0.0.1:100", addr);
    }
    {
        uint64_t ipport = BladeNetUtil::string_to_addr("255.255.255.255:65534");
        string addr = BladeNetUtil::addr_to_string(ipport); 
        EXPECT_EQ("255.255.255.255:65534", addr);
    }

}


TEST_F(CommonTest, UtilityTest)
{
    //split_string
    {
        string test = "funing";
        string father;
        string name;
        EXPECT_FALSE(split_string(test, '/', father, name));
    }
    {
        string test = "/";
        string father = "";
        string name = "";
        EXPECT_TRUE(split_string(test, '/', father, name));
        EXPECT_EQ("/", father);
        EXPECT_EQ("", name);
    }
    {
        string test = "/root/123/11111";
        string father = "";
        string name = "";
        EXPECT_TRUE(split_string(test, '/', father, name));
        EXPECT_EQ("/root/123", father);
        EXPECT_EQ("11111", name);
    }
    {
        string test = "/root/";
        string father = "";
        string name = "";
        EXPECT_TRUE(split_string(test, '/', father, name));
        EXPECT_EQ("/root", father);
        EXPECT_EQ("", name);
    }
    {
        string test = "a/b/c";
        string father = "";
        string name = "";
        EXPECT_TRUE(split_string(test, '/', father, name));
        EXPECT_EQ("a/b", father);
        EXPECT_EQ("c", name);
    }
}

TEST_F(CommonTest, CheckTest)
{
    string path;
    for(int i= 0; i < MAX_PATH_LENGTH; i++) {
        path.push_back('a');
    }
    printf("\nsize: %ld\n", path.size());
    EXPECT_TRUE(check_path_length(path));

    path.push_back('a');
    EXPECT_FALSE(check_path_length(path));

    path.clear();
    for(int i= 0; i < MAX_PATH_DEPTH * 10; i++) {
        if(i % 10 == 0)
            path.push_back('/');
        else
            path.push_back('a');
    }

    EXPECT_TRUE(check_path_length(path));

    path.push_back('a');
    path.push_back('/');
    path.push_back('a');
    EXPECT_FALSE(check_path_length(path));
}

}
}
