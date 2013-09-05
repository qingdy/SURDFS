/*
 * Because of functions here will make an effect on nameserver,
 * so this test will pass only if the nameserver was first started.
 */
#include <fcntl.h>
#include "client.h"
#include "bladestore_ops.h"
#include "blade_common_define.h"
#include "located_blocks.h"
#include "blade_file_info.h"
#include "bladestore_ops.h"
#include "gtest/gtest.h"
namespace bladestore
{

namespace client
{
using namespace bladestore::message;
using namespace bladestore::common;
class ClientTest : public ::testing::Test
{
protected:
    static void SetUpTestCase()
    {
        client = new Client();
        int ret = client->Init();
        if (BLADE_ERROR == ret)
            abort();
        client->SetWorkingDir("/cp");
    }
    static void TearDownTestCase()
    {
        delete client;
        client = NULL;
    }
    static Client *client;
};

Client * ClientTest::client = NULL;

// MkAbPath now made private, this test abandoned
// TEST_F(ClientTest, MkAbPath)
// {
//     ASSERT_TRUE(client != NULL);
//     EXPECT_EQ("/a/b/c", client->make_absolute_path("/a/b/c/"));
//     EXPECT_EQ("/cp/a/b/c", client->make_absolute_path("a/b/c/"));
//     EXPECT_EQ("/", client->make_absolute_path("/"));
//     EXPECT_EQ("/root", client->make_absolute_path("~"));
//     EXPECT_EQ("/cp", client->make_absolute_path("."));
//     EXPECT_EQ("", client->make_absolute_path("//"));
//     EXPECT_EQ("/cp/abcde/ff", client->make_absolute_path("./abcde/ff/"));
//     EXPECT_EQ("/root/abcde/ff", client->make_absolute_path("~/abcde/ff/"));
//     EXPECT_EQ("", client->make_absolute_path("~/abc/./de/ff/"));
//     EXPECT_EQ("/root", client->make_absolute_path("~/"));
//     EXPECT_EQ("/cp", client->make_absolute_path("./"));
//     EXPECT_EQ("", client->make_absolute_path("/a/b\\c"));
// }

TEST_F(ClientTest, GetListing)
{
    ASSERT_TRUE(client != NULL);
    vector<string> file_names;
    file_names.push_back("/");
    EXPECT_EQ(file_names, client->GetListing("/"));
    EXPECT_EQ(file_names, client->GetListing("~"));
    client->MakeDirectory("/mytest");
    client->MakeDirectory("/mytest/test1");
    int64_t fid = client->Create("/mytest/test2", 0, 3);
    file_names.push_back("mytest");
    EXPECT_EQ(file_names, client->GetListing("/"));
    EXPECT_EQ(file_names, client->GetListing("~"));
    EXPECT_EQ(file_names, client->GetListing("."));
    file_names.clear();
    EXPECT_EQ(BLADE_SUCCESS, client->SetWorkingDir("mytest/test1"));
    EXPECT_EQ(file_names, client->GetListing("."));
    EXPECT_EQ(file_names, client->GetListing("/mytest/test1"));
    EXPECT_EQ(file_names, client->GetListing("/mytest/test2"));
    EXPECT_EQ(file_names, client->GetListing("/mytest test2"));
    EXPECT_EQ(file_names, client->GetListing("/invalidpath"));
    EXPECT_EQ(file_names, client->GetListing(""));
    file_names.push_back("test1");
    file_names.push_back("test2");
    EXPECT_EQ(file_names, client->GetListing("/mytest"));
    EXPECT_EQ(BLADE_SUCCESS, client->SetWorkingDir("/mytest"));
    EXPECT_EQ(file_names, client->GetListing("."));
    EXPECT_EQ(BLADE_SUCCESS, client->SetWorkingDir("/mytest/test1"));
    EXPECT_EQ(file_names, client->GetListing(".."));

    // end Get_listing testing and delete created dir and files
    EXPECT_EQ(BLADE_SUCCESS, client->Delete("/mytest/test1"));
    EXPECT_EQ(BLADE_SUCCESS, client->Delete("/mytest/test2"));
    EXPECT_EQ(BLADE_SUCCESS, client->Delete("/mytest"));
}

TEST_F(ClientTest, MkDir)
{
    ASSERT_TRUE(client != NULL);
    EXPECT_EQ(BLADE_ERROR, client->MakeDirectory("/"));
    EXPECT_EQ(BLADE_SUCCESS, client->MakeDirectory("/test"));
    EXPECT_EQ(BLADE_ERROR, client->MakeDirectory("/test"));
    EXPECT_EQ(BLADE_SUCCESS, client->MakeDirectory("/test/test1"));
    EXPECT_EQ(BLADE_ERROR, client->MakeDirectory("/nonexist/test1"));
    EXPECT_GT(client->Create("/file", 0, 3), ROOTFID);
    EXPECT_EQ(BLADE_ERROR, client->MakeDirectory("/file/testdir"));
    EXPECT_EQ(BLADE_SUCCESS, client->SetWorkingDir("/test"));
    EXPECT_EQ(BLADE_SUCCESS, client->MakeDirectory("test2"));
    EXPECT_EQ(BLADE_ERROR, client->MakeDirectory(""));
    EXPECT_EQ(BLADE_ERROR, client->MakeDirectory("."));
    EXPECT_EQ(BLADE_SUCCESS, client->SetWorkingDir("/test/test2"));
    EXPECT_EQ(BLADE_ERROR, client->MakeDirectory(".."));
    EXPECT_EQ(BLADE_ERROR, client->MakeDirectory("/.."));
    EXPECT_EQ(BLADE_ERROR, client->MakeDirectory("/a b"));
    EXPECT_EQ(BLADE_ERROR, client->MakeDirectory("a b"));
    EXPECT_EQ(BLADE_SUCCESS, client->SetWorkingDir("/"));

    EXPECT_EQ(BLADE_SUCCESS, client->Delete("/test/test2"));
    EXPECT_EQ(BLADE_SUCCESS, client->Delete("/test/test1"));
    EXPECT_EQ(BLADE_SUCCESS, client->Delete("/test"));
    EXPECT_EQ(BLADE_SUCCESS, client->Delete("/file"));
}

TEST_F(ClientTest, SetWorkingDir)
{
    ASSERT_TRUE(client != NULL);
    EXPECT_EQ(BLADE_SUCCESS, client->MakeDirectory("/test"));
    EXPECT_EQ(BLADE_SUCCESS, client->SetWorkingDir("test"));
    EXPECT_EQ(BLADE_SUCCESS, client->SetWorkingDir("/test"));
    EXPECT_EQ(BLADE_ERROR, client->SetWorkingDir("/notexistdir"));
    EXPECT_GT(client->Create("/testfile", 0, 3), ROOTFID);
    EXPECT_EQ(BLADE_ERROR, client->SetWorkingDir("/testfile"));
    EXPECT_EQ(BLADE_SUCCESS, client->MakeDirectory("/test/test1"));
    EXPECT_EQ(BLADE_SUCCESS, client->MakeDirectory("/test/test1/test2"));
    EXPECT_EQ(BLADE_SUCCESS, client->MakeDirectory("/test/test1/test3"));
    EXPECT_EQ(BLADE_SUCCESS, client->SetWorkingDir("test1/test2"));
    EXPECT_EQ(BLADE_SUCCESS, client->SetWorkingDir("../../"));
    EXPECT_EQ(BLADE_SUCCESS, client->SetWorkingDir("test1/test2"));
    EXPECT_EQ(BLADE_SUCCESS, client->SetWorkingDir("../test3"));
    EXPECT_EQ(BLADE_ERROR, client->SetWorkingDir(""));
    EXPECT_EQ(BLADE_ERROR, client->SetWorkingDir("/test xxx"));
    EXPECT_EQ(BLADE_ERROR, client->SetWorkingDir("/nonexist xxx"));
    EXPECT_EQ(BLADE_SUCCESS, client->SetWorkingDir("/"));
    EXPECT_EQ(BLADE_ERROR, client->SetWorkingDir(".."));
    EXPECT_EQ(BLADE_SUCCESS, client->SetWorkingDir("."));
    EXPECT_EQ(BLADE_SUCCESS, client->SetWorkingDir("~"));
    EXPECT_EQ(BLADE_SUCCESS, client->SetWorkingDir("~/test"));

    EXPECT_EQ(BLADE_SUCCESS, client->Delete("/testfile"));
    EXPECT_EQ(BLADE_SUCCESS, client->Delete("/test/test1/test2"));
    EXPECT_EQ(BLADE_SUCCESS, client->Delete("/test/test1/test3"));
    EXPECT_EQ(BLADE_SUCCESS, client->Delete("/test/test1"));
    EXPECT_EQ(BLADE_SUCCESS, client->Delete("/test"));
}

TEST_F(ClientTest, Create)
{
    ASSERT_TRUE(client != NULL);
    EXPECT_GT(client->Create("/testfile1", 0, 3), ROOTFID);
    EXPECT_EQ(client->Create("/testfile1", 0, 3), -RET_FILE_EXIST);
    EXPECT_EQ(BLADE_SUCCESS, client->MakeDirectory("/test"));
    EXPECT_EQ(client->Create("/test", 0, 3), -RET_FILE_EXIST);
    EXPECT_EQ(client->Create("/notexistdir/file", 0, 3), -RET_INVALID_DIR_NAME);
    EXPECT_GT(client->Create("/testfile2", 1, 3), ROOTFID);
    EXPECT_GT(client->Create("/testfile3", 0, 1), ROOTFID);
    EXPECT_EQ(client->Create("/testfile4", 0, 0), CLIENT_CREATE_INVALID_REPLICATION);
    EXPECT_EQ(client->Create("/testfile5", 0, -1), CLIENT_CREATE_INVALID_REPLICATION);
    EXPECT_EQ(client->Create("/testfile5", 0, 100), CLIENT_CREATE_INVALID_REPLICATION);
    EXPECT_EQ(client->Create("/testfile5", 0, 65535), CLIENT_CREATE_INVALID_REPLICATION);
    EXPECT_EQ(client->Create("/testfile5", 0, 65540), CLIENT_CREATE_INVALID_REPLICATION);
    EXPECT_EQ(BLADE_SUCCESS, client->SetWorkingDir("/test"));
    EXPECT_GT(client->Create("testfile6", 0, 3), ROOTFID);
    EXPECT_GT(client->Create("./testfile7", 0, 3), ROOTFID);
    EXPECT_GT(client->Create("../testfile8", 0, 3), ROOTFID);
    EXPECT_GT(client->Create("~/testfile9", 0, 3), ROOTFID);
    EXPECT_EQ(client->Create("testfilea testb", 0, 3), CLIENT_INVALID_FILENAME);

    EXPECT_EQ(BLADE_SUCCESS, client->SetWorkingDir("/"));
    EXPECT_EQ(BLADE_SUCCESS, client->Delete("/testfile1"));
    EXPECT_EQ(BLADE_SUCCESS, client->Delete("/testfile2"));
    EXPECT_EQ(BLADE_SUCCESS, client->Delete("/testfile3"));
    EXPECT_EQ(BLADE_SUCCESS, client->Delete("/test/testfile6"));
    EXPECT_EQ(BLADE_SUCCESS, client->Delete("/test/testfile7"));
    EXPECT_EQ(BLADE_SUCCESS, client->Delete("/testfile8"));
    EXPECT_EQ(BLADE_SUCCESS, client->Delete("/testfile9"));
    EXPECT_EQ(BLADE_SUCCESS, client->Delete("/test"));
}

TEST_F(ClientTest, Open)
{
    system("./writetest /writtenfile 10240000");
    ASSERT_TRUE(client != NULL);
    EXPECT_EQ(BLADE_SUCCESS, client->MakeDirectory("test"));
    EXPECT_GT(client->Open("/writtenfile", O_RDONLY), ROOTFID);
    EXPECT_EQ(client->Open("/writtenfile", O_CREAT), CLIENT_OPEN_NOT_RDONLY);
    EXPECT_EQ(client->Open("/nonexistfile", O_RDONLY), CLIENT_OPEN_FILE_ERROR);
    EXPECT_EQ(client->Open("/nonexistdir/file", O_RDONLY), CLIENT_OPEN_FILE_ERROR);
    EXPECT_GT(client->Open("./writtenfile", O_RDONLY), ROOTFID);
    EXPECT_EQ(BLADE_SUCCESS, client->SetWorkingDir("/test"));
    EXPECT_GT(client->Open("../writtenfile", O_RDONLY), ROOTFID);
    EXPECT_EQ(BLADE_SUCCESS, client->SetWorkingDir("/"));
    EXPECT_GT(client->Open("~/writtenfile", O_RDONLY), ROOTFID);
    EXPECT_EQ(client->Open("/writtenfile /file", O_RDONLY), CLIENT_INVALID_FILENAME);
    EXPECT_EQ(BLADE_SUCCESS, client->SetWorkingDir("/"));
    EXPECT_EQ(BLADE_SUCCESS, client->Delete("/test"));
    EXPECT_EQ(BLADE_SUCCESS, client->Delete("/writtenfile"));
}

TEST_F(ClientTest, GetFileInfo)
{
    ASSERT_TRUE(client != NULL);
    const FileInfo &file_info = client->GetFileInfo("/");
    EXPECT_EQ(2, file_info.get_file_id());
    EXPECT_EQ(2, file_info.get_file_type());
    EXPECT_EQ(0, file_info.get_file_size());
    EXPECT_GE(file_info.get_num_replicas(), 1);
    EXPECT_LE(file_info.get_num_replicas(), 10);
}

}
}
