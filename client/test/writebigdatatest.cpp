#include <fcntl.h>
#include "client.h"
#include "bladestore_ops.h"
#include "blade_common_define.h"
#include "int_to_string.h"
#include "gtest/gtest.h"
#include "log.h"
using bladestore::common::BLADE_ERROR;
namespace bladestore
{

namespace client
{
class TestNS : public ::testing::Test
{
protected:
    virtual void SetUp()
    {
        client_ = new Client();
        int ret = client_->Init();
        if (BLADE_ERROR == ret)
            abort();
    }
    virtual void TearDown()
    {
        delete client_;
        client_ = NULL;
    }
    Client *client_;
};

TEST_F(TestNS, BigDataTest)
{
    int64_t readlocallen, writenlen, writentotal;
    int64_t buffersize = 10<<20;
    char *buf = (char *)malloc(buffersize);
    client_->MakeDirectory("/testdir4");
    for (int32_t k = 0; k<7; ++k) {
        writentotal = 0;
        string ks = string("/testdir4/file").append(Int32ToString(k));
        printf("filepath: %s.\n", ks.c_str());
        int64_t fid = client_->Create(ks, 0, 3);
        ASSERT_GT(fid, 0);
        int localfid = open("/root/BladeStore/trunk/client/test/sample", O_RDONLY);
        do{
            readlocallen = read(localfid, buf, buffersize);
            if(readlocallen > 0) {
                LOGV(LL_INFO, "readlocallen: %ld.", readlocallen);
                writenlen = client_->Write(fid, buf, readlocallen);
                ASSERT_EQ(readlocallen, writenlen);
            }
            writentotal += writenlen;
        } while (readlocallen > 0);
        close(localfid);
        client_->Close(fid);
        LOGV(LL_INFO, "WritenLen: %ld.", writentotal);
    }

    free(buf);
}

}
}
