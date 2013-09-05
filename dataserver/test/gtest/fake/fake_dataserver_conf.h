/* Sohu R&D 2012
 * 
 * File   name: dataserver_conf.h
 * Description: This file defines config details;
 * 
 * Version : 1.0
 * Author  : @YYY
 * Date    : 2012-07-03
 */

#ifndef BLADESTORE_FAKE_DATASERVER_CONFIG_H
#define BLADESTORE_FAKE_DATASERVER_CONFIG_H 

#include <string>
#include "blade_net_util.h"
#include "blade_common_define.h"
#include "dataserver_conf.h"

namespace bladestore
{
namespace dataserver
{
using std::string;
using namespace bladestore::common;

class FakeDataServerConfig: public DataServerConfig
{
public:
    static DataServerConfig * Instance();
    FakeDataServerConfig();
    ~FakeDataServerConfig();
    //get items 

    DISALLOW_COPY_AND_ASSIGN(FakeDataServerConfig);
};

DataServerConfig * FakeDataServerConfig::Instance()
{
    if (config_ == NULL) {
        config_ = new DataServerConfig();
        assert(config_);
        config_->GetConf("/root/yyy/BladeStore/trunk/dataserver/test/conf/dataserver.conf");
    }
    return config_;
}
FakeDataServerConfig::FakeDataServerConfig(){}
FakeDataServerConfig::~FakeDataServerConfig(){}

}//end of namespace dataserver
}//end of namespace bladestore

#endif

