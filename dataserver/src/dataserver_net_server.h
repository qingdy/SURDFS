/* Sohu R&D 2012
 * 
 * File   name: dataserver_net_server.h
 * Description: This file defines  net IO server.
 * 
 * Version : 1.0
 * Author  : @YYY
 * Date    : 2012-06-03
 */

#ifndef BLADESTORE_DATASERVER_NET_SERVER_H
#define BLADESTORE_DATASERVER_NET_SERVER_H

#include "stdbool.h"
#include "blade_common_define.h"

#ifdef GTEST 
#define private public
#define protected public
#endif
namespace bladestore
{
namespace dataserver
{
using pandora::AmFrame;
class DataServerStreamSocketHandler;
class DataServerListenSocketHandler;
class DataServerImpl;
class DataServerConfig;

class DataNetServer
{
public:
    explicit DataNetServer(DataServerImpl *impl);
    ~DataNetServer();

    bool Start();

    DataServerListenSocketHandler   * listen_handler(){ return listen_handler_;}
    DataServerStreamSocketHandler   * stream_handler(){ return stream_handler_;}
    //connect to DS
    DataServerStreamSocketHandler   * ConnectToDS(uint64_t ds_id);
    bool ConnectToNS();

private:
    DataNetServer(); //disallow default constructor
    bool Init();
    bool Listen();
    //connect to nameserver

    DataServerImpl                  * ds_impl_;
    DataServerConfig                * config_;
    AmFrame::EndPointOptions          end_point_options_;
    DataServerListenSocketHandler   * listen_handler_;
    DataServerStreamSocketHandler   * stream_handler_;

    DISALLOW_COPY_AND_ASSIGN(DataNetServer);
};

}//end of namespace dataserver
}//end of namespace bladestore

#endif
