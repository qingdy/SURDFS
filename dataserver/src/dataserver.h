/* Sohu R&D 2012
 * 
 * File   name: dataserver.h
 * Description: This file defines  how dataserver runs.
 * 
 * Version : 1.0
 * Author  : @landyliu
 * Date    : 2012-06-01
 */

#ifndef BLADESTORE_DATASERVER_H
#define BLADESTORE_DATASERVER_H

#include <stdint.h>
#include <stdbool.h>
#include "blade_common_define.h"

namespace pandora 
{
class Log;
}

namespace bladestore
{
namespace dataserver
{
using pandora::Log;
class DataServerConfig;
class DataServerImpl;

class DataServer 
{
public:
    DataServer();
    ~DataServer();

    bool Init();
    bool Start();
    bool Stop();

private:
    DataServerConfig    * config_;
    DataServerImpl      * ds_impl_;
    DISALLOW_COPY_AND_ASSIGN(DataServer);
};

}//end of namespace dataserver
}//end of namespace bladestore

#endif
