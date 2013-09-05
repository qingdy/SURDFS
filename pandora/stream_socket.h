/*
* Copyright (c) 2012£¬Sohu R&D
* All rights reserved.
* 
* File   name : stream_socket.h
* Description : 
* 
* Version : 1.0
* Author  : binghan@sohu-inc.com
* Date    : 2012-01-04
*/

#ifndef PANDORA_SYSTEM_NET_STREAM_SOCKET_H
#define PANDORA_SYSTEM_NET_STREAM_SOCKET_H
#include "socket.h"

namespace pandora
{
class StreamSocket : public Socket
{
public:    
    StreamSocket(){}
    ~StreamSocket(){}
    bool Connect();
};
}
#endif
