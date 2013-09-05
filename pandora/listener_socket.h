/*
* Copyright (c) 2012£¬Sohu R&D
* All rights reserved.
* 
* File   name : listener_socket.h
* Description : 
* 
* Version : 1.0
* Author  : binghan@sohu-inc.com
* Date    : 2012-01-04
*/
#ifndef PANDORA_SYSTEM_NET_LISTENER_SOCKET_H
#define PANDORA_SYSTEM_NET_LISTENER_SOCKET_H

#include "socket.h"

namespace pandora
{
class ListenerSocket : public Socket
{
public:
    ListenerSocket(){}
    ~ListenerSocket(){}
    bool Listen(int backlog = SOMAXCONN);
    Socket* Accept();
};
}
#endif
