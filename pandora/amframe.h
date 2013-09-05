/*
 *  Copyright (c) 2012, Sohu R&D
 *  All rights reserved.
 * 
 *  File   name: amframe.h 
 *  Description: This file defines the class AmFrame
 * 
 *  Version: 1.0
 *  Author : jianyuli@sohu-inc.com
 *  Date   : 2012-1-12
 */

#ifndef PANDORA_AMFRAME_AMFRAME_H
#define PANDORA_AMFRAME_AMFRAME_H

#include <stdint.h>
#include <string>
#include <vector>
#include "atomic.h"
#include "packet.h"

namespace pandora
{

class Transport;
class SocketContext;
class ListenSocketContext;

class SocketHandler;
class ListenSocketHandler;
class StreamSocketHandler;

union SocketId
{
    explicit SocketId(int64_t i = -1)
    {
        id_ = i;
    }
    struct
    {
        int32_t sock_fd_;
        int32_t sequence_id_;
    };
    bool operator == (const SocketId& sock_id) const
    {
        return id_ == sock_id.id_;
    }
    int GetFd() const
    {
        return sock_fd_;
    }
    int64_t GetId() const
    {
        return id_;
    }
    int64_t id_;
};

class AmFrame
{
    friend class SocketContext;
    friend class Transport;
    friend class ListenSocketContext;
    friend class StreamSocketContext;

public:
    static const size_t kDefault_Max_Output_Queue_Length = 1000;
    static const size_t kDefault_Max_Packet_Length = 2 * 1024 * 1024L;
    static const uint64_t  kDefault_Max_Connection_Num = 0x10000;

    class EndPoint
    {
    public:
        friend class AmFrame;
        friend class SocketHandler;
        EndPoint() : sock_id_(-1)
        {
        }
        SocketId sock_id() const
        {
            return sock_id_;
        }
        int64_t GetId() const
        {
            return sock_id_.id_;
        }
        int GetFd() const
        {
            return sock_id_.sock_fd_;
        }
        bool IsValid() const
        {
            return GetFd() >= 0;
        }
        bool operator == (const EndPoint& rhs)
        {
            return sock_id_ == rhs.sock_id_;
        }
        bool operator != (const EndPoint& rhs)
        {
            return !(sock_id_ == rhs.sock_id_);
        }

        explicit EndPoint(SocketId id): sock_id_(id) {}
        explicit EndPoint(int64_t id)
        {
            sock_id_.id_ = id;
        }

    private:
        void set_sock_id(SocketId id)
        {
            sock_id_ = id;
        }
        SocketId sock_id_;
    };

    class ListenEndPoint : public EndPoint
    {
        friend class ListenSocketHandler;
    public:
        ListenEndPoint() {}
    private:
        ListenEndPoint(SocketId id) : EndPoint(id) {}
    };

    class StreamEndPoint : public EndPoint
    {
        friend class StreamSocketHandler;
    public:
        StreamEndPoint() {}
    private:
        StreamEndPoint(SocketId id) : EndPoint(id) {}
    };

    class EndPointOptions
    {
    public:
        EndPointOptions() :
            max_output_queue_length_(kDefault_Max_Output_Queue_Length),
            max_packet_length_(kDefault_Max_Packet_Length),
            send_buffer_size_(640 * 1024),
            receive_buffer_size_(640 * 1024)
        {
        }

        size_t max_output_queue_length() const
        {
            return max_output_queue_length_;
        }

        EndPointOptions& set_max_output_queue_length(size_t value)
        {
            max_output_queue_length_ = value;
            return *this;
        }
        
        size_t max_packet_length() const
        {
            return max_packet_length_;
        }

        EndPointOptions& set_max_packet_length(size_t value)
        {
            max_packet_length_ = value;
            return *this;
        }

        size_t send_buffer_size() const
        {
            return send_buffer_size_;
        }

        EndPointOptions& set_send_buffer_size(size_t value)
        {
            send_buffer_size_ = value;
            return *this;
        }

        size_t receive_buffer_size() const
        {
            return receive_buffer_size_;
        }

        EndPointOptions& set_receive_buffer_size(size_t value)
        {
            receive_buffer_size_ = value;
            return *this;
        }
        
    private:
        size_t max_output_queue_length_;
        size_t max_packet_length_;
        size_t send_buffer_size_;
        size_t receive_buffer_size_;
    };

    // @param max_bufferd_size: the maximum size of packets to be sent
    AmFrame(uint64_t max_connection_num = kDefault_Max_Connection_Num);
    ~AmFrame();

    // @retval >0 socket id
    // @retval <0 -errno
    int64_t AsyncListen(
        const char* address,
        ListenSocketHandler* handler,
        const EndPointOptions& options = EndPointOptions()
    );

    // @retval >0 socket id
    // @retval <0 -errno
    int64_t AsyncConnect(
        const char* remote_address,
        StreamSocketHandler* handler,
        const EndPointOptions& options = EndPointOptions()
    );

    // @retval 0 succeed, otherwise failed
    int CloseEndPoint(EndPoint& endpoint);

    // @brief Send a packet through TCP stream conncetion
    // @param endpoint: The endpoint of source socket
    // @param packet: The packet to be sent, its memory is freed by AmFrame 
    // @retval 0 succeed, otherwise failed
    int SendPacket(const StreamEndPoint& endpoint,
                   Packet* packet,
                   bool need_ack = false,
                   void *client_data = NULL,
                   int32_t timeout = 1000);

    // @brief Send a packet through TCP stream conncetion
    // @param endpoint: The endpoint of source socket
    // @param data: The data buffer to be sent, AmFrame will create a packet which
    //              contains its copy, then send that packet
    // @param size: Data buffer size
    // @retval 0 succeed, otherwise failed
    int SendPacket(const StreamEndPoint& endpoint,
                   const void* data, size_t size,
                   int32_t channel_id = 0,
                   bool need_ack = false,
                   void *client_data = NULL,
                   int32_t timeout = 1000);

private:
    int ParseAddr(char *src, char **args, int cnt);
    static SocketId GenerateSocketId(int32_t sock_fd);

private:
    static uint32_t sequence_id_;
    atomic64_t current_connection_num_;
    uint64_t max_connection_num_;
    Transport* transport_;
};

} // namespace pandora

#endif // PANDORA_AMFRAME_AMFRAME_H
