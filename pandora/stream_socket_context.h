/*
 *  Copyright (c) 2012, Sohu R&D
 *  All rights reserved.
 * 
 *  File   name: stream_socket_context.h 
 *  Description: This file defines the class StreamSocketContext
 * 
 *  Version: 1.0
 *  Author : jianyuli@sohu-inc.com
 *  Date   : 2012-1-12
 */

#ifndef PANDORA_AMFRAME_STREAM_SOCKET_CONTEXT_H
#define PANDORA_AMFRAME_STREAM_SOCKET_CONTEXT_H

#include "thread_cond.h"
#include "socket_context.h"

namespace pandora
{

class StreamSocketContext : public SocketContext
{
public:
    StreamSocketContext(
        AmFrame* amframe,
        Socket* socket,
        SocketId sock_id,
        StreamSocketHandler* handler,
        const AmFrame::EndPointOptions& options,
        bool is_server
    );
    ~StreamSocketContext();

    virtual bool HandleWriteEvent();

    virtual bool HandleReadEvent();

    virtual void CheckTimeout(int64_t now);

    virtual void Close();

    virtual int32_t SendPacket(Packet* packet, 
                               bool need_ack,
                               void* client_data,
                               int32_t timeout);

    void set_start_connect_time(int64_t start_connect_time)
    {
        start_connect_time_ = start_connect_time;
    }

    virtual StreamSocketHandler* event_handler() const // override
    {
        return static_cast<StreamSocketHandler*>(
                            SocketContext::event_handler()
                            );
    }

private:
    void Disconnect();

    bool CheckIoTimeout(int64_t now);

    // Handle the read event of this socket
    // @retval true: succeed
    // @retval false: error occured
    bool HandleInput();

    // Handle the write event of this socket
    // @retval true: succeed
    // @retval false: error occured
    bool HandleOutput();

    // Try to send packet until blocked or meet error
    // @param packet: packet to be sent
    // @retval >0: packet has been sent out successfully
    // @retval =0: blocked
    // @retval <0: error occured
    int SendPacket(Packet* packet);

    // Try to send data buffer until blocked or meet error
    // @retval >0: packet has been sent out successfully
    // @retval =0: blocked
    // @retval <0: error occured
    int SendBuffer(const void* buffer, size_t size);

    // Read the data arrived at this socket
    int Receive(void* buffer, size_t buffer_length);

    // Detect the packet boundery in the recevied data, then notify up-level
    // @return the number of packets detected
    // @retval <0: error occured
    int SplitAndIndicatePackets();

    int CheckAndIndicatePacket(char*& buffer, int& left_length);

    void AdjustReceiveBufferSize();
    void ResizeReceiveBuffer(size_t size);

    int GetSoError();
private:
    size_t  sent_length_; // Already sent length of the packet being sent
    bool    send_channel_id_;
    Packet* send_packet_;
    
    size_t  min_buffer_length_;
    char*   receive_buffer_;
    size_t  receive_buffer_length_;
    size_t  received_length_;
    int32_t detected_packet_length_;
    Packet  receive_packet_;

    PacketQueue output_queue_;
    //PacketQueue my_queue_;
    CThreadCond output_cond_;
    ChannelPool channel_pool_;
    int32_t queue_total_size_;
    int32_t queue_limit_;

    int64_t start_connect_time_; // The start time for connecting
};

} // namespace pandora

#endif // PANDORA_AMFRAME_STREAM_SOCKET_CONTEXT_H
