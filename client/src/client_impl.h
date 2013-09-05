/*
 * Copyright (c) 2012, Sohu R&D
 * All rights reserved.
 *
 * File   name: client_impl.h
 * Description: This file defines the class client.
 * 
 * Version : 1.0
 * Author: carloswjx, guili
 *
 */

#ifndef BLADESTORE_CLIENT_CLIENT_IMPL_H
#define BLADESTORE_CLIENT_CLIENT_IMPL_H

#include <stdint.h>
#include <sys/types.h>
#include <fcntl.h>
#include <string>
#include <map>
#include "log.h"
#include "client_net_handler.h"
#include "client_response.h"

namespace bladestore
{

namespace common
{
class FileInfo;
class LocatedBlock;
class LocatedBlockSet;
}

namespace client
{
using namespace std;
using namespace pandora;
using namespace bladestore::message;
using namespace bladestore::common;
using bladestore::common::FileInfo;

class LeaseChecker;
class LRUCache;
class ClientConf;
class ClientChecksum;
class CacheNode;
class CurrentBlock;

struct ChunkBuffer
{
    ChunkBuffer():start(0), buf(NULL), bufsz(0) {}
    ~ChunkBuffer() {if (buf) delete buf;}
    int64_t start;
    char * buf;
    size_t bufsz;
};

class ClientImpl
{
public:
    ClientImpl();
    ~ClientImpl();

    int32_t Init();

    // directory operations
    int32_t         MakeDirectory(const string& dirname);
    int32_t         IsValidPath(const string& dirname);
    vector<string>  GetListing(const string& dirname);

    // file operations
    int64_t Create(const string& filename, int8_t oflag, int16_t replication);
    int64_t Open(string filename, int8_t oflag);
    ssize_t Read(int64_t file_id, void *buf, size_t nbytes);
    ssize_t Write(int64_t file_id, void *buf, size_t nbytes);
    off_t   Lseek(int64_t file_id, off_t offset, int64_t whence);
    int32_t Close(int64_t file_id);

    // directory and file operations
    const FileInfo GetFileInfo(const string& filename);
    int32_t DeleteFile(const string& filename);

    // renew lease of block
    int32_t RenewLease(int64_t block_id);

private:
    int32_t ConnectToNameServer(const string& nameserver_addr);
    void    DisconnectToNameServer();
    int32_t ConnectToDataServer(uint64_t dataserver_id);
    void    DisconnectToDataServer();

    // block operations
    LocatedBlockSet GetBlockLocationsFromNS(const string& filename, int64_t offset, int64_t length);
    LocatedBlock AddBlock(int64_t file_id);
    int32_t AbandonBlock(int64_t block_id);
    int32_t Complete(int64_t file_id, int64_t block_id, int64_t block_size);

    LocatedBlockSet GetBlockLocations(CacheNode *file_cache, int64_t offset);

    int64_t ReadBlock(ResponseSync *response, int64_t offset, int64_t length);
    ssize_t WriteToServer(int64_t file_id, void *buf, size_t nbytes);
    int32_t WritePipeline();
    int64_t WritePacket(ResponseSync *response, int32_t unit_count, int64_t length, char *data);
    bool    PrepareWritePacket(int64_t file_id);
    bool    WaitForRWPackets(ResponseSync *response, int16_t operation, char *buf, ssize_t already);
    bool    CheckAllRWPackets(ResponseSync *response, int16_t operation);
    int     GetPathComponents(const string origin_pathname, int64_t *parentfid, string &name, bool flag);

    ClientStreamSocketHandler *conn_handler_;
    ClientStreamSocketHandler *ds_conn_handler_;

    LRUCache       * lru_cache_;
    LeaseChecker   * lease_checker_;
    ClientConf     * client_conf_;  // read configurations of client
    CurrentBlock   * current_block_;
    ChunkBuffer    * chunk_buffer_;

    DISALLOW_COPY_AND_ASSIGN(ClientImpl);
};

}  // end of namespace client
}  // end of namespace bladestore

#endif  // BLADESTORE_CLIENT_CLIENT_IMPL_H
