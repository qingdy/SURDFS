/*
 * Copyright (c) 2012, Sohu R&D
 * All rights reserved.
 *
 * File   name: client.h
 * Description: This file defines the class client.
 * 
 * Version : 1.0
 * Author: carloswjx, guili
 *
 */

#ifndef BLADESTORE_CLIENT_CLIENT_H
#define BLADESTORE_CLIENT_CLIENT_H

#include <sys/types.h>
#include <fcntl.h>
#include <string>
#include <vector>
#include "blade_common_define.h"
#include "blade_file_info.h"

namespace bladestore
{
namespace client
{

using std::string;
using std::vector;
using namespace bladestore::common;
class ClientImpl;
class ClientConf;

class Client
{
public:
    Client();
    ~Client();

    int Init();

    /* ***************************************************************
     * direcotry opetations
     * ***************************************************************
     */
    
    /*
     * Makedirectory: create a directory
     * @param[in] dirname
     * @retval: 0 on success; not 0 on failure
     */
    int MakeDirectory(const string& dirname);
    /*
     * SetWorkingDir: 
     * @param[in] dirname
     * @retval: 0 on success; not 0 on failure
     */
    int SetWorkingDir(const string& dirname);
    /*
     * GetListing: get all the files and subdirs of a directory
     * @param[in] dirname
     * @retval: not empty on success; empty on failure or empty directory
     */
    vector<string> GetListing(const string& dirname);

    /* ***************************************************************
     * file opetations
     * ***************************************************************
     */

    /*
     * Create: create a file to write and get the file id
     * @param[in] filename
     * @param[in] oflag: how the file is created, TRUE on "safe write" or FALSE on "nomal", default FALSE 
     * @param[in] replication: specify the replication of the file, default 3
     * @retval: non-negative fid on success; negative on failue
     */
    int Create(const string& filename, bool oflag = false, int replication = BLADE_MIN_REPLICAS_PER_FILE);
    /*
     * Open: open a file to read
     * @param[in] filename
     * @param[in] oflag: the mode when opening, limited to O_RDONLY at present
     * @retval: non-negative fid on success; negative on failure
     */
    int Open(const string& filename, int oflag = O_RDONLY);
    /*
     * Read:
     * @param[in]  file_id
     * @param[out] buf: the buffer to store the data that read from a file
     * @param[in]  nbytes: buf size
     * @retval: expected number bytes on success, others on error or end of file
     */
    ssize_t Read(int file_id, void *buf, size_t nbytes);
    /*
     * Write
     * @param[in] file_id
     * @param[in] buf: the data to be written
     * @param[in] nbytes: the size of the data being writing
     * @retval: expected number bytes on success, others on error
     */
    ssize_t Write(int file_id, void *buf, size_t nbytes);
    /*
     * Close: close a file being reading or writing, close the treads
     * @param[in] file_id
     * @retval: 0 on success; negative on failure
     */
    int Close(int file_id);
    /*
     * Lseek: set the current file offset in an opened file
     * @param[in] file_id
     * @param[in] offset: the offset to the start point set by whence
     * @param[in] whence: set the start point, "SEEK_SET", "SEEK_CUR", or "SEEK_END"
     * @retval: non-negative offset on sucess; -1 on failure
     */
    off_t Lseek(int file_id, off_t offset, int whence);

    /* ***************************************************************
     * direcotry and file operations:
     * ***************************************************************
     */

    /*
     * Delete: delete a file or a directory
     * @param[in] filename
     * @retval: 0 on success; not 0 on failure
     */
    int Delete(const string& filename);
    /*
     * GetFileInfo: get the detailed information of a file
     * @param[in] filename
     * @retval: file_id in FileInfo positive on success; others on failure
     * FileInfo:
     *      int64_t        get_file_id()
     *      int8_t         get_file_type()
     *      struct timeval get_mtime()
     *      struct timeval get_ctime()
     *      struct timeval get_crtime()
     *      int64_t        get_block_count()
     *      int16_t        get_num_replicas()
     *      off_t          get_file_size()
     */
    FileInfo GetFileInfo(const string& filename);

private:
    string MakeAbsolutePath(const string& tail);
    string strip_dots(string path);

    string current_path_;
    string home_path_;
    ClientImpl *client_impl_;

    DISALLOW_COPY_AND_ASSIGN(Client);
};

}//end of namespace client
}//end of namespace bladestore
#endif
