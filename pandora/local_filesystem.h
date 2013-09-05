/*
* Copyright (c) 2011£¬Sohu R&D
* All rights reserved.
* 
* File   name£ºlocal_filesystem.h
* Description: This file defines the class ¡­
* 
* Version £º1.0
* Author  £ºboliang@sohu-inc.com
* Date    £º2012-2-7
*
*/

#ifndef PANDORA_LOCAL_FILESYSTEM_H
#define PANDORA_LOCAL_FILESYSTEM_H

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>


#include "filesystem.h"

namespace pandora
{

#define MAX_PATH_LEN 1024 * 10

class LocalFileSystem : public FileSystem
{
public:
    LocalFileSystem(){}
    virtual ~LocalFileSystem(){}
    
    std::string GetFileSystemName()
    {
        return fsName_;
    }
    
    bool CheckPath(const std::string &uri)
    {
        (void)uri;

        return true;
    }
    
    std::string GetDefaultUri()
    {
        return "";
    }
    
    int32_t SetDefaultUri(const std::string &uri)
    {
        (void)uri;
        return -1;
    }

    int32_t GetDefaultReplication()
    {
        return 1;
    }

    int32_t SetDefaultReplication(const int32_t rep)
    {
        (void)rep;
        return 1;
    }
    
    //defualt dir
    std::string GetHomeDirectory();
    std::string GetWorkingDirectory();
    int32_t SetWorkingDirectory(const std::string &dir) ;

    //file status
    FileStatus* GetFileStatus(const std::string &path);

    //file op
    int32_t MoveFromLocalFile(const std::string &src, const std::string &dst);
    int32_t MoveToLocalFile(const std::string &src, const std::string &dst);

    int32_t Rename(const std::string &src, const std::string &dst);

    //file
    int32_t Exists(const std::string &path);
    
    FileDescriptor* Create(const std::string &path, const bool isOverWritten = true);
    
    FileDescriptor* Append(const std::string &path, const  bool needCreate = true);
    
    FileDescriptor* Open(const std::string &path); //open to read
    
    int32_t Delete(const std::string &path);

    //dir
    int32_t MakeDirectory(const std::string &path, int32_t mode = 0755);

    int32_t Close()
    {
        return 0;
    }
    
    static const char* fsName_;
    static const FileSystemType fsType_ = FS_LOCAL;
};

class LocalFileStatus :public FileStatus
{
public:
    LocalFileStatus(bool isDir, const std::string &path, time_t accessTime, time_t modificationTime, 
        const std::string &group, const std::string &owner);
    
    virtual ~LocalFileStatus(){}

    bool IsDir()
    {
        return isDir_;
    }

    std::string GetPath()
    {
        return path_;
    }

    time_t GetAccessTime()
    {
        return accessTime_;
    }

    time_t GetModificationTime()
    {
        return modificationTime_;
    }

    std::string GetGroup()
    {
        return group_;
    }

    std::string GetOwner()
    {
        return owner_;
    }

    int64_t GetLen();
    
    int32_t SetGroup(const std::string &group);
    
    int32_t SetOwner(const std::string &owner);

private:
    
    bool isDir_;
    std::string path_;

    time_t accessTime_;
    time_t modificationTime_;

    std::string group_;
    std::string owner_;
};

class LocalFileDescriptor : public FileDescriptor
{
public:
    //rwflag == true,this is a write filedescriptor
    LocalFileDescriptor(FILE *file,bool rwflag):file_(file),isWrite_(rwflag){}

    virtual ~LocalFileDescriptor(){}

    int64_t GetPos();
    int32_t Close();

    //read
    int64_t Read(void *buffer, const int64_t length);
    int32_t Seek(const int64_t desired);

    //write
     int64_t Write(const void *buffer, const int64_t length);

private:
    FILE *file_;
    bool isWrite_;
};

}

#endif

