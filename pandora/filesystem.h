/*
* Copyright (c) 2011£¬Sohu R&D
* All rights reserved.
* 
* File   name£ºfilesystem.h
* Description: This file defines the class ¡­
* 
* Version £º1.0
* Author  £ºboliang@sohu-inc.com
* Date    £º2012-2-6
*
*/

#ifndef PANDORA_FILESYSTEM_H
#define PANDORA_FILESYSTEM_H

#include <stdint.h>
#include <string>

namespace pandora
{

class LocalFileSystem;
class FileSystemConf;
class FileStatus;
class FileDescriptor;

typedef enum FileSystemType
{
    FS_LOCAL = 0x00,
    FS_HDFS = 0x01,
    FS_BLADEFS = 0x02,
    FS_FTP = 0x03,
    FS_UNKOWN = 0x04,
};

class FileSystem
{
public:
    //
    virtual ~FileSystem() {};
    
    //filesystem
    static FileSystem* Get(const std::string &uri, FileSystemConf *conf);
    static LocalFileSystem* GetLocalSystem();
    
    //FileSystem* Get(const std::string &uri, FileSystemConf *conf);
    
    virtual std::string GetFileSystemName()
    {
        return "Unkown";
    }
    
    virtual bool CheckPath(const std::string &uri)
    {
        (void)uri;
        return "";
    }
    
    virtual std::string GetDefaultUri()
    {
        return "";
    }
    
    virtual int32_t SetDefaultUri(const std::string &uri)
    {
        (void)uri;
        return -1;
    }
    
    virtual int32_t GetDefaultReplication()
    {
        return 0;
    }
    
    virtual int32_t SetDefaultReplication(const int32_t rep)
    {
        (void)rep;
        return 0;
    }
    
    //defualt dir
    virtual std::string GetHomeDirectory()
    {
        return "";
    }
    
    virtual std::string GetWorkingDirectory()
    {
        return "";
    }
    
    virtual int32_t SetWorkingDirectory(const std::string &dir)
    {
        (void)dir;
        
        return -1;
    }
    
    //file status
    virtual FileStatus* GetFileStatus(const std::string &path)
    {
        (void)path;
        
        return NULL;
    }

    //file op
    virtual int32_t MoveFromLocalFile(const std::string &src, const std::string &dst)
    {
        (void)src;
        (void)dst;
        
        return -1;
    }
    
    virtual int32_t MoveToLocalFile(const std::string &src, const std::string &dst)
    {
        (void)src;
        (void)dst;
        
        return -1;
    }

    virtual int32_t Rename(const std::string &src, const std::string &dst)
    {
        (void)src;
        (void)dst;
        
        return -1;
    }

    //file
    virtual int32_t Exists(const std::string &path)
    {
        (void)path;
        
        return -1;
    }
    
    virtual FileDescriptor* Create(const std::string &path, const bool isOverWritten = true)
    {
        (void)path;
        (void)isOverWritten;
        
        return NULL;

    }
    
    virtual FileDescriptor* Append(const std::string &path, const  bool needCreate = true)
    {
        (void)path;
        (void)needCreate;
        
        return NULL;
    }
    
    virtual FileDescriptor* Open(const std::string &path) //open to read
    {
        (void)path;
        
        return NULL;
    }
    
    virtual int32_t Delete(const std::string &path)
    {
        (void)path;
        
        return -1;
    }

    //dir
    virtual int32_t MakeDirectory(const std::string &path, int32_t mode = 0755)
    {
        (void)path;
        (void)mode;

        return -1;
    }

    //close
    virtual int32_t Close()
    {
        return -1;
    }

protected:
    FileSystem() {}
    
};

class FileStatus
{
public:
    FileStatus() {}
    virtual ~FileStatus() {}
    
    //
    virtual bool IsDir()
    {
        return false;
    }
    
    virtual int64_t GetLen()
    {
        return -1;
    }
    
    virtual std::string GetPath()
    {
        return "";
    }

    //time
    virtual time_t GetAccessTime()
    {
        return 0;
    }
    
    virtual time_t GetModificationTime()
    {
        return 0;
    }

    //
    virtual std::string GetGroup()
    {
        return "";
    }
    
    virtual int32_t SetGroup(const std::string &group)
    {
        (void)group;
        
        return -1;
    }
    
    virtual std::string GetOwner()
    {
        return "";
    }
    
    virtual int32_t SetOwner(const std::string &owner)
    {
        (void)owner;

        return -1;
    }
};

class FileDescriptor
{
public:
    FileDescriptor(){}
    virtual ~FileDescriptor() {}
    
    //
    virtual int64_t GetPos()
    {
        return -1;
    }
    
    virtual int32_t Close()
    {
        return -1;
    }

    //read
    //virtual int64_t Read(const long position, const void *buffer, const int offset, const int length) = 0;
    virtual int64_t Read(void *buffer, const int64_t length)
    {
        (void)buffer;
        (void)length;
        
        return -1;
    }
    
    virtual int32_t Seek(const int64_t desired)
    {
        (void)desired;
        
        return -1;
    }

    //write
    virtual int64_t Write(const void *buffer, const int64_t length)
    {
        (void)buffer;
        (void)length;
         
        return -1;
    }
};

class FileSystemConf
{
    
};


}

#endif
