/*
 * Copyright (c) 2012, Sohu R&D
 * All rights reserved.
 * 
 * File   name: client.cpp
 * Description: the class client.
 */

#include "client.h"
#include "client_impl.h"
#include "bladestore_ops.h"
#include "utility.h"
#include "string_util.h"

namespace bladestore
{
namespace client
{

Client::Client()
{
    client_impl_ = NULL;
}

Client::~Client()
{
    if (client_impl_) {
        delete client_impl_;
        client_impl_ = NULL;
    }
}

int Client::Init()
{
    client_impl_ = new ClientImpl();
    if (NULL == client_impl_)
        return BLADE_ERROR;

    int ret = client_impl_->Init();
    if (BLADE_ERROR == ret)
        return BLADE_ERROR;

    current_path_ = "/";
    home_path_ = "/";

    return BLADE_SUCCESS;
}

// direcotry opetations:
int Client::MakeDirectory(const string& dirname)
{
    string ab_dirname = MakeAbsolutePath(dirname);
    if (ab_dirname.empty()) 
        return BLADE_ERROR;

    int32_t ret = client_impl_->MakeDirectory(ab_dirname);
    if (RET_SUCCESS == ret) {
        return BLADE_SUCCESS;
    } else {
        return BLADE_ERROR;
    }
}

int Client::SetWorkingDir(const string& dirname)
{
    string ab_dirname = MakeAbsolutePath(dirname);
    if (ab_dirname.empty())
        return BLADE_ERROR;

    int32_t ret = client_impl_->IsValidPath(ab_dirname);
    if (RET_SUCCESS == ret) {
        if (ab_dirname.compare("/") && ab_dirname[ab_dirname.length() - 1] == '/')
            current_path_ = ab_dirname.erase(ab_dirname.length() - 1);
        current_path_ = ab_dirname;
        return BLADE_SUCCESS;
    } else {
        return BLADE_ERROR;
    }
}

vector<string> Client::GetListing(const string& dirname)
{
    string ab_dirname = MakeAbsolutePath(dirname);
    if (ab_dirname.empty())
        return vector<string>();
    else
        return client_impl_->GetListing(ab_dirname);
}

// file operations:
int Client::Create(const string& filename, bool oflag, int replication)
{
    int createmode = 0;
    if (oflag)
        createmode = 1;

    if (replication < 1 || replication > BLADE_MAX_REPLICAS_PER_FILE)
        return CLIENT_CREATE_INVALID_REPLICATION;

    string ab_filename = MakeAbsolutePath(filename);
    if (ab_filename.empty())
        return CLIENT_INVALID_FILENAME;
    else
        return client_impl_->Create(ab_filename, createmode, replication);
}

int Client::Open(const string& filename, int oflag)
{
    if(oflag != O_RDONLY)
        return CLIENT_OPEN_NOT_RDONLY;

    string ab_filename = MakeAbsolutePath(filename);
    if (ab_filename.empty())
        return CLIENT_INVALID_FILENAME;
    else
        return client_impl_->Open(ab_filename, oflag);
}

ssize_t Client::Read(int file_id, void *buf, size_t nbytes)
{
    if (file_id < ROOTFID || nbytes > CLIENT_MAX_BUFFERSIZE || buf == NULL)
        return CLIENT_INVALID_FILEID;

    return client_impl_->Read(file_id, buf, nbytes);
}

ssize_t Client::Write(int file_id, void *buf, size_t nbytes)
{
    if (file_id < ROOTFID || nbytes > CLIENT_MAX_BUFFERSIZE || buf == NULL)
        return CLIENT_INVALID_FILEID;
    if (nbytes == 0)
        return 0;

    return client_impl_->Write(file_id, buf, nbytes);
}

int Client::Close(int file_id)
{
    if (file_id < ROOTFID)
        return CLIENT_INVALID_FILEID;

    return client_impl_->Close(file_id);
}

off_t Client::Lseek(int file_id, off_t offset, int whence)
{
    if (file_id < ROOTFID)
        return CLIENT_INVALID_FILEID;

    return client_impl_->Lseek(file_id, offset, whence);
}

// direcotry and file operations:
int Client::Delete(const string& filename)
{
    string ab_filename = MakeAbsolutePath(filename);
    if (ab_filename.empty())
        return CLIENT_INVALID_FILENAME;

    int32_t ret = client_impl_->DeleteFile(ab_filename);
    if (RET_SUCCESS == ret) {
        return BLADE_SUCCESS;
    } else {
        return BLADE_ERROR;
    }
}

FileInfo Client::GetFileInfo(const string& filename)
{
    string ab_filename = MakeAbsolutePath(filename);
    if (ab_filename.empty())
        return FileInfo();
    else
        return client_impl_->GetFileInfo(ab_filename);
}

string Client::MakeAbsolutePath(const string& name)
{
    string abpath;
    string tail(name);
    StringTrim(tail);

    // don't allow spaces after trim
    if (tail.find(' ') != string::npos || tail.empty())
        return "";

    const char *input = tail.c_str();
    if (input[0] == '/') {
        abpath = strip_dots(tail);
    } else {
        const char *c = current_path_.c_str();
        bool is_root = (c[0] == '/' && c[1] == '\0');
        string head(c);
        if (!is_root)
            head.append("/");
        abpath = strip_dots(head + tail);
    }

    string::size_type pathlen = abpath.size();
    // don't allow "//" in path
    if (pathlen == 0 || abpath[0] != '/' || string::npos != abpath.find("//"))
        return "";
    else {
        if (CheckPathLength(abpath))
            return abpath;
        else
            return "";
    }
}

string Client::strip_dots(string path)
{
    vector <string> component;
    string result;
    string::size_type start = 0;

    while (start != string::npos) {
        assert(path[start] == '/');
        string::size_type slash = path.find('/', start + 1);
        string nextc(path, start, slash - start);
        start = slash;
        if (nextc.compare("/..") == 0) {
            if (!component.empty())
                component.pop_back();
        } else if (nextc.compare("/.") != 0)
            component.push_back(nextc);
    }

    if (component.empty())
        component.push_back(string("/"));

    for (vector <string>::iterator c = component.begin();
            c != component.end(); ++c) {
        result += *c;
    }
    return result;
}

}//end of namespace client
}//end of namespace bladestore
