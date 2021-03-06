/*
 *version  : 1.0
 *author   : chen yunyun
 *date     : 2012-5-1
 *fun      : used by HA
 */
#ifndef  BLADESTORE_COMMON_FILE_H 
#define  BLADESTORE_COMMON_FILE_H 
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <string>

#include "blade_fileinfo_manager.h"
#include "file_directory_utils.h"

using namespace std;

namespace bladestore
{
namespace common
{

class IFileBuffer
{
public:
	IFileBuffer() 
	{

	}

	virtual ~IFileBuffer() 
	{

	}

public:
	virtual char *get_buffer() = 0;
    virtual int64_t get_base_pos() = 0;
    virtual void set_base_pos(const int64_t pos) = 0;
    virtual int assign(const int64_t size, const int64_t align) = 0;
    virtual int assign(const int64_t size) = 0;
};

class IFileAsyncCallback
{
public:
	IFileAsyncCallback() 
	{

	}

	virtual ~IFileAsyncCallback() 
	{

	} 

public:
	virtual void callback(const int io_ret, const int io_errno) = 0;
}; 

class IFileReader
{
public:
	IFileReader() : fd_(-1) 
	{

	}

	virtual ~IFileReader() 
	{

	}

public:
	virtual int open(const string &fname);
	virtual void close();
	virtual bool is_opened() const;

public:
	virtual int pread(void *buf, const int64_t count, const int64_t offset, int64_t &read_size) = 0;
	virtual int pread(const int64_t count, const int64_t offset, IFileBuffer &file_buf, int64_t &read_size) = 0;

public:
	virtual int get_open_flags() const = 0;
	virtual int get_open_mode() const = 0;

public:
	virtual int pread_by_fd(const int fd, void *buf, const int64_t count, const int64_t offset, int64_t &read_size) = 0;
	virtual int pread_by_fd(const int fd, const int64_t count, const int64_t offset, IFileBuffer &file_buf, int64_t &read_size) = 0;

protected:
	int fd_;
};

class IFileAppender
{
public:
	IFileAppender() : fd_(-1) 
	{

	}

	virtual ~IFileAppender() 
	{

	}

public:
	virtual int open(const string &fname, const bool is_create, const bool is_trunc);
	virtual int create(const string &fname);
	virtual void close();
	virtual bool is_opened() const;

public:
	virtual int append(const void *buf, const int64_t count, const bool is_fsync) = 0;
	virtual int async_append(const void *buf, const int64_t count, IFileAsyncCallback *callback) = 0;
	virtual int fsync() = 0;

public:
	virtual int64_t get_file_pos() const = 0;
	virtual int get_open_flags() const = 0;
	virtual int get_open_mode() const = 0;

private:
	virtual int prepare_buffer_() = 0;
	virtual void set_normal_flags_() = 0;
	virtual void add_truncate_flags_() = 0;
	virtual void add_create_flags_() = 0;
	virtual void add_excl_flags_() = 0;
	virtual void set_file_pos_(const int64_t file_pos) = 0;

protected:
	int fd_;
};

class BufferFileReader : public IFileReader
{
	static const int OPEN_FLAGS = O_RDONLY;
	static const int OPEN_MODE = S_IRWXU;
public:
	BufferFileReader();
	~BufferFileReader();

public:
	int pread(void *buf, const int64_t count, const int64_t offset, int64_t &read_size);
	int pread(const int64_t count, const int64_t offset, IFileBuffer &file_buf, int64_t &read_size);
	int get_open_flags() const;
	int get_open_mode() const;

public:
	int pread_by_fd(const int fd, void *buf, const int64_t count, const int64_t offset, int64_t &read_size);
	int pread_by_fd(const int fd, const int64_t count, const int64_t offset, IFileBuffer &file_buf, int64_t &read_size);
};

class DirectFileReader : public IFileReader
{   
	static const int OPEN_FLAGS = O_RDONLY | O_DIRECT;
	static const int OPEN_MODE = S_IRWXU;
public:
	static const int64_t DEFAULT_ALIGN_SIZE = 4 * 1024;
	static const int64_t DEFAULT_BUFFER_SIZE = 1 * 1024 * 1024;

public:
	DirectFileReader(const int64_t buffer_size = DEFAULT_BUFFER_SIZE, const int64_t align_size = DEFAULT_ALIGN_SIZE);
	~DirectFileReader();

public:
	int pread(void *buf, const int64_t count, const int64_t offset, int64_t &read_size);
	int pread(const int64_t count, const int64_t offset, IFileBuffer &file_buf, int64_t &read_size);
	int get_open_flags() const;
	int get_open_mode() const;

public:
	int pread_by_fd(const int fd, void *buf, const int64_t count, const int64_t offset, int64_t &read_size);
	int pread_by_fd(const int fd, const int64_t count, const int64_t offset, IFileBuffer &file_buf, int64_t &read_size);

private:
	const int64_t align_size_;
	const int64_t buffer_size_;
	char *buffer_;
};


class BladeFileBuffer : public IFileBuffer
{
	static const int64_t MIN_BUFFER_SIZE = 1 * 1024 * 1024;
public:
    BladeFileBuffer();
    ~BladeFileBuffer();

public:
	char *get_buffer();
    int64_t get_base_pos();
    void set_base_pos(const int64_t pos);
    int assign(const int64_t size, const int64_t align);
    int assign(const int64_t size);

public:
    void release();

private:
    char *buffer_;
    int64_t base_pos_;
    int64_t buffer_size_;
};

// file reader 线程不安全 只允许同时一个线程调用
class BladeFileReader
{
public:
    BladeFileReader();
    ~BladeFileReader();

public:
    int open(const string &fname, const bool dio, const int64_t align_size = DirectFileReader::DEFAULT_ALIGN_SIZE);
    void close();
    bool is_opened() const;

public:
    int pread(void *buf, const int64_t count, const int64_t offset, int64_t &read_size);
    int pread(const int64_t count, const int64_t offset, IFileBuffer &file_buf, int64_t &read_size);

public:
    static int read_record(IFileInfoMgr& fileinfo_mgr, const uint64_t file_id, const int64_t offset, const int64_t size, IFileBuffer &file_buf);
    static int read_record(const IFileInfo& file_info, const int64_t offset, const int64_t size, IFileBuffer &file_buf);

private:
    IFileReader *file_;
};


class DirectFileAppender : public IFileAppender
{   
	static const int64_t DEBUG_MAGIC = 0x1a2b3c4d;
	static const int NORMAL_FLAGS = O_RDWR | O_DIRECT;
	static const int CREAT_FLAGS = O_CREAT;
	static const int TRUNC_FLAGS = O_TRUNC;
	static const int EXCL_FLAGS = O_EXCL;
	static const int OPEN_MODE = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
public:
	static const int64_t DEFAULT_ALIGN_SIZE = 4 * 1024;
	static const int64_t DEFAULT_BUFFER_SIZE = 2 * 1024 * 1024;

public:
	DirectFileAppender(const int64_t buffer_size = DEFAULT_BUFFER_SIZE, const int64_t align_size = DEFAULT_ALIGN_SIZE);
	~DirectFileAppender();

public:
	int append(const void *buf, const int64_t count, const bool is_fsync);
	int async_append(const void *buf, const int64_t count, IFileAsyncCallback *callback);
	int fsync();
	int get_open_flags() const;
	int get_open_mode() const;
	int64_t get_file_pos() const {return file_pos_;}

private:
	int buffer_sync_();

private:
	int prepare_buffer_();
	void set_normal_flags_();
	void add_truncate_flags_();
	void add_create_flags_();
	void add_excl_flags_();
	void set_file_pos_(const int64_t file_pos);

private:
	int open_flags_;
	const int64_t align_size_;
	const int64_t buffer_size_;
	int64_t buffer_pos_;
	int64_t file_pos_;
	char *buffer_;
	int64_t buffer_length_;
};

class BufferFileAppender : public IFileAppender                                                                           
{   
	static const int NORMAL_FLAGS = O_WRONLY;
	static const int CREAT_FLAGS = O_CREAT;
	static const int TRUNC_FLAGS = O_TRUNC;
	static const int EXCL_FLAGS = O_EXCL;
	static const int OPEN_MODE = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
public:
	static const int64_t DEFAULT_BUFFER_SIZE = 2 * 1024 * 1024;
public:
	BufferFileAppender(const int64_t buffer_size = DEFAULT_BUFFER_SIZE);
	~BufferFileAppender();
public:
	int append(const void *buf, const int64_t count, const bool is_fsync);
	int async_append(const void *buf, const int64_t count, IFileAsyncCallback *callback);
	int fsync();
	int get_open_flags() const;
	int get_open_mode() const;
	int64_t get_file_pos() const {return file_pos_;}
private:
	int buffer_sync_();
private:
	int prepare_buffer_();
	void set_normal_flags_();
	void add_truncate_flags_();
	void add_create_flags_();
	void add_excl_flags_();
	void set_file_pos_(const int64_t file_pos);
private:
	int open_flags_;
	const int64_t buffer_size_;
	int64_t buffer_pos_;
	int64_t file_pos_;
	char *buffer_;
};

// file appender 线程不安全 只允许同时一个线程调用
// 关闭普通io的实现 暂时只支持dio
class BladeFileAppender
{
public:
    BladeFileAppender();
    virtual ~BladeFileAppender();

public:
    // 子类可以重载open close get_file_pos
    virtual int open(const string &fname, const bool dio, const bool is_create, const bool is_trunc = false, const int64_t align_size = DirectFileAppender::DEFAULT_ALIGN_SIZE);

    virtual void close();

    virtual int64_t get_file_pos() const {return NULL == file_ ? -1 : file_->get_file_pos();}

public:
    int create(const string &fname, const bool dio, const int64_t align_size = DirectFileAppender::DEFAULT_ALIGN_SIZE);

    bool is_opened() const;

public:
    // 子类可以重载append fsync
    // 如果没有设置is_fsync 那么默认使用内部buffer缓存数据 直到buffer满后刷磁盘
    virtual int append(const void *buf, const int64_t count, const bool is_fsync);
    virtual int fsync();

public:
    // 暂时不实现
    int async_append(const void *buf, const int64_t count, IFileAsyncCallback *callback);

private:
    IFileAppender *file_;
};

// 如果目标不存在则改名 否在返回失败
extern int atomic_rename(const char *oldpath, const char *newpath);

// 不被信号打断的pwrite
extern int64_t unintr_pwrite(const int fd, const void *buf, const int64_t count, const int64_t offset);

// 不被信号打断的pread
extern int64_t unintr_pread(const int fd, void *buf, const int64_t count, const int64_t offset);

// 判断buf count offest是否对齐
extern bool is_align(const void *buf, const int64_t count, const int64_t offset, const int64_t align_size);

// 使用一个对齐的辅助buffer 进行dio读取
extern int64_t pread_align(const int fd, void *buf, const int64_t count, const int64_t offset, void *align_buffer, const int64_t buffer_size, const int64_t align_size);

// 使用一个对齐的辅助buffer 进行dio写入 buffer可能存在为了对齐而保留的数据 因此需要传入和传出buffer_pos
extern int64_t pwrite_align(const int fd, const void *buf, const int64_t count, const int64_t offset, void *align_buffer, const int64_t buffer_size, const int64_t align_size, int64_t &buffer_pos);

// 获取一个文件句柄指向文件的size
extern int64_t get_file_size(const int fd);
extern int64_t get_file_size(const char *fname);

}//end of namespace common
}//end of namespace bladestore
#endif 
