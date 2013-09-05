/*
 *version  : 1.0
 *author   : chen yunyun
 *date     : 2012-5-4
 *
 */
#ifndef BLADESTORE_COMMON_FILE_UTILS_H
#define BLADESTORE_COMMON_FILE_UTILS_H

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

namespace bladestore
{
namespace common
{
class FileUtils
{
public:
	FileUtils ();
	explicit FileUtils(int fd);
	virtual ~ FileUtils ();

	virtual int32_t open (const char *filepath, const int64_t flags, mode_t mode);
	int32_t open (const char* filepath, const int64_t flags);
	virtual void close ();
	bool is_open();
	void set_fd(int32_t fd)
	{
		fd_ = fd;
		own_fd_ = false;
	}
	int64_t lseek (const int64_t offset, const int64_t whence);
	int64_t read (char *data, const int64_t size);
	virtual int64_t write (const char *data, const int64_t size, bool sync = false);
	int64_t pread(char *data, const int64_t size, const int64_t offset);
	int64_t pwrite(const char *data, const int64_t size, const int64_t offfset, bool sync = false);
	virtual int ftruncate(int64_t length);

private:
	bool own_fd_;
	int32_t fd_;
};
}//end of namespace common
}//end of namespace bladestore
#endif

