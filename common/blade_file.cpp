#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include <malloc.h>
#include <errno.h>

#include "blade_file.h"
#include "utility.h"
#include "log.h"

using namespace pandora;

namespace bladestore
{
namespace common
{

namespace fileutil
{
template <class T>
int open(const string &fname, T &file, int &fd)
{
	int ret = BLADE_SUCCESS;
	if (-1 != fd)
	{
		LOGV(MSG_WARN, "file has been open fd=%d", fd);
		ret = BLADE_INIT_TWICE;
	}
	else if (0 == fname.length())
	{
		ret = BLADE_INVALID_ARGUMENT;
	}
	else
	{
		const char *fname_ptr = NULL;
		char buffer[BLADE_MAX_FILENAME_LENGTH];
		if ('\0' != fname[fname.length() - 1])
		{
			if (BLADE_MAX_FILENAME_LENGTH > snprintf(buffer, BLADE_MAX_FILENAME_LENGTH, "%.*s", fname.length(), fname.c_str()))
            {
				fname_ptr = buffer;
            }
		}
		else
		{
			fname_ptr = fname.c_str();
		}

		if (NULL == fname_ptr)
		{
			LOGV(LL_WARN, "prepare fname string fail fname=[%.*s]", fname.length(), fname.c_str());
            ret = BLADE_INVALID_ARGUMENT;
		}
		else if (-1 == (fd = ::open(fname_ptr, file.get_open_flags(), file.get_open_mode())))
		{
			if (ENOENT == errno)
            {
				ret = BLADE_FILE_NOT_EXIST;
            }
            else
            {
				LOGV(LL_WARN, "open fname=[%s] fail errno=%u", fname_ptr, errno);
				ret = BLADE_IO_ERROR;
            }
		}
		else
		{
			LOGV(MSG_INFO, "open fname=[%s] fd=%d succ", fname_ptr, fd);
		}
	}

	return ret;
}

}//end of namespace fileutil

int IFileReader::open(const string &fname)
{
	return fileutil::open(fname, *this, fd_);
}

void IFileReader::close()
{
	if (-1 != fd_)
	{
		::close(fd_);
		fd_ = -1;
	}
}

bool IFileReader::is_opened() const
{
	return fd_ != -1;
}


BufferFileReader::BufferFileReader()
{

}

BufferFileReader::~BufferFileReader()
{
	if (-1 != fd_)
	{
		this->close();
	}
}

int BufferFileReader::pread(void *buf, const int64_t count, const int64_t offset, int64_t &read_size)
{
	return pread_by_fd(fd_, buf, count, offset, read_size);
}

int BufferFileReader::pread_by_fd(const int fd, void *buf, const int64_t count, const int64_t offset, int64_t &read_size)
{
	int ret = BLADE_SUCCESS;
	int64_t read_ret = 0;
	if (0 == count)
	{
		read_size = 0;
	}
	else if (NULL == buf || 0 > count)
	{
		ret = BLADE_INVALID_ARGUMENT;
	}
	else if (-1 == fd)
	{
		LOGV(MSG_WARN, "file has not been open");
		ret = BLADE_ERROR;
	}
	else if (0 > (read_ret = unintr_pread(fd, buf, count, offset)))
	{
		LOGV(MSG_WARN, "read fail fd=%d count=%ld offset=%ld read_ret=%ld errno=%u", fd, count, offset, read_ret, errno);
		ret = BLADE_IO_ERROR;
	}
	else
	{
		read_size = read_ret;
	}
	return ret;
}

int BufferFileReader::pread(const int64_t count, const int64_t offset, IFileBuffer &file_buf, int64_t &read_size)
{
	return pread_by_fd(fd_, count, offset, file_buf, read_size);
}

int BufferFileReader::pread_by_fd(const int fd, const int64_t count, const int64_t offset, IFileBuffer &file_buf, int64_t &read_size)
{
	int ret = BLADE_SUCCESS;
	if (0 == count)
	{
		read_size = 0;
	}
	else if (0 > count || BLADE_SUCCESS != (ret = file_buf.assign(count)) || NULL == file_buf.get_buffer())
	{
		LOGV(MSG_WARN, "file_buf assign fail count=%ld ret=%d or get_buffer null pointer", count, ret);
		ret = (BLADE_SUCCESS == ret) ? BLADE_INVALID_ARGUMENT : ret;
	}
	else
	{
		file_buf.set_base_pos(0);
		ret = pread_by_fd(fd, file_buf.get_buffer(), count, offset, read_size);
	}
	return ret;
}

int BufferFileReader::get_open_flags() const
{
	return OPEN_FLAGS;
}

int BufferFileReader::get_open_mode() const
{
	return OPEN_MODE;
}


DirectFileReader::DirectFileReader(const int64_t buffer_size,const int64_t align_size) : align_size_(align_size), buffer_size_(buffer_size), buffer_(NULL)
{

}

DirectFileReader::~DirectFileReader()
{
	if (-1 != fd_)
	{
		this->close();
	}
	if (NULL != buffer_)
	{
		::free(buffer_);
		buffer_ = NULL;
	}
}

int DirectFileReader::pread(void *buf, const int64_t count, const int64_t offset, int64_t &read_size)
{
	return pread_by_fd(fd_, buf, count, offset, read_size);
}

int DirectFileReader::pread_by_fd(const int fd, void *buf, const int64_t count, const int64_t offset, int64_t &read_size)
{
	int ret = BLADE_SUCCESS;
	bool param_align = is_align(buf, count, offset, align_size_);
	if (0 == count)
	{
		read_size = 0;
	}
	else if (NULL == buf || 0 > count)
	{
		ret = BLADE_INVALID_ARGUMENT;
	}
	else if (-1 == fd)
	{
		LOGV(MSG_WARN, "file has not been open");
		ret = BLADE_ERROR;
	}
	else if (!param_align && NULL == buffer_ && NULL == (buffer_ = (char*)::memalign(align_size_, buffer_size_ + align_size_)))
	{
		LOGV(MSG_WARN, "prepare buffer fail param_align=%s buffer=%p buf=%p count=%ld offset=%ld align_size=%ld buffer_size=%ld", STR_BOOL(param_align), buffer_, buf, count, offset, align_size_, buffer_size_);
		ret = BLADE_ERROR;
	}
	else
	{
		int64_t read_ret = 0;
		if (param_align)
		{
			read_ret = unintr_pread(fd, buf, count, offset);
		}
		else
		{
			read_ret = pread_align(fd, buf, count, offset, buffer_, buffer_size_, align_size_);
		}

		if (0 > read_ret)
		{
			LOGV(MSG_WARN, "read fail fd=%d count=%ld offset=%ld read_ret=%ld errno=%u", fd_, count, offset, read_ret, errno);
            ret = BLADE_IO_ERROR;
		}
		else
		{
			read_size = read_ret;
		}
	}
	return ret;
}

int DirectFileReader::pread(const int64_t count, const int64_t offset, IFileBuffer &file_buf, int64_t &read_size)
{
	return pread_by_fd(fd_, count, offset, file_buf, read_size);
}

int DirectFileReader::pread_by_fd(const int fd, const int64_t count, const int64_t offset, IFileBuffer &file_buf, int64_t &read_size)
{
	int ret = BLADE_SUCCESS;
	int64_t offset2read = lower_align(offset, align_size_);
	int64_t size2read = upper_align(offset + count, align_size_) - offset2read;
	if (0 == count)
	{
		read_size = 0;
	}
	else if (0 > count || BLADE_SUCCESS != (ret = file_buf.assign(size2read, align_size_)) || NULL == file_buf.get_buffer())
	{
		LOGV(MSG_WARN, "file_buf assign fail count=%ld ret=%d or get_buffer null pointer", count, ret);
		ret = (BLADE_SUCCESS == ret) ? BLADE_INVALID_ARGUMENT : ret;
	}
	else if (-1 == fd)
	{
		LOGV(MSG_WARN, "file has not been open");
		ret = BLADE_ERROR;
	}
	else
	{
		int64_t buffer_offset = offset - offset2read;
		int64_t read_ret = 0;
		if (0 > (read_ret = unintr_pread(fd, file_buf.get_buffer(), size2read, offset2read)))
		{
			LOGV(MSG_WARN, "read fail fd=%d count=%ld offset=%ld read_ret=%ld errno=%u", fd, count, offset, read_ret, errno);
			ret = BLADE_IO_ERROR;
		}
		else
		{
            file_buf.set_base_pos(buffer_offset);
            read_size = (read_ret < buffer_offset) ? 0 : std::min(read_ret - buffer_offset, count);
		}
	}
	return ret;
}

int DirectFileReader::get_open_flags() const
{
	return OPEN_FLAGS;
}

int DirectFileReader::get_open_mode() const
{
	return OPEN_MODE;
}

int IFileAppender::open(const string &fname, const bool is_create, const bool is_trunc)
{
	int ret = BLADE_SUCCESS;
	if (is_trunc)
	{
		this->add_truncate_flags_();
	}

	if (is_create)
	{
		this->add_create_flags_();
	}

	ret = fileutil::open(fname, *this, fd_);
	this->set_normal_flags_();

	if (BLADE_SUCCESS == ret)
	{
		int64_t file_pos = get_file_size(fd_);
		this->set_file_pos_(file_pos);
		if (0 > file_pos || BLADE_SUCCESS != (ret = this->prepare_buffer_()))
		{
			this->close();
			ret = (BLADE_SUCCESS == ret) ? BLADE_ERROR : ret;
		}
	}
	return ret;
}

int IFileAppender::create(const string &fname)
{
	int ret = BLADE_SUCCESS;
	this->add_create_flags_();
	this->add_excl_flags_();
	ret = fileutil::open(fname, *this, fd_);
	this->set_normal_flags_();
	this->set_file_pos_(0);
	if (BLADE_SUCCESS != (ret = this->prepare_buffer_()))
	{
		this->close();
	}
	return ret;
}
      
void IFileAppender::close()
{
	if (-1 != fd_)
	{
		this->fsync();
		::close(fd_);
		fd_ = -1;
	}
}

bool IFileAppender::is_opened() const
{
	return fd_ != -1;
}

DirectFileAppender::DirectFileAppender(const int64_t buffer_size, const int64_t align_size) : open_flags_(NORMAL_FLAGS),
                                                                        align_size_(align_size),
                                                                        buffer_size_(buffer_size),
                                                                        buffer_pos_(0),
                                                                        file_pos_(0),
                                                                        buffer_(NULL),
                                                                        buffer_length_(0)
{


}

DirectFileAppender::~DirectFileAppender()
{
	if (-1 != fd_)
	{
		this->close();
	}
	if (NULL != buffer_)
	{
		::free(buffer_);
		buffer_ = NULL;
	}
}

int DirectFileAppender::buffer_sync_()
{
	int ret = BLADE_SUCCESS;
	if (NULL != buffer_ && 0 != buffer_length_)
	{
		int64_t offset2write = lower_align(file_pos_, align_size_);
		int64_t size2write = buffer_pos_;

		size2write = upper_align(buffer_pos_, align_size_);
		int64_t write_ret = 0;
		memset(buffer_ + buffer_pos_, 0, buffer_size_ - buffer_pos_);
		if (size2write != (write_ret = unintr_pwrite(fd_, buffer_, size2write, offset2write)))
		{
			LOGV(MSG_WARN, "write buffer fail fd=%d buffer=%p count=%ld offset=%ld write_ret=%ld errno=%u file_pos=%ld align_size=%ld buffer_pos=%ld", fd_, buffer_, size2write, offset2write, write_ret, errno, file_pos_, align_size_, buffer_pos_);
            ret = BLADE_IO_ERROR;
		}
		else
		{
            file_pos_ += buffer_length_;
            buffer_length_ = 0;
            int64_t size2reserve = buffer_pos_ - lower_align(buffer_pos_, align_size_);
            if (0 != size2reserve)
            {
				memmove(buffer_, buffer_ + buffer_pos_ - size2reserve, size2reserve);
				buffer_pos_ = size2reserve;
            }
            else
            {
				buffer_pos_ = 0;
			}
		}
	}
	return ret;
}

int DirectFileAppender::fsync()
{
	int ret = BLADE_SUCCESS;
	if (-1 == fd_)
	{
		LOGV(MSG_WARN, "file has not been open");
		ret = BLADE_ERROR;
	}
	else if (BLADE_SUCCESS == (ret = buffer_sync_()))
	{
		if (0 != ::ftruncate(fd_, file_pos_))
		{
			LOGV(MSG_WARN, "ftruncate fail fd=%d file_pos=%ld errno=%u", fd_, file_pos_, errno);
			ret = BLADE_IO_ERROR;
		}
	}
	return ret;
}

int DirectFileAppender::async_append(const void *buf, const int64_t count, IFileAsyncCallback *callback)
{
	// TODO will support soon
	UNUSED(buf);
	UNUSED(count);
	UNUSED(callback);
	return BLADE_ERROR;
}

int DirectFileAppender::append(const void *buf, const int64_t count, const bool is_fsync)
{
	int ret = BLADE_SUCCESS;
	if (-1 == fd_)
	{
		LOGV(MSG_WARN, "file has not been open");
		ret = BLADE_ERROR;
	}
	else if (0 == count)
	{
		// do nothing
	}
	else if (NULL == buf || 0 > count)
	{
		LOGV(MSG_WARN, "invalid param buf=%p count=%ld", buf, count);
		ret = BLADE_INVALID_ARGUMENT;
	}
	else
	{
		bool buffer_synced = false;
		if (buffer_size_ < (buffer_pos_ + count))
		{
			ret = buffer_sync_();
            buffer_synced = true;
		}

		if (BLADE_SUCCESS == ret)
		{
			if (buffer_synced && buffer_size_ < (buffer_pos_ + count))
            {
				bool param_align = is_align(buf, count, file_pos_, align_size_);
				int64_t write_ret = 0;
				if (param_align)
				{
					write_ret = unintr_pwrite(fd_, buf, count, file_pos_);
				}
				else
				{
					write_ret = pwrite_align(fd_, buf, count, file_pos_, buffer_, buffer_size_, align_size_, buffer_pos_);
				}

				if (count != write_ret)
				{
					LOGV(LL_WARN, "write fail fd=%d buffer=%p count=%ld offset=%ld write_ret=%ld errno=%u align_buffer=%p align_size=%ld buffer_pos=%ld", fd_, buf, count, file_pos_, write_ret, errno, buffer_, align_size_, buffer_pos_);
					ret = BLADE_ERROR;
				}
				else
				{
					file_pos_ += count;
				}
            }
            else
            {
				memcpy(buffer_ + buffer_pos_, buf, count);
				buffer_pos_ += count;
				buffer_length_ += count;
			}
		}
	}

	if (BLADE_SUCCESS == ret && is_fsync)
	{
		ret = this->fsync();
	}
	return ret;
}

int DirectFileAppender::prepare_buffer_()
{
	int ret = BLADE_SUCCESS;
	if (NULL == buffer_ && NULL == (buffer_ = (char*)::memalign(align_size_, buffer_size_)))
	{
          LOGV(MSG_WARN, "prepare buffer fail align_size=%ld buffer_size=%ld", align_size_, buffer_size_);
          ret = BLADE_ERROR;
	}
	else if (0 != (file_pos_ % align_size_))
	{
		int64_t offset2read = lower_align(file_pos_, align_size_);
		int64_t size2read = file_pos_ - offset2read;
		int64_t read_ret = 0;
		if (size2read != (read_ret = unintr_pread(fd_, buffer_, align_size_, offset2read)))
		{
			LOGV(MSG_WARN, "read buffer fail fd=%d buffer=%p align_size=%ld offset2read=%ld size2read=%ld read_ret=%ld errno=%u", fd_, buffer_, align_size_, offset2read, size2read, read_ret, errno);
            ret = BLADE_IO_ERROR;
		}
		else
		{
            buffer_pos_ = read_ret;
		}
	}
	return ret;
}

void DirectFileAppender::set_normal_flags_()
{
	open_flags_ = NORMAL_FLAGS;
}

void DirectFileAppender::add_truncate_flags_()
{
	open_flags_ |= TRUNC_FLAGS;
}

void DirectFileAppender::add_create_flags_()
{
	open_flags_ |= CREAT_FLAGS;
}

void DirectFileAppender::add_excl_flags_()
{
	open_flags_ |= EXCL_FLAGS;
}

void DirectFileAppender::set_file_pos_(const int64_t file_pos)
{
	file_pos_ = file_pos;
}

int DirectFileAppender::get_open_flags() const
{
	return open_flags_;
}

int DirectFileAppender::get_open_mode() const
{
	return OPEN_MODE;
}


BladeFileBuffer::BladeFileBuffer() : buffer_(NULL), base_pos_(0), buffer_size_(0)
{

}

BladeFileBuffer::~BladeFileBuffer()
{
	if (NULL != buffer_)
	{
		::free(buffer_);
		buffer_ = NULL;
	}
}

char *BladeFileBuffer::get_buffer()
{
	return buffer_;
}

int64_t BladeFileBuffer::get_base_pos()
{
	return base_pos_;
}

void BladeFileBuffer::set_base_pos(const int64_t pos)
{
	if (pos > buffer_size_)
	{
		LOGV(MSG_WARN, "base_pos=%ld will be greater than buffer_size=%ld", pos, buffer_size_);
	}
	base_pos_ = pos;
}

int BladeFileBuffer::assign(const int64_t size, const int64_t align)
{
	int ret = BLADE_SUCCESS;
	if (0 >= size || 0 >= align || !is2n(align) || 0 != size % align)
	{
		LOGV(MSG_WARN, "invalid size=%ld or donot match align=%ld", size, align);
        ret = BLADE_INVALID_ARGUMENT;
	}
	else if (NULL == buffer_ || buffer_size_ < size || (MIN_BUFFER_SIZE < buffer_size_ && MIN_BUFFER_SIZE > size) || 0 != (int64_t)buffer_ % align)
	{
		int64_t alloc_size = size;
        if (MIN_BUFFER_SIZE > size)
        {
			alloc_size = MIN_BUFFER_SIZE;
        }

        if (NULL != buffer_)
        {
			::free(buffer_);
			buffer_ = NULL;
			base_pos_ = 0;
			buffer_size_ = 0;
        }

        if (NULL == (buffer_ = (char*)::memalign(align, alloc_size)))
        {
			LOGV(MSG_WARN, "memalign fail align=%ld alloc_size=%ld size=%ld errno=%u", align, alloc_size, size, errno);
			ret = BLADE_ERROR;
        }
        else
        {
			buffer_size_ = alloc_size;
        }
	}
	else
	{
        base_pos_ = 0;
	}
	return ret;
}

int BladeFileBuffer::assign(const int64_t size)
{
 	int ret = BLADE_SUCCESS;
	if (0 >= size)
	{
        LOGV(MSG_WARN, "invalid size=%ld", size);
        ret = BLADE_INVALID_ARGUMENT;
	}
	else if (NULL == buffer_ || buffer_size_ < size || (MIN_BUFFER_SIZE < buffer_size_ && MIN_BUFFER_SIZE > size))
	{
        int64_t alloc_size = size;
        if (MIN_BUFFER_SIZE > size);
        {
			alloc_size = MIN_BUFFER_SIZE;
        }

        if (NULL != buffer_)
        {
			::free(buffer_);
			buffer_ = NULL;
			base_pos_ = 0;
			buffer_size_ = 0;
        }

        if (NULL == (buffer_ = (char*)::malloc(alloc_size)))
        {
			LOGV(MSG_WARN, "malloc fail alloc_size=%ld size=%ld errno=%u", alloc_size, size, errno);
			ret = BLADE_ERROR;
        }
        else
        {
			buffer_size_ = alloc_size;
        }
	}
	else
	{
		base_pos_ = 0;
	}
	return ret;
}


BladeFileReader::BladeFileReader() : file_(NULL)
{

}

BladeFileReader::~BladeFileReader()
{
	if (NULL != file_)
	{
        delete file_;
        file_ = NULL;
	}
}

int BladeFileReader::open(const string & fname, const bool dio, const int64_t align_size)
{
	int ret = BLADE_SUCCESS;
	if (NULL != file_)
	{
		ret = BLADE_INIT_TWICE;
	}
	else if (!is2n(align_size))
	{
		LOGV(LL_WARN, "invalid align_size=%ld", align_size);
		ret = BLADE_INVALID_ARGUMENT;
	}
	else
	{
        if (dio)
        {
          file_ = new(std::nothrow) DirectFileReader(DirectFileReader::DEFAULT_BUFFER_SIZE, align_size);
        }
        else
        {
          file_ = new(std::nothrow) BufferFileReader();
        }
	}

	if (NULL == file_)
	{
		LOGV(MSG_WARN, "construct file reader fail fname=[%.*s]", fname.length(), fname.c_str());
		ret = BLADE_ERROR;
	}
	else
	{
		ret = file_->open(fname);
	}
	return ret;
}

void BladeFileReader::close()
{
	if (NULL != file_)
	{
		file_->close();
		delete file_;
		file_ = NULL;
	}
}

bool BladeFileReader::is_opened() const
{
	return NULL != file_ && file_->is_opened();
}

int BladeFileReader::pread(void *buf, const int64_t count, const int64_t offset, int64_t &read_size)
{
	int ret = BLADE_SUCCESS;
	if (NULL == file_)
	{
		ret = BLADE_ERROR;
	}
	else
	{
		ret = file_->pread(buf, count, offset, read_size);
	}
	return ret;
}

int BladeFileReader::pread(const int64_t count, const int64_t offset, IFileBuffer &file_buf, int64_t &read_size)
{
	int ret = BLADE_SUCCESS;
	if (NULL == file_)
	{
		ret = BLADE_ERROR;
	}
	else
	{
		ret = file_->pread(count, offset, file_buf, read_size);
	}
	return ret;
}

int BladeFileReader::read_record(IFileInfoMgr& fileinfo_mgr,
                                  const uint64_t file_id,
                                  const int64_t offset,
                                  const int64_t size,
                                  IFileBuffer &file_buf)
{
	int ret = BLADE_SUCCESS;
	const IFileInfo *file_info = fileinfo_mgr.GetFileInfo(file_id);
	if (NULL == file_info)
	{
		LOGV(MSG_WARN, "get file info fail file_id=%lu offset=%ld size=%ld", file_id, offset, size);
        ret = BLADE_ERROR;
	}
	else
	{
		ret = read_record(*file_info, offset, size, file_buf);
        fileinfo_mgr.RevertFileInfo(file_info);
	}

	return ret;
}

int BladeFileReader::read_record(const IFileInfo& file_info, const int64_t offset, const int64_t size, IFileBuffer &file_buf)
{
	int ret = BLADE_SUCCESS;
	int fd = file_info.GetFD();
	int flags = 0;
	if (-1 == fd)
	{
		LOGV(MSG_WARN, "invalid fd");
		ret = BLADE_INVALID_ARGUMENT;
	}
	else if (-1 == (flags = fcntl(fd, F_GETFL)))
	{
		LOGV(MSG_WARN, "fcntl F_GETFL fail fd=%d errno=%u", fd, errno);
		ret = BLADE_ERROR;
	}
	else
	{
        BufferFileReader buffer_reader;
        DirectFileReader direct_reader;
        IFileReader *preader = NULL;
        if (O_DIRECT == (O_DIRECT & flags))
        {
			preader = &direct_reader;
        }
        else
        {
			preader = &buffer_reader;
        }
        int64_t read_size = 0;
        ret = preader->pread_by_fd(fd, size, offset, file_buf, read_size);
        if (size != read_size)
        {
			LOGV(MSG_WARN, "read_size=%ld do not equal size=%ld", read_size, size);
			ret = (BLADE_SUCCESS == ret) ? BLADE_ERROR : ret;
        }
	}
	return ret;
}


BladeFileAppender::BladeFileAppender() : file_(NULL)
{

}

BladeFileAppender::~BladeFileAppender()
{
	if (NULL != file_)
	{
		delete file_;
		file_ = NULL;
	}
}

int BladeFileAppender::open(const string &fname, const bool dio, const bool is_create, const bool is_trunc, const int64_t align_size)
{
	int ret = BLADE_SUCCESS;

	if (NULL != file_)
	{
		ret = BLADE_INIT_TWICE;
	}
	else if (!is2n(align_size))
	{
		LOGV(MSG_WARN, "invalid align_size=%ld", align_size);
        ret = BLADE_INVALID_ARGUMENT;
	}
	else
	{
        if (dio)
        {
			file_ = new(std::nothrow) DirectFileAppender(DirectFileAppender::DEFAULT_BUFFER_SIZE, align_size);
        }
        else
        {
			file_ = new(std::nothrow) BufferFileAppender();
			//LOGV(MSG_WARN, "do not support buffer io now fname=[%.*s]", fname.length(), fname.c_str());
        }
	}

	if (NULL == file_)
	{
        LOGV(MSG_WARN, "construct file appender fail fname=[%.*s]", fname.length(), fname.c_str());
        ret = BLADE_ERROR;
	}
	else
	{
        ret = file_->open(fname, is_create, is_trunc);
	}

	return ret;
}

int BladeFileAppender::create(const string &fname, const bool dio, const int64_t align_size)
{
	int ret = BLADE_SUCCESS;
	if (NULL != file_)
	{
		ret = BLADE_INIT_TWICE;
	}
	else if (!is2n(align_size))
	{
		LOGV(MSG_WARN, "invalid align_size=%ld", align_size);
		ret = BLADE_INVALID_ARGUMENT;
	}
	else
	{
		if (dio)
		{
			file_ = new(std::nothrow) DirectFileAppender(DirectFileAppender::DEFAULT_BUFFER_SIZE, align_size);
		}
		else
		{
			file_ = new(std::nothrow) BufferFileAppender();
			//LOGV(MSG_WARN, "do not support buffer io now fname=[%.*s]", fname.length(), fname.c_str());
		}
		if (NULL == file_)
		{
			LOGV(MSG_WARN, "construct file appender fail fname=[%.*s]", fname.length(), fname.c_str());
			ret = BLADE_ERROR;
		}
		else
		{
			ret = file_->create(fname);
		}
	}
	return ret;
}

void BladeFileAppender::close()
{
	if (NULL != file_)
	{
		file_->close();
		delete file_;
		file_ = NULL;
	}
}

bool BladeFileAppender::is_opened() const
{
	return NULL != file_ && file_->is_opened();
}

int BladeFileAppender::append(const void *buf, const int64_t count, const bool is_fsync)
{
	int ret = BLADE_SUCCESS;
	if (NULL == file_)
	{
		ret = BLADE_ERROR;
	}
	else
	{
		ret = file_->append(buf, count, is_fsync);
	}
	return ret;
}

int BladeFileAppender::async_append(const void *buf, const int64_t count, IFileAsyncCallback *callback)
{
	int ret = BLADE_SUCCESS;
	if (NULL == file_)
	{
		ret = BLADE_ERROR;
	}
	else
	{
		ret = file_->async_append(buf, count, callback);
	}
	return ret;
}

int BladeFileAppender::fsync()
{
	int ret = BLADE_SUCCESS;
	if (NULL == file_)
	{
		ret = BLADE_ERROR;
	}
	else
	{
		ret = file_->fsync();
	}
	return ret;
}

BufferFileAppender::BufferFileAppender(const int64_t buffer_size) : open_flags_(NORMAL_FLAGS),
	buffer_size_(buffer_size),
	buffer_pos_(0),
	file_pos_(0),
	buffer_(NULL)
{
}

BufferFileAppender::~BufferFileAppender()
{
	if (-1 != fd_)
	{
		this->close();
	}
	if (NULL != buffer_)
	{
		::free(buffer_);
		buffer_ = NULL;
	}
}

int BufferFileAppender::buffer_sync_()
{
	int ret = BLADE_SUCCESS;
	if (NULL != buffer_
			&& 0 != buffer_pos_)
	{
		int64_t write_ret = 0;
		if (buffer_pos_ != (write_ret = unintr_pwrite(fd_, buffer_, buffer_pos_, file_pos_)))
		{
			LOGV(LL_WARN, "write buffer fail fd=%d buffer=%p count=%ld offset=%ld write_ret=%ld errno=%u",
					fd_, buffer_, buffer_pos_, file_pos_, write_ret, errno);
			ret = BLADE_IO_ERROR;
		}
		else
		{
			file_pos_ += buffer_pos_;
			buffer_pos_ = 0;
		}
	}
	return ret;
}

int BufferFileAppender::fsync()
{
	int ret = BLADE_SUCCESS;
	if (-1 == fd_)
	{
		LOGV(LL_WARN, "file has not been open");
		ret = BLADE_ERROR;
	}
	else if (BLADE_SUCCESS == (ret = buffer_sync_()))
	{
		if (0 != ::fsync(fd_))
		{
			LOGV(LL_WARN, "fsync fail fd=%d errno=%u", fd_, errno);
			ret = BLADE_IO_ERROR;
		}
	}
	return ret;
}

int BufferFileAppender::async_append(const void *buf, const int64_t count, IFileAsyncCallback *callback)
{
	UNUSED(buf);
	UNUSED(count);
	UNUSED(callback);
	return BLADE_ERROR;
}

int BufferFileAppender::append(const void *buf, const int64_t count, const bool is_fsync)
{
	int ret = BLADE_SUCCESS;
	if (-1 == fd_)
	{
		LOGV(LL_WARN, "file has not been open");
		ret = BLADE_ERROR;
	}
	else if (0 == count)
	{
		// do nothing
	}
	else if (NULL == buf || 0 > count)
	{
		LOGV(LL_WARN, "invalid param buf=%p count=%ld", buf, count);
		ret = BLADE_INVALID_ARGUMENT;
	}
	else
	{
		if ((buffer_size_ - buffer_pos_) < count)
		{
			ret = buffer_sync_();
		}

		if (BLADE_SUCCESS == ret)
		{
			if ((buffer_size_ - buffer_pos_) < count)
			{
				int64_t write_ret = 0;
				if (count != (write_ret = unintr_pwrite(fd_, buf, count, file_pos_)))
				{
					LOGV(LL_WARN, "write fail fd=%d buffer=%p count=%ld offset=%ld write_ret=%ld errno=%u",
							fd_, buf, count, file_pos_, write_ret, errno);
					ret = BLADE_IO_ERROR;
				}
				else 
				{
					file_pos_ += count;
				}
			}
			else
			{
				memcpy(buffer_ + buffer_pos_, buf, count);
				buffer_pos_ += count;
			}
		}
	}
	if (BLADE_SUCCESS == ret && is_fsync)
	{
		ret = this->fsync();
	}
	return ret;
}

int BufferFileAppender::prepare_buffer_()
{
	int ret = BLADE_SUCCESS;
	if (NULL == buffer_
			&& NULL == (buffer_ = (char*)::malloc(buffer_size_)))
	{
		LOGV(LL_WARN, "prepare buffer fail buffer_size=%ld", buffer_size_);
		ret = BLADE_ERROR;
	}
	return ret;
}

void BufferFileAppender::set_normal_flags_()
{
	open_flags_ = NORMAL_FLAGS;
}

void BufferFileAppender::add_truncate_flags_()
{
	open_flags_ |= TRUNC_FLAGS;
}

void BufferFileAppender::add_create_flags_()
{
	open_flags_ |= CREAT_FLAGS;
}

void BufferFileAppender::add_excl_flags_()
{
	open_flags_ |= EXCL_FLAGS;
}

void BufferFileAppender::set_file_pos_(const int64_t file_pos)
{
	file_pos_ = file_pos;
}

int BufferFileAppender::get_open_flags() const
{
	return open_flags_;
}

int BufferFileAppender::get_open_mode() const
{
	return OPEN_MODE;
}

int atomic_rename(const char *oldpath, const char *newpath)
{
	int ret = 0;
	if (NULL == oldpath || NULL == newpath)
	{
		ret = -1;
		errno = EINVAL;
	}
	else
	{
		if (0 == (ret = ::link(oldpath, newpath)))
		{
			unlink(oldpath);
		}
	}
	return ret;
}

int64_t unintr_pwrite(const int fd, const void *buf, const int64_t count, const int64_t offset)
{
	int64_t length2write = count;
	int64_t offset2write = 0;
	int64_t write_ret = 0;
	while (length2write > 0)
	{
		write_ret = ::pwrite(fd, (char*)buf + offset2write, length2write, offset + offset2write);
		if (0 >= write_ret)
		{
			if (errno == EINTR) // 阻塞IO不需要判断EAGAIN
			{
				continue;
			}
			offset2write = -1;
			break;
		}
		length2write -= write_ret;
		offset2write += write_ret;
	}
	return offset2write;
}

int64_t unintr_pread(const int fd, void *buf, const int64_t count, const int64_t offset)
{
	int64_t length2read = count;
	int64_t offset2read = 0;
	int64_t read_ret= 0;
	while (length2read > 0)
	{
		read_ret = ::pread(fd, (char*)buf + offset2read, length2read, offset + offset2read);
		if (0 >= read_ret)
		{
			if (errno == EINTR) // 阻塞IO不需要判断EAGAIN
			{
				continue;
			}
			if (0 > read_ret)
			{
				offset2read = -1;
			}
			break;
        }
        offset2read += read_ret;
        if (length2read > read_ret)
        {
			break;
        }
        else
        {
			length2read -= read_ret;
        }
	}
	return offset2read;
}

bool is_align(const void *buf, const int64_t count, const int64_t offset, const int64_t align_size)
{
	bool bret = false;
	if (0 == (int64_t)buf % align_size && 0 == count % align_size && 0 == offset % align_size)
	{
		bret = true;
	}
	return bret;
}

int64_t pread_align(const int fd, void *buf, const int64_t count, const int64_t offset, void *align_buffer, const int64_t buffer_size, const int64_t align_size)
{
	int64_t ret = 0;
	int64_t offset2read = lower_align(offset, align_size);
	int64_t total2read = upper_align(offset + count, align_size) - offset2read;
	int64_t buffer_offset = offset - offset2read;
      
	int64_t size2read = 0;
	int64_t read_ret = 0;
	int64_t size2copy = 0;
	int64_t got_size = 0;
	while (total2read > 0 && ret < count)
	{
        // 每次多读一个align_size 但是不使用 保证可以确定是否到达文件末尾
        size2read = std::min(buffer_size + align_size, total2read);
        read_ret = unintr_pread(fd, align_buffer, size2read, offset2read);
        if (0 > read_ret)
        {
			ret = -1;
			break;
        }
        else if (read_ret <= buffer_offset)
        {
			// 读取的数据量小于等于用于对齐的数据 即用户指定的offset后没有数据了 break出来 
			break;
        }
        else
        {
			got_size = std::min(read_ret, buffer_size);
			size2copy = std::min(got_size - buffer_offset, count - ret);
			memcpy((char*)buf + ret, (char*)align_buffer + buffer_offset, size2copy);
			ret += size2copy;

			offset2read += got_size;
			total2read -= got_size;
			buffer_offset = 0;

			// 读取的数据量小于请求的 到文件末尾了 break出来
			if (size2read > read_ret)
			{
				break;
			}
		}
	}
	return ret;
}

int64_t pwrite_align(const int fd, const void *buf, const int64_t count, const int64_t offset, void *align_buffer, const int64_t buffer_size, const int64_t align_size, int64_t &buffer_pos)
{
	int ret = 0;
	int64_t total2write = count;
	int64_t total_offset = offset;

	int64_t write_ret = 0;
	while (0 < total2write)
	{
		int64_t size2copy = std::min(buffer_size - buffer_pos, total2write);
		memcpy((char*)align_buffer + buffer_pos, (char*)buf + ret, size2copy);
		total2write -= size2copy;
        buffer_pos += size2copy;

        int64_t offset2write = lower_align(total_offset, align_size);
        int64_t size2write = upper_align(buffer_pos, align_size);
        memset((char*)align_buffer + buffer_pos, 0, buffer_size - buffer_pos);
        write_ret = unintr_pwrite(fd, align_buffer, size2write, offset2write);
        if (size2write != write_ret)
        {
			ret = -1;
			break;
        }
        total_offset += size2copy;
        ret += size2copy;
        if (buffer_pos == size2write)
        {
			buffer_pos = 0;
        }
	}

	int64_t size2reserve = buffer_pos - lower_align(buffer_pos, align_size);
	if (0 != size2reserve)
	{
		memmove(align_buffer, (char*)align_buffer + buffer_pos - size2reserve, size2reserve);
		buffer_pos = size2reserve;
	}
	else
	{
		buffer_pos = 0;
	}
	return ret;
}

int64_t get_file_size(const int fd)
{
	int64_t ret = -1;
	struct stat st;
	if (-1 != fd && 0 == fstat(fd, &st))
	{
		ret = st.st_size;
	}
	return ret;
}

int64_t get_file_size(const char *fname)
{
	int64_t ret = -1;
	struct stat st;
	if (NULL != fname && 0 == stat(fname, &st))
	{
		ret = st.st_size;
	}
	return ret;
}

}//end of namespace ha
}//end of namespace bladestore
