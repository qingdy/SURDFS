#include <stdint.h>
#include "blade_thread_buffer.h"
#include "log.h"

namespace
{
	const pthread_key_t INVALID_THREAD_KEY = -1;
}

namespace bladestore
{ 
namespace common
{

ThreadSpecificBuffer::ThreadSpecificBuffer(const int32_t size) : key_(INVALID_THREAD_KEY), size_(size)
{
	create_thread_key();
}

ThreadSpecificBuffer::~ThreadSpecificBuffer()
{
	delete_thread_key();
}

int ThreadSpecificBuffer::create_thread_key()
{
	int ret = pthread_key_create(&key_, destroy_thread_key);
	if (0 != ret)
	{
		LOGV(LL_ERROR, "cannot create thread key:%d", ret);
    }
    return (0 == ret) ? BLADE_SUCCESS : BLADE_ERROR;
}

int ThreadSpecificBuffer::delete_thread_key()
{
	int ret = -1;
	if (INVALID_THREAD_KEY != key_)
	{
		ret = pthread_key_delete(key_);
	}
	if (0 != ret)
	{
		LOGV(LL_ERROR,"delete thread key key_ failed.");
    }
	return (0 == ret) ? BLADE_SUCCESS : BLADE_ERROR;
}

void ThreadSpecificBuffer::destroy_thread_key(void* ptr)
{
	LOGV(LL_INFO, "delete thread specific ptr:%p", ptr);
	if (NULL != ptr) 
		free(ptr);
}

ThreadSpecificBuffer::Buffer* ThreadSpecificBuffer::get_buffer() const
{
	Buffer * buffer = NULL;
	if (INVALID_THREAD_KEY != key_ && size_ > 0)
	{
		void* ptr = pthread_getspecific(key_);
		if (NULL == ptr)
		{
			ptr = (void *)malloc(size_ + sizeof(Buffer));
			if (NULL != ptr)
			{
				int ret = pthread_setspecific(key_, ptr);
				if (0 != ret) 
				{
					LOGV(MSG_ERROR, "pthread_setspecific failed:%d", ret);
					free(ptr);
					ptr = NULL;
				}
				else
				{
					 buffer = new (ptr) Buffer(static_cast<char*>(ptr) + sizeof(Buffer), size_);
				}

			}
			else
			{
				// malloc failed;
				LOGV(LL_ERROR, "malloc thread specific memeory failed.");
			}
		}
		else
		{
      		// got exist ptr;
			buffer = reinterpret_cast<Buffer*>(ptr);
		}
	}
	else
	{
		LOGV(LL_ERROR, "thread key must be initialized and size must great than zero, key:%u,size:%d", key_, size_);
	}
	return buffer;
}

int ThreadSpecificBuffer::Buffer::advance(const int32_t size)
{
	int ret = BLADE_SUCCESS;
	if (size < 0)
  	{
    	if (end_ + size < start_)
    	{
			ret = BLADE_ERROR;
		}
	}
	else
	{
		if (end_ + size > end_of_storage_)
		{
			ret = BLADE_ERROR;
		}
	}

  	if (BLADE_SUCCESS == ret) 
	{
		end_ += size;
	}

	assert(end_ >= start_ && end_ <= end_of_storage_);
	return ret;
}

void ThreadSpecificBuffer::Buffer::reset() 
{
	end_ = start_;
}

int ThreadSpecificBuffer::Buffer::write(const char* bytes, const int32_t size)
{
	int ret = BLADE_SUCCESS;
	if (NULL == bytes || size < 0)
	{
		ret = BLADE_ERROR;
	}
	else
	{
		if (size > remain()) 
			ret = BLADE_ERROR;
    	else
    	{
			::memcpy(end_, bytes, size);
			advance(size);
    	}
	}
  return ret;
}

} // end namespace common
} // end namespace bladestore




