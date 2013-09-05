/*
 *version : 1.0
 *author  : chenyunyun
 *date    : 2010-3-29
 */
#ifndef	BLADESTORE_COMMON_THREAD_BUFFER_H 
#define BLADESTORE_COMMON_THREAD_BUFFER_H 

#include <pthread.h>
#include <assert.h>
#include "blade_common_define.h"
namespace bladestore
{ 
namespace common
{

class ThreadSpecificBuffer
{
public:
	static const int32_t MAX_THREAD_BUFFER_SIZE = 4*1024*1024L; //最大8M

public:
	class Buffer
	{
	public:
		Buffer(char* start, const int32_t size) : end_of_storage_(start + size), end_(start)
 		{
 		}

	public:
		int write(const char* bytes, const int32_t size);
		int advance(const int32_t size);
		void reset() ;
		
		inline int32_t used() const 
		{ 
			return end_ - start_; 
		}
		inline int32_t remain() const 
		{
			return end_of_storage_ - end_; 
		}
		inline int32_t capacity() const 
		{
			return end_of_storage_ - start_; 
		}
		inline char* current() const 
		{
			return end_; 
		}
	private:
		char* end_of_storage_;
		char* end_;
		char start_[0];
	};

public:
	ThreadSpecificBuffer(const int32_t size = MAX_THREAD_BUFFER_SIZE);
	~ThreadSpecificBuffer();

	Buffer * get_buffer() const;

private:
	int create_thread_key();
	int delete_thread_key();
	static void destroy_thread_key(void* ptr);

	pthread_key_t key_;
	int32_t size_;
};

} // end of namespace common
} // end of namespace bladestore

#endif 


