#include <set>
#include <stdint.h>

#include "file_system_image.h"
#include "blade_block_collect.h"
#include "blade_dataserver_info.h"
#include "blade_nameserver_param.h"
#include "layout_manager.h"
#include "blade_common_define.h"
#include "lease_manager.h"

using namespace pandora;
using namespace bladestore::common;
using namespace std;

namespace bladestore
{
namespace nameserver
{

//用来序列化layout的相关信息

FileSystemImage::FileSystemImage(BladeLayoutManager * layout_manager) : read_fd_(-1), write_fd_(-1)
{
	layout_manager_ = layout_manager;
	::memset((char*) &header_, 0, sizeof(header_));
}

FileSystemImage::~FileSystemImage()
{

}

void FileSystemImage::set_lease_manager(LeaseManager * lease_manager)
{
	lease_manager_ = lease_manager;
}


int FileSystemImage::save_image(char * filename)
{
	write_fd_ = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (write_fd_ == -1)
	{
		LOGV(LL_WARN, "can't open image file write, '%s'", filename);
		return BLADE_READ_FILE_ERROR;
	}
//	reverse space for ImageHeader
	memset(&header_, 0, sizeof(ImageHeader));
	header_.flag_ = IMAGE_FLAG;
	header_.version_= 1;
	if (write(write_fd_, &header_, sizeof(ImageHeader)) != sizeof(ImageHeader))
	{
		LOGV(LL_ERROR, "write header fail");
		close(write_fd_);
		write_fd_ = -1;
		return BLADE_WRITE_FILE_ERROR;
	}

//	save blocks map
	layout_manager_->blocks_mutex_.rlock()->lock();

	BladeBlockCollect * block_collect = NULL;
	BLOCKS_MAP_ITER blocks_map_iter = layout_manager_->blocks_map_.begin();
	for ( ; blocks_map_iter != layout_manager_->blocks_map_.end(); blocks_map_iter++)
	{
		block_collect = blocks_map_iter->second;
		if (NULL == block_collect)
		{
			continue;
		}

		if (write(write_fd_, &(block_collect->block_id_), sizeof(int64_t)) != sizeof(int64_t))
		{
			LOGV(LL_ERROR, "write block_collect fail");
			close(write_fd_);
			write_fd_ = -1;
			return BLADE_WRITE_FILE_ERROR;
		}

		int64_t ds_set_size = block_collect->dataserver_set_.size();	
		if (write(write_fd_, &(ds_set_size), sizeof(int64_t)) != sizeof(int64_t))
		{
			LOGV(LL_ERROR, "write block_collect fail");
			close(write_fd_);
			write_fd_ = -1;
			return BLADE_WRITE_FILE_ERROR;
		}

		typedef std::set<uint64_t>::iterator DS_ITER;
		uint64_t ds_id;
		DS_ITER ds_iter;
		for(ds_iter = block_collect->dataserver_set_.begin(); ds_iter != block_collect->dataserver_set_.end(); ds_iter++)
		{
			ds_id = *ds_iter;
			if (write(write_fd_, &(ds_id), sizeof(uint64_t)) != sizeof(uint64_t))
			{
				LOGV(LL_ERROR, "write block_collect fail");
				close(write_fd_);
				write_fd_ = -1;
				return BLADE_WRITE_FILE_ERROR;
			}
		}
	
		if (write(write_fd_, &(block_collect->version_), sizeof(int32_t)) != sizeof(int32_t))
		{
			LOGV(LL_ERROR, "write block_collect fail");
			close(write_fd_);
			write_fd_ = -1;
			return BLADE_WRITE_FILE_ERROR;
		}

		if (write(write_fd_, &(block_collect->replicas_number_), sizeof(int16_t)) != sizeof(int16_t))
		{
			LOGV(LL_ERROR, "write block_collect fail");
			close(write_fd_);
			write_fd_ = -1;
			return BLADE_WRITE_FILE_ERROR;
		}
	
		header_.block_count_ += 1;
	}
	layout_manager_->blocks_mutex_.rlock()->unlock();

//	save server map
	layout_manager_->server_mutex_.rlock()->lock();

	DataServerInfo * dataserver_info = NULL;
	SERVER_MAP_ITER server_map_iter = layout_manager_->server_map_.begin();
	for( ; server_map_iter != layout_manager_->server_map_.end(); server_map_iter++)
	{
		dataserver_info = server_map_iter->second;
		if (NULL == dataserver_info)
		{	
			continue;
		}
		
		if (write(write_fd_, &(dataserver_info->dataserver_metrics_), sizeof(dataserver_info->dataserver_metrics_)) != sizeof(dataserver_info->dataserver_metrics_))
		{
			LOGV(LL_ERROR, "write dataserver_info fail");
			close(write_fd_);
			write_fd_ = -1;
			return BLADE_WRITE_FILE_ERROR;
		}

		if (write(write_fd_, &(dataserver_info->load_score_), sizeof(double)) != sizeof(double))
		{
			LOGV(LL_ERROR, "read dataserver_info fail");
			close(write_fd_);
			write_fd_ = -1;
			return BLADE_WRITE_FILE_ERROR;
		}
		
		int64_t block_count = dataserver_info->blocks_hold_.size();
		if (write(write_fd_, &(block_count), sizeof(int64_t)) != sizeof(int64_t))
		{
			LOGV(LL_ERROR, "write dataserver_info fail");
			close(write_fd_);
			write_fd_ = -1;
			return BLADE_WRITE_FILE_ERROR;
		}

		typedef std::set<int64_t>::iterator BLOCK_ITER;
		int64_t block_id;
		BLOCK_ITER iter = dataserver_info->blocks_hold_.begin();
		for(int i = 0; iter != dataserver_info->blocks_hold_.end(); i++, iter++)
		{
			block_id = *iter;	
			if (write(write_fd_, &block_id, sizeof(int64_t)) != sizeof(int64_t))
			{
				LOGV(LL_ERROR, "write dataserver_info fail");
				close(write_fd_);
				write_fd_ = -1;
				return BLADE_WRITE_FILE_ERROR;
			}
		}

		if (write(write_fd_, &(dataserver_info->dataserver_id_), sizeof(uint64_t)) != sizeof(uint64_t))
		{
			LOGV(LL_ERROR, "write dataserver_info fail");
			close(write_fd_);
			write_fd_ = -1;
			return BLADE_WRITE_FILE_ERROR;
		}

		if (write(write_fd_, &(dataserver_info->rack_id_), sizeof(int32_t)) != sizeof(int32_t))
		{
			LOGV(LL_ERROR, "write dataserver_info fail");
			close(write_fd_);
			write_fd_ = -1;
			return BLADE_WRITE_FILE_ERROR;
		}

		if (write(write_fd_, &(dataserver_info->is_alive_), sizeof(bool)) != sizeof(bool))
		{
			LOGV(LL_ERROR, "write dataserver_info fail");
			close(write_fd_);
			write_fd_ = -1;
			return BLADE_WRITE_FILE_ERROR;
		}

		header_.server_count_ += 1;
	}

	layout_manager_->server_mutex_.rlock()->unlock();
	
	//save lease map
	lease_manager_->get_lease_mutex().rlock()->lock();
	LEASE_MAP_ITER iter = lease_manager_->lease_map_.begin();	
	for(; iter != lease_manager_->lease_map_.end(); iter++)
	{
		if (write(write_fd_, &(iter->first), sizeof(int64_t)) != sizeof(int64_t))
		{
			LOGV(LL_ERROR, "write lease fail");
			close(write_fd_);
			write_fd_ = -1;
			return BLADE_WRITE_FILE_ERROR;
		}
		if (write(write_fd_, &(iter->second.file_id_), sizeof(int64_t)) != sizeof(int64_t))
		{
			LOGV(LL_ERROR, "write lease fail");
			close(write_fd_);
			write_fd_ = -1;
			return BLADE_WRITE_FILE_ERROR;
		}
		if (write(write_fd_, &(iter->second.block_version_), sizeof(int32_t)) != sizeof(int32_t))
		{
			LOGV(LL_ERROR, "write lease fail");
			close(write_fd_);
			write_fd_ = -1;
			return BLADE_WRITE_FILE_ERROR;
		}
		if (write(write_fd_, &(iter->second.is_safe_write_), sizeof(bool)) != sizeof(bool))
		{
			LOGV(LL_ERROR, "write lease fail");
			close(write_fd_);
			write_fd_ = -1;
			return BLADE_WRITE_FILE_ERROR;
		}
		header_.lease_count_ += 1;	
	}
	lease_manager_->get_lease_mutex().rlock()->unlock();

	//rewrite the header
	if (lseek(write_fd_, SEEK_SET, 0) == -1)
	{
		LOGV(LL_ERROR, "write block fail");
		close(write_fd_);
		write_fd_ = -1;
		return BLADE_FILE_OP_ERROR;
	}

	if (write(write_fd_, &header_, sizeof(ImageHeader)) != sizeof(ImageHeader))
	{
		LOGV(LL_ERROR, "write header fail");
		close(write_fd_);
		write_fd_ = -1;
		return BLADE_WRITE_FILE_ERROR;
	}

	close(write_fd_);
	LOGV(LL_INFO, "fsimage saved (%s)", filename);
	return BLADE_SUCCESS;
}

int FileSystemImage::recovery_from_image(char * filename)
{
	if (access(filename, R_OK) == 0)
	{
		read_fd_ = ::open(filename, O_RDONLY);

		if (-1 == read_fd_)
		{
			LOGV(LL_WARN, "can't open image file '%s'", filename);
			return BLADE_OPEN_FILE_ERROR;
		}

		//prepare for read
		if (lseek(read_fd_, SEEK_SET, 0) == -1)
		{
			LOGV(LL_ERROR, "read block fail");
			close(read_fd_);
			read_fd_ = -1;
			return BLADE_FILE_OP_ERROR;
		}

		if (::read(read_fd_, &header_, sizeof(ImageHeader)) != sizeof(ImageHeader))
		{
			LOGV(LL_ERROR, "read fail");
			::close(read_fd_);
			return BLADE_READ_FILE_ERROR;
		}

		if (header_.flag_ != IMAGE_FLAG)
		{
			LOGV(LL_ERROR, "file format is error");
			::close(read_fd_);
			return BLADE_FILE_FORMAT_ERROR;
		}

		//read BladeBlockCollect 
		BladeBlockCollect * block_collect = NULL;
		LOGV(LL_DEBUG, "FSIMAGE BLOCK COUNT : %d", header_.block_count_);
		for (int64_t i = 0; i < header_.block_count_; ++i)
		{
			block_collect = new BladeBlockCollect();
			if (NULL == block_collect)
			{
				return BLADE_ALLOC_ERROR;
			}

			if (::read(read_fd_, &(block_collect->block_id_), sizeof(int64_t)) != sizeof(int64_t))
			{
				LOGV(LL_ERROR, "read block_collect fail");
				close(read_fd_);
				read_fd_ = -1;
				return BLADE_READ_FILE_ERROR;
			}

			int64_t ds_set_size; 
			if (::read(read_fd_, &(ds_set_size), sizeof(int64_t)) != sizeof(int64_t))
			{
				LOGV(LL_ERROR, "read block_collect fail");
				close(read_fd_);
				read_fd_ = -1;
				return BLADE_READ_FILE_ERROR;
			}

			uint64_t ds_id;
			for(int i = 0; i < ds_set_size; i++)
			{
				if (::read(read_fd_, &(ds_id), sizeof(uint64_t)) != sizeof(uint64_t))
				{
					LOGV(LL_ERROR, "read block_collect fail");
					close(read_fd_);
					read_fd_ = -1;
					return BLADE_READ_FILE_ERROR;
				}
				block_collect->dataserver_set_.insert(ds_id);
			}

			if (::read(read_fd_, &(block_collect->version_), sizeof(int32_t)) != sizeof(int32_t))
			{
				LOGV(LL_ERROR, "read block_collect fail");
				close(read_fd_);
				read_fd_ = -1;
				return BLADE_READ_FILE_ERROR;
			}

			if (::read(read_fd_, &(block_collect->replicas_number_), sizeof(int16_t)) != sizeof(int16_t))
			{
				LOGV(LL_ERROR, "read block_collect fail");
				close(read_fd_);
				read_fd_ = -1;
				return BLADE_READ_FILE_ERROR;
			}

			// TODO  check insert success?
			bool succ = layout_manager_->Insert(block_collect, true);
			LOGV(LL_DEBUG, "INSERT BLOCK succ : %d", succ);
		}

		//read DataServerInfo && rebuild rack_map_
		LOGV(LL_DEBUG, "FSIMAGE BLOCK COUNT : %d", header_.server_count_);
		DataServerInfo * dataserver_info = NULL;
		for (int64_t i = 0; i < header_.server_count_; ++i)
		{
			dataserver_info = new DataServerInfo();
			if (NULL == dataserver_info)
			{
				return BLADE_ALLOC_ERROR;
			}

			if (::read(read_fd_, &(dataserver_info->dataserver_metrics_), sizeof(dataserver_info->dataserver_metrics_)) != sizeof(dataserver_info->dataserver_metrics_))
			{
				LOGV(LL_ERROR, "read dataserver_info fail");
				close(read_fd_);
				read_fd_ = -1;
				return BLADE_READ_FILE_ERROR;
			}

			if (::read(read_fd_, &(dataserver_info->load_score_), sizeof(double)) != sizeof(double))
			{
				LOGV(LL_ERROR, "read dataserver_info fail");
				close(read_fd_);
				read_fd_ = -1;
				return BLADE_READ_FILE_ERROR;
			}

			int64_t block_count;
			if (::read(read_fd_, &(block_count), sizeof(int64_t)) != sizeof(int64_t))
			{
				LOGV(LL_ERROR, "read dataserver_info fail");
				close(read_fd_);
				read_fd_ = -1;
				return BLADE_READ_FILE_ERROR;
			}

			for (int i = 0; i < block_count; i++)
			{
				int64_t block_id;
				if (::read(read_fd_, &block_id, sizeof(int64_t)) != sizeof(int64_t))
				{
					LOGV(LL_ERROR, "read dataserver_info fail");
					close(read_fd_);
					read_fd_ = -1;
					return BLADE_READ_FILE_ERROR;
				}
				LOGV(LL_DEBUG, "INSERT BLOCK : %ld", block_id);
				dataserver_info->blocks_hold_.insert(block_id);
			}
			set<int64_t>::iterator iter = dataserver_info->blocks_hold_.begin();
			for (; iter != dataserver_info->blocks_hold_.end(); iter++)
			{
				LOGV(LL_DEBUG, "BLOCK ID IS : %ld", *iter);
			}

			if (::read(read_fd_, &(dataserver_info->dataserver_id_), sizeof(uint64_t)) != sizeof(uint64_t))
			{
				LOGV(LL_ERROR, "read dataserver_info fail");
				close(read_fd_);
				read_fd_ = -1;
				return BLADE_READ_FILE_ERROR;
			}

			if (::read(read_fd_, &(dataserver_info->rack_id_), sizeof(int32_t)) != sizeof(int32_t))
			{
				LOGV(LL_ERROR, "read dataserver_info fail");
				close(read_fd_);
				read_fd_ = -1;
				return BLADE_READ_FILE_ERROR;
			}

			if (::read(read_fd_, &(dataserver_info->is_alive_), sizeof(bool)) != sizeof(bool))
			{
				LOGV(LL_ERROR, "read dataserver_info fail");
				close(read_fd_);
				read_fd_ = -1;
				return BLADE_READ_FILE_ERROR;
			}
			dataserver_info->last_update_time_ = TimeUtil::GetTime() + 100000000;
			bool is_new;
			layout_manager_->join_ds(dataserver_info, is_new);
			delete dataserver_info;
			dataserver_info = NULL;
			LOGV(LL_DEBUG, "JOIN DS IS_NEW : %d", is_new);
		}

		//restore lease_map
		int64_t block_id;
		int64_t file_id;
		int32_t block_version;
		bool is_safe_write;
		for (int64_t i = 0; i < header_.lease_count_; i++)
		{
			if (::read(read_fd_, &(block_id), sizeof(int64_t)) != sizeof(int64_t))
			{
				LOGV(LL_ERROR, "read lease info fail");
				close(read_fd_);
				read_fd_ = -1;
				return BLADE_READ_FILE_ERROR;
			}
			if (read(read_fd_, &(file_id), sizeof(int64_t)) != sizeof(int64_t))
			{
				LOGV(LL_ERROR, "read lease info fail");
				close(read_fd_);
				read_fd_ = -1;
				return BLADE_READ_FILE_ERROR;
			}
			if (read(read_fd_, &(block_version), sizeof(int32_t)) != sizeof(int32_t))
			{
				LOGV(LL_ERROR, "read lease info fail");
				close(read_fd_);
				read_fd_ = -1;
				return BLADE_READ_FILE_ERROR;
			}
			if (read(read_fd_, &(is_safe_write), sizeof(bool)) != sizeof(bool))
			{
				LOGV(LL_ERROR, "read lease info fail");
				close(read_fd_);
				read_fd_ = -1;
				return BLADE_READ_FILE_ERROR;
			}
			lease_manager_->register_lease(file_id, block_id, block_version, is_safe_write);
		}
		close(read_fd_);
	}
	return BLADE_SUCCESS;
}

}//end of namespace nameserver
}//end of namespace bladestore
