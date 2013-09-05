/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-5-10
 *
 */
#ifndef BLADESTORE_NAMESERVER_FILE_SYSTEM_IMAGE_H
#define BLADESTORE_NAMESERVER_FILE_SYSTEM_IMAGE_H

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string>
#include <ext/hash_map>
#include <errno.h>


namespace bladestore
{
namespace nameserver
{

class BladeLayoutManager;
class LeaseManager;

class FileSystemImage
{
public:
	FileSystemImage(BladeLayoutManager * layout_manager);

	virtual ~FileSystemImage();

	int save_image(char * filename);

	int recovery_from_image(char * filename);

	void set_lease_manager(LeaseManager * lease_manager);

private:
	static const int64_t IMAGE_FLAG = 0x49534544414c42; //BLADESI
	mutable int32_t read_fd_;
	mutable int32_t write_fd_;

	//元数据信息
	struct ImageHeader
	{
		int64_t flag_;
		int32_t version_;
		int64_t server_count_;//当前系统DS数量
		int64_t block_count_;//当前系统block数量
		int64_t lease_count_;//lease map 中lease的数量
	};

	mutable ImageHeader header_;
	BladeLayoutManager * layout_manager_;
	LeaseManager * lease_manager_;
};

}//end of namesapce nameserver
}//end of namespace bladestore

#endif
