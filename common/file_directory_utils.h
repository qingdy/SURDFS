/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-5-4
 *
 */

#ifndef BLADESTORE_COMMON_FILE_DIRECTORY_UTILS_H
#define BLADESTORE_COMMON_FILE_DIRECTORY_UTILS_H

#include <string>
#include <vector>

namespace bladestore
{
namespace common
{

#ifndef S_IRWXUGO
# define S_IRWXUGO (S_IRWXU | S_IRWXG | S_IRWXO)
#endif

class FileDirectoryUtils
{
public:
	static const int MAX_PATH = 512;
	static bool exists (const char *filename);
	static bool is_directory (const char *dirname);
	static bool create_directory (const char *dirname);
	static bool create_full_path (const char *fullpath);
	static bool delete_file (const char *filename);
	static bool delete_directory (const char *dirname);
	static bool delete_directory_recursively (const char *directory);
	static bool rename (const char *srcfilename, const char *destfilename);
	static int64_t get_size (const char *filename);

	static int vsystem(const char* cmd);
	static int cp(const char* src_path, const char* src_name, const char* dst_path, const char* dst_name);
	static int cp_safe(const char* src_path, const char* src_name, const char* dst_path, const char* dst_name);
	static int mv(const char* src_path, const char* src_name, const char* dst_path, const char* dst_name);
	static int rm(const char* path, const char* name);
};

typedef FileDirectoryUtils FSU;

}//end of namespace common
}//end of namespace bladestore
#endif

