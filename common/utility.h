/*
 *version  : 1.0
 *author   : chen yunyun
 *date     : 2012-5-2
 *
 */

#ifndef BLADESTORE_COMMON_UTILITY_H
#define BLADESTORE_COMMON_UTILITY_H

#include <stdint.h>
#include <string>

#include "log.h"

using namespace pandora;
using std::string;

#define STR_BOOL(b) ((b) ? "true" : "false")

namespace bladestore
{ 
namespace common 
{

void hex_dump(const void* data, const int32_t size, const bool char_type = true, const int32_t log_level = LL_DEBUG);
int32_t parse_string_to_int_array(const char* line, const char del, int32_t *array, int32_t& size);
int32_t hex_to_str(const void* in_data, const int32_t data_length, void* buff, const int32_t buff_size);
int32_t str_to_hex(const void* in_data, const int32_t data_length, void* buff, const int32_t buff_size);
int64_t lower_align(int64_t input, int64_t align);
int64_t upper_align(int64_t input, int64_t align);
bool is2n(int64_t input);
bool all_zero(const char *buffer, const int64_t size);
bool all_zero_small(const char *buffer, const int64_t size);
char* str_trim(char *str);
bool split_string(const string path, const char dlim, string &dir, string &filename);
int GetDiskUsage(const char* path, int64_t* used_bytes, int64_t* total_bytes); 
int get_load_avg();
bool CheckPathLength(const string& path);

}//end of namespace common
}//end of namespace bladestore

#endif

