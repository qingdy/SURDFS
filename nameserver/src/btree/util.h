/*
 *version  : 1.0
 *author   : chen yunyun
 *date     : 2012-4-18
 *
 */
#ifndef BLADESTORE_BTREE_UTIL_H
#define BLADESTORE_BTREE_UTIL_H

#include <string>
#include <deque>
#include <sstream>
#include "types.h"
#include "blade_common_define.h"

using std::string;
using std::deque;
using std::ostream;
using namespace bladestore::common;

namespace bladestore
{
namespace btree
{

int64_t chunk_start_offset(int64_t offset);

const string to_string(long long n);

long long to_number(const string s);

void Split(deque <string> &component, const string path, char sep);

bool file_exists(string s);

float compute_time_diff(const struct timeval &start, const struct timeval &end);

}//end of namespace btree
}//end of namespace bladestore
#endif
