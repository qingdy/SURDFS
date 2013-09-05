extern "C" {
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
}

#include <iostream>
#include <sstream>
#include <cstdio>
#include <cstdlib>
#include <cerrno>
#include "util.h"

using namespace std;

namespace bladestore
{
namespace btree
{

int64_t chunk_start_offset(int64_t offset)
{
	return (offset / BLADE_BLOCK_SIZE) * BLADE_BLOCK_SIZE;
}

const string to_string(long long n)
{
	std::ostringstream s(std::ostringstream::out);

	s << n;
	return s.str();
}

long long to_number(string s)
{
	char *endptr;
	long long n = strtoll(s.c_str(), &endptr, 10);
	if (*endptr != '\0')
	{
		n = -1;
	}
	return n;
}

void Split(deque <string> &component, const string path, char sep)
{
	string::size_type start = 0;
	string::size_type slash = 0;

	while (slash != string::npos) 
	{
		assert(slash == 0 || path[slash] == sep);
		slash = path.find(sep, start);
		string nextc = path.substr(start, slash - start);
		start = slash + 1;
		component.push_back(nextc);
	}
}

bool file_exists(string name)
{
	struct stat s;
	if (-1 == stat(name.c_str(), &s))
	{
		return false;
	}

	return S_ISREG(s.st_mode);
}

float compute_time_diff(const struct timeval &startTime, const struct timeval &endTime)
{
	float timeSpent;
	
	timeSpent = (endTime.tv_sec * 1e6 + endTime.tv_usec) - (startTime.tv_sec * 1e6 + startTime.tv_usec);
	return timeSpent / 1e6;
}

}//end of namespace btree
}//end of namsspace bladestore
