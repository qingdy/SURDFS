#include <string.h>
#include <sys/vfs.h>

#include "utility.h"
#include "blade_common_define.h"

namespace bladestore
{ 
namespace common 
{

void hex_dump(const void* data, const int32_t size, const bool char_type /*= true*/, const int32_t log_level /*LL_DEBUG*/)
{

	/* dumps size bytes of *data to stdout. Looks like:
	 * [0000] 75 6E 6B 6E 6F 77 6E 20
	 * 30 FF 00 00 00 00 39 00 unknown 0.....9.
	 * (in a single line of course)
	 */

	unsigned const char *p = (unsigned char*)data;
	unsigned char c = 0;
	int n = 0;
	char bytestr[4] = {0};
	char addrstr[10] = {0};
	char hexstr[ 16*3 + 5] = {0};
	char charstr[16*1 + 5] = {0};

	for(n = 1; n <= size; n++) 
	{
		if (n%16 == 1) 
		{
			/* store address for this line */
			snprintf(addrstr, sizeof(addrstr), "%.4x", (int)((unsigned long)p-(unsigned long)data) );
		}

		c = *p;
		if (isprint(c) == 0) 
		{
			c = '.';
		}

		/* store hex str (for left side) */
		snprintf(bytestr, sizeof(bytestr), "%02X ", *p);
		strncat(hexstr, bytestr, sizeof(hexstr)-strlen(hexstr)-1); 

		/* store char str (for right side) */
		snprintf(bytestr, sizeof(bytestr), "%c", c);
		strncat(charstr, bytestr, sizeof(charstr)-strlen(charstr)-1);

		if (n % 16 == 0)
		{ 
			/* line completed */
			if (char_type) 
			{
			//	LOGV(log_level, "[%ld] [%4.4s]   %-50.50s  %s\n", pthread_self(), addrstr, hexstr, charstr);
			}
			else 
			{
				LOGV(log_level, "[%ld] [%4.4s]   %-50.50s\n", pthread_self(), addrstr, hexstr);
			}
			hexstr[0] = 0;
			charstr[0] = 0;
		} 
		else if(n % 8 == 0) 
		{
			/* half line: add whitespaces */
			strncat(hexstr, "  ", sizeof(hexstr)-strlen(hexstr)-1);
			strncat(charstr, " ", sizeof(charstr)-strlen(charstr)-1);
		}
		p++; /* next byte */
	}

	if (strlen(hexstr) > 0) 
	{
		/* print rest of buffer if not empty */
		if (char_type) 
		{
			//LOGV(log_level, "[%ld] [%4.4s]   %-50.50s  %s\n", pthread_self(), addrstr, hexstr, charstr);
		}
		else 
		{
			LOGV(log_level, "[%ld] [%4.4s]   %-50.50s\n", pthread_self(), addrstr, hexstr);
		}
	}
}

int32_t parse_string_to_int_array(const char* line, const char del, int32_t *array, int32_t& size) 
{
	int ret = 0;
	const char* start = line;
	const char* p = NULL;
	char buffer[BLADE_MAX_ROW_KEY_LENGTH];

	if (NULL == line || NULL == array || size <= 0)
	{
		ret = BLADE_INVALID_ARGUMENT;
	}

	int32_t idx = 0;
	if (BLADE_SUCCESS == ret)
	{
		while (NULL != start) 
		{
			p = strchr(start, del);
			if (NULL != p) 
			{
				memset(buffer, 0, BLADE_MAX_ROW_KEY_LENGTH);
				strncpy(buffer, start, p - start);
				if (strlen(buffer) > 0) 
				{
					if (idx >= size) 
					{
						ret = BLADE_SIZE_OVERFLOW;
						break;
					}
					else
					{
						array[idx++] = strtol(buffer, NULL, 10);
					}
				}
				start = p + 1;
			} 
			else 
			{
				memset(buffer, 0, BLADE_MAX_ROW_KEY_LENGTH);
				strcpy(buffer, start);
				if (strlen(buffer) > 0) 
				{
					if (idx >= size) 
					{
						ret = BLADE_SIZE_OVERFLOW;
						break;
					}
					else
					{
						array[idx++] = strtol(buffer, NULL, 10);
					}
				}
				break;
			}
		}

		if (BLADE_SUCCESS == ret) 
		{
			size = idx;
		}
	}
	return ret;
}

int32_t hex_to_str(const void* in_data, const int32_t data_length, void* buff, const int32_t buff_size)
{
	unsigned const char *p = NULL;
	int32_t i = 0;
	if (in_data != NULL && buff != NULL && buff_size >= data_length * 2)
	{
		p = (unsigned const char *)in_data;
		for (; i < data_length; i++)
		{
			sprintf((char*)buff + i * 2, "%02X", *(p + i));
		}
	}
	return i;
}

int32_t str_to_hex(const void* in_data, const int32_t data_length, void* buff, const int32_t buff_size)
{
	unsigned const char *p = NULL;
	unsigned char *o = NULL;
	unsigned char c;
	int32_t i = 0;
	if (in_data != NULL && buff != NULL && buff_size >= data_length / 2)
	{
		p = (unsigned const char *)in_data;
		o = (unsigned char *)buff;
		c = 0;
		for (i = 0; i < data_length; i++)
		{   
			c = c << 4;
			if (*(p + i) > 'F' || *(p + i) < 'A' && *(p + i) > '9' || *(p + i) < '0')
			{
				break;
			}

			if (*(p + i) >= 'A')
			{
				c += *(p + i) - 'A' + 10; 
			}
			else
			{
				c += *(p + i) - '0';
			}

			if (i % 2 == 1)
			{   
				*(o + i / 2) = c;
				c = 0;
			}   
		}   
	}
	return i;
}

int64_t lower_align(int64_t input, int64_t align)
{
	int64_t ret = input;
	ret = (input + align - 1) & ~(align - 1);
	ret = ret - ((ret - input + align - 1) & ~(align - 1));
	return ret;
}

int64_t upper_align(int64_t input, int64_t align)
{
	int64_t ret = input;
	ret = (input + align - 1) & ~(align - 1);
	return ret;
}

bool is2n(int64_t input)
{
	return (((~input + 1) & input) == input);
}

bool all_zero(const char *buffer, const int64_t size)
{
	bool bret = true;
	const char *buffer_end = buffer + size;
	const char *start = (char*)upper_align((int64_t)buffer, sizeof(int64_t));
	start = std::min(start, buffer_end);
	const char *end = (char*)lower_align((int64_t)(buffer + size), sizeof(int64_t));
	end = std::max(end, buffer);

	bret = all_zero_small(buffer, start - buffer);
	if (bret)
	{
		bret = all_zero_small(end, buffer + size - end);
	}
	if (bret)
	{
		const char *iter = start;
		while (iter < end)
		{
			if (0 != *((int64_t*)iter))
			{
				bret = false;
				break;
			}
			iter += sizeof(int64_t);
		}
	}
	return bret;
}

bool all_zero_small(const char *buffer, const int64_t size)
{
	bool bret = true;
	if (NULL != buffer)
	{
		for (int64_t i = 0; i < size; i++)
		{
			if (0 != *(buffer + i))
			{
				bret = false;
				break;
			}
		}
	}
	return bret;
}

char* str_trim(char *str)
{
	char *p = str, *sa = str;
	while (*p) 
	{
		if(*p!=' ')
		{
			*str++ = *p;
		}
		p++;
	}
	*str = 0;
	return sa;
}

bool split_string(const string path, const char dlim, string &dir, string &filename)
{
    int npos = -1; 
    if((npos = path.find_last_of(dlim)) < 0)
    {   
        return false;
    }
    if(npos == 0)
    {   
        dir = path.substr(0, 1);
    }   
    else
    {    
        dir = path.substr(0, npos);
    }   
    if((npos+1) != path.size())
    {   
        filename = path.substr(npos+1);
    } 
    else
    {
        filename = "";
    }  
    return true;
}         

// disk usage
int GetDiskUsage(const char* path, int64_t* used_bytes, int64_t* total_bytes)
{   
	*used_bytes = 1;
	*total_bytes = 1;
	struct statfs buf;
	if (statfs(path, &buf) != 0)
	{   
		return BLADE_FILESYSTEM_ERROR;
	}   
	*used_bytes = (int64_t)(buf.f_blocks - buf.f_bfree) * buf.f_bsize;
	*total_bytes = (int64_t)(buf.f_blocks) * buf.f_bsize;
	return BLADE_SUCCESS;
}

// get avg load in one minute
int get_load_avg()
{   
	int l = 0;
	double d;
	if (getloadavg(&d, 1) == 1)
	{   
		l = (int) (d * 100);
		if (l < 10) 
			l = 10; 
	}   
	return l;
}

bool CheckPathLength(const string& path)
{
    if(path.size() > MAX_PATH_LENGTH)
        return false;

    string::size_type pos = 0;
    int32_t depth = 0;
    while((pos = path.find_first_of("/", pos)) != string::npos) {
        depth++;
        if(depth > MAX_PATH_DEPTH)
            return false;

        pos++;
    }
    return true;
}

}//end of namespace common
}//end of namespace bladestore

