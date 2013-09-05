#include "blade_crc.h"
#include "log.h"
namespace bladestore
{
namespace common
{
//crc是初始值,
uint32_t Func::crc(uint32_t crc, const char* data, const int32_t len)
{   
	int32_t i;
	for (i = 0; i < len; ++i)
	{   
		crc = (crc >> 8) ^ crc32_tab[(crc ^ (*data)) & 0xff];
		data++;
	}

	return (crc);
} 

bool Func::crc_generate(const  char * data, char * checksum, int32_t len, int32_t start_paticial_chunk_length)
{   
    uint32_t crc ;
    if( 0 >= len || len <start_paticial_chunk_length)
        return false;
    if (0 != start_paticial_chunk_length) {
        crc = Func::crc(0, data, start_paticial_chunk_length);
        checksum[3] = (unsigned char)crc;
        crc >>= 8;
        checksum[2] = (unsigned char)crc;
        crc >>= 8;
        checksum[1] = (unsigned char)crc;
        crc >>= 8;
        checksum[0] = (unsigned char)crc;
        //LOGV(LL_INFO, "crc: %d checksum:%s\n",len,checksum);
        checksum += 4 ;
        len -= start_paticial_chunk_length;
        data += start_paticial_chunk_length; 
    }
    if( 0 == len)
        return true;
    do {
        if(len < BLADE_CHUNK_SIZE)
            crc = Func::crc(0,data,len);
        else
            crc = Func::crc(0,data,BLADE_CHUNK_SIZE);

        checksum[3] = (unsigned char)crc;
        crc >>= 8;
        checksum[2] = (unsigned char)crc;
        crc >>= 8;
        checksum[1] = (unsigned char)crc;
        crc >>= 8;
        checksum[0] = (unsigned char)crc;
        //LOGV(LL_INFO, "crc: %d checksum:%s\n",len,checksum);
        checksum += 4 ;
        len -= BLADE_CHUNK_SIZE;
        data += BLADE_CHUNK_SIZE; 
    }while(len > 0);
    return true;
}

bool Func::crc_generate(const char * data, char * checksum, int32_t len)
{
    return Func::crc_generate(data, checksum, len, 0);
}


bool  Func::crc_check(const char * data, const char* checksum, int32_t len, int32_t start_paticial_chunk_length)
{
    uint32_t crc = 0;
    if (start_paticial_chunk_length > 0) {
        crc = Func::crc(0, data, start_paticial_chunk_length);        
        for (int8_t i=3; i >= 0; i--){
            if (checksum[i] != (char)crc){
                //LOGV(LL_INFO, "crc  error: i:%d,len:%d.\n", i, len);
                return false;
            }
           // LOGV(LL_INFO, "crcok: %d \n",i);
            crc >>= 8;
        }
        checksum += 4;
        data += start_paticial_chunk_length; 
        len -= start_paticial_chunk_length;
    }
    while(len >0){
        if (len < BLADE_CHUNK_SIZE)
            crc = Func::crc(0, data, len);
        else
            crc = Func::crc(0, data, BLADE_CHUNK_SIZE);
        for (int8_t i=3; i >= 0; i--) {
            if (checksum[i] != (char)crc) {
                //LOGV(LL_INFO, "crc normal error,i:%d,len:%d.\n", i, len);
                return false;
            }
            //LOGV(LL_INFO, "crc ok:,i:%d,len:%d.\n", i, len);
            crc >>= 8;
        }
        checksum += 4;
        data += BLADE_CHUNK_SIZE; 
        len -= BLADE_CHUNK_SIZE;
    }
    return true;
}

bool  Func::crc_check(const char * data, const char* checksum, int32_t len)
{
   return Func::crc_check(data, checksum, len, 0);
}

bool Func::crc_update(char * checksum_origin, int32_t checksum_offset, const char* data, int32_t data_offset, int32_t len)
{
	uint32_t crc = (unsigned char)checksum_origin[checksum_offset];
	crc <<= 8;
	crc |= (unsigned char)checksum_origin[checksum_offset + 1];
	crc <<= 8;
	crc |= (unsigned char)checksum_origin[checksum_offset + 2];
	crc <<= 8;
	crc |= (unsigned char)checksum_origin[checksum_offset + 3];

    crc = Func::crc(crc, data + data_offset, len);

    checksum_origin[checksum_offset + 3] = (unsigned char)crc;
    crc >>= 8;
    checksum_origin[checksum_offset + 2] = (unsigned char)crc;
    crc >>= 8;
    checksum_origin[checksum_offset + 1] = (unsigned char)crc;
    crc >>= 8;
    checksum_origin[checksum_offset] = (unsigned char)crc;
    return true;
}

}//end of namespace common
}//end of namespace bladestore
