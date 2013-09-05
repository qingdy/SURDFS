/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-6-12
 *
 */
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdint.h>

#include <gtest/gtest.h>

#include "blade_common_define.h"
#include "blade_serialization.h"

using namespace bladestore::common;

static const int64_t BUF_SIZE = 64;

void write_buf_to_file(const char *filename,const char *buf,int64_t size)
{
	if (NULL == buf || size <= 0 || NULL == filename)
		return;
	//  int fd = open(filename,O_RDWR|O_CREAT,0755);
	//  if (fd < 0)
	//    return;
	//  if (write(fd,buf,size) != size)
	//  {
	//    printf("write buf to file error\n");
	//  }
	//  close(fd);
	return;
}

TEST(serialization,int8)
{
	char *buf = static_cast<char *>(malloc(BUF_SIZE));
	int64_t pos = 0;

	for(int i=0;i<BUF_SIZE;++i)
	{
		ASSERT_EQ(serialization::encode_i8(buf,BUF_SIZE,pos,i),BLADE_SUCCESS);
	}
	write_buf_to_file("serialization_int8",buf,BUF_SIZE);
	ASSERT_EQ(pos,BUF_SIZE);
	pos = 0;
	for(int i=0;i<BUF_SIZE;++i)
	{
		int8_t t = 0;
		ASSERT_EQ(serialization::decode_i8(buf,BUF_SIZE,pos,&t),BLADE_SUCCESS);
		ASSERT_EQ(t,i);
	}

	uint8_t k = 255;
	pos = 0;
	ASSERT_EQ(serialization::encode_i8(buf,BUF_SIZE,pos,static_cast<int8_t>(k)),BLADE_SUCCESS);
	uint8_t v = 0;
	pos = 0;
	ASSERT_EQ(serialization::decode_i8(buf,BUF_SIZE,pos,reinterpret_cast<int8_t *>(&v)),BLADE_SUCCESS);
	ASSERT_EQ(v,k);
	ASSERT_EQ(pos,1);
	free(buf);

}

TEST(serialization,int16)
{
	char *buf = static_cast<char *>(malloc(BUF_SIZE));
	int64_t pos = 0;
	ASSERT_EQ(serialization::encode_i16(buf,BUF_SIZE,pos,20000),BLADE_SUCCESS);
	ASSERT_EQ(pos,2);
	write_buf_to_file("serialization_int16",buf,pos);
	pos = 0;
	int16_t t = 0;
	ASSERT_EQ(serialization::decode_i16(buf,BUF_SIZE,pos,&t),BLADE_SUCCESS);
	ASSERT_EQ(t,20000);
	ASSERT_EQ(pos,2);
	free(buf);
}

TEST(serialization,int32)
{
	char *buf = static_cast<char *>(malloc(BUF_SIZE));
	int64_t pos = 0;
	ASSERT_EQ(serialization::encode_i32(buf, BUF_SIZE, pos, 280813453),BLADE_SUCCESS);
	ASSERT_EQ(pos,4);
	write_buf_to_file("serialization_int32",buf,pos);
	pos = 0;
	int32_t t = 0;
	ASSERT_EQ(serialization::decode_i32(buf,BUF_SIZE,pos,&t),BLADE_SUCCESS);
	ASSERT_EQ(t,280813453);
	ASSERT_EQ(pos,4);
	free(buf);
}

TEST(serialization,int64)
{
	char *buf = static_cast<char *>(malloc(BUF_SIZE));
	int64_t pos = 0;
	ASSERT_EQ(serialization::encode_i64(buf,BUF_SIZE,pos,15314313412536),BLADE_SUCCESS);
	ASSERT_EQ(pos,8);
	write_buf_to_file("serialization_int64",buf,pos);
	pos = 0;
	int64_t t = 0;
	ASSERT_EQ(serialization::decode_i64(buf,BUF_SIZE,pos,&t),BLADE_SUCCESS);
	ASSERT_EQ(t,15314313412536); 
	ASSERT_EQ(pos,8);
	free(buf);
}

TEST(serialization,vint32)
{
	int64_t need_len = serialization::encoded_length_vi32(1311317890);
	need_len += serialization::encoded_length_vi32(1);
	char *buf = static_cast<char *>(malloc(need_len));
	assert(buf != NULL);
	int64_t pos = 0;
	ASSERT_EQ(serialization::encode_vi32(buf,need_len,pos,1311317890),BLADE_SUCCESS);
	ASSERT_EQ(serialization::encode_vi32(buf,need_len,pos,1),BLADE_SUCCESS);
	ASSERT_EQ(pos,need_len);
	write_buf_to_file("serialization_vint32",buf,pos);
	pos = 0;
	int32_t t = 0;
	ASSERT_EQ(serialization::decode_vi32(buf,need_len,pos,&t),BLADE_SUCCESS);
	ASSERT_EQ(t,1311317890); 
	ASSERT_EQ(serialization::decode_vi32(buf,need_len,pos,&t),BLADE_SUCCESS);
	ASSERT_EQ(t,1); 

	ASSERT_EQ(pos,need_len);
	free(buf);
}

TEST(serialization,_bool_)
{
	char *buf = static_cast<char *>(malloc(BUF_SIZE));
	int64_t pos = 0;
	ASSERT_EQ(serialization::encode_bool(buf,BUF_SIZE,pos,true),BLADE_SUCCESS);
	ASSERT_EQ(serialization::encode_bool(buf,BUF_SIZE,pos,false),BLADE_SUCCESS);
	bool val = false;
	ASSERT_EQ(serialization::encode_bool(buf,BUF_SIZE,pos,val),BLADE_SUCCESS);
	val = true;
	ASSERT_EQ(serialization::encode_bool(buf,BUF_SIZE,pos,val),BLADE_SUCCESS);
	ASSERT_EQ(pos,4);
	write_buf_to_file("serialization_bool",buf,pos);
	pos = 0;
	bool t;
	ASSERT_EQ(serialization::decode_bool(buf,BUF_SIZE,pos,&t),BLADE_SUCCESS);
	ASSERT_EQ(t,true);
	ASSERT_EQ(serialization::decode_bool(buf,BUF_SIZE,pos,&t),BLADE_SUCCESS);
	ASSERT_EQ(t,false);
	ASSERT_EQ(serialization::decode_bool(buf,BUF_SIZE,pos,&t),BLADE_SUCCESS);
	ASSERT_EQ(t,false);
	ASSERT_EQ(serialization::decode_bool(buf,BUF_SIZE,pos,&t),BLADE_SUCCESS);
	ASSERT_EQ(t,true);
	ASSERT_EQ(pos,4);
	free(buf);
}

TEST(serialization,_buf_)
{
	const char *buf_1 = "aust_for_test_1";
	const char *buf_2 = "just_for_test_2";
	const char *buf_3 = 0;
	const char *buf_4 = "test_4";

	int64_t need_len = serialization::encoded_length_vstr(buf_1);
	need_len += serialization::encoded_length_vstr(buf_2);
	need_len += serialization::encoded_length_vstr(buf_3);
	need_len += serialization::encoded_length_vstr(buf_4);

	char *buf = static_cast<char *>(malloc(need_len));
	int64_t pos = 0;
	ASSERT_EQ(serialization::encode_vstr(buf,need_len,pos,buf_1),BLADE_SUCCESS);
	ASSERT_EQ(serialization::encode_vstr(buf,need_len,pos,buf_2),BLADE_SUCCESS);
	ASSERT_EQ(serialization::encode_vstr(buf,need_len,pos,buf_3),BLADE_SUCCESS);
	ASSERT_EQ(serialization::encode_vstr(buf,need_len,pos,buf_4),BLADE_SUCCESS);
	ASSERT_EQ(pos,need_len);
	write_buf_to_file("serialization_buf",buf,pos);
	pos = 0;
	int64_t len_1;
	int64_t len_2;
	int64_t len_3;
	int64_t len_4;
	const char *b1 = serialization::decode_vstr(buf,need_len,pos,&len_1);
	ASSERT_EQ(len_1,static_cast<int64_t>(static_cast<int64_t>(strlen(buf_1) + 1)));
	ASSERT_EQ(strncmp(b1,buf_1,strlen(b1)),0);
	const char *b2 = serialization::decode_vstr(buf,need_len,pos,&len_2);
	ASSERT_EQ(len_2,static_cast<int64_t>(static_cast<int64_t>(strlen(buf_2) + 1)));
	ASSERT_EQ(strncmp(b2,buf_2,strlen(b2)),0);
	const char *b3 = serialization::decode_vstr(buf,need_len,pos,&len_3);
	(void)b3;
	ASSERT_EQ(len_3,0);
	const char *b4 = serialization::decode_vstr(buf,need_len,pos,&len_4);
	ASSERT_EQ(len_4,static_cast<int64_t>(static_cast<int64_t>(strlen(buf_4) + 1)));
	ASSERT_EQ(strncmp(b4,buf_4,strlen(b4)),0);
	ASSERT_EQ(need_len,pos);
	free(buf);
}

TEST(serialization,_buf_copy)
{
	const char *buf_1 = "aust_for_test_1";
	const char *buf_2 = "just_for_test_2";
	const char *buf_3 = 0;
	const char *buf_4 = "test_4";

	int64_t need_len = serialization::encoded_length_vstr(buf_1);
	need_len += serialization::encoded_length_vstr(buf_2);
	need_len += serialization::encoded_length_vstr(buf_3);
	need_len += serialization::encoded_length_vstr(buf_4);

	char *buf = static_cast<char *>(malloc(need_len));
	int64_t pos = 0;
	ASSERT_EQ(serialization::encode_vstr(buf,need_len,pos,buf_1),BLADE_SUCCESS);
	ASSERT_EQ(serialization::encode_vstr(buf,need_len,pos,buf_2),BLADE_SUCCESS);
	ASSERT_EQ(serialization::encode_vstr(buf,need_len,pos,buf_3),BLADE_SUCCESS);
	ASSERT_EQ(serialization::encode_vstr(buf,need_len,pos,buf_4),BLADE_SUCCESS);
	ASSERT_EQ(pos,need_len);
	pos = 0;
	int64_t len_1;
	int64_t len_2;
	int64_t len_3;
	int64_t len_4;

	char *buf_out = static_cast<char *>(malloc(need_len));
	int64_t tt = serialization::decoded_length_vstr(buf,need_len,pos);
	ASSERT_EQ(tt,static_cast<int64_t>(static_cast<int64_t>(strlen(buf_1) + 1)));
	const char *b1 = serialization::decode_vstr(buf,need_len,pos,buf_out,need_len,&len_1);
	ASSERT_EQ(len_1,static_cast<int64_t>(strlen(buf_1) + 1));
	ASSERT_EQ(strncmp(b1,buf_1,strlen(b1)),0);
	const char *b2 = serialization::decode_vstr(buf,need_len,pos,buf_out,need_len,&len_2);
	ASSERT_EQ(len_2,static_cast<int64_t>(strlen(buf_2) + 1));
	ASSERT_EQ(strncmp(b2,buf_2,strlen(b2)),0);
	const char *b3 = serialization::decode_vstr(buf,need_len,pos,buf_out,need_len,&len_3);
	(void)b3;
	ASSERT_EQ(len_3,0);
	const char *b4 = serialization::decode_vstr(buf,need_len,pos,buf_out,need_len,&len_4);
	ASSERT_EQ(len_4,static_cast<int64_t>(strlen(buf_4) + 1));
	ASSERT_EQ(strncmp(b4,buf_4,strlen(b4)),0);
	ASSERT_EQ(need_len,pos);
	free(buf);
	free(buf_out);
}

int main(int argc, char **argv)
{
	::testing::InitGoogleTest(&argc,argv);
	return RUN_ALL_TESTS();
}
