
AR = ar
CC = g++
CFLAGS = -g -Wall -fPIC -Werror

OUTPUT := ../../../lib/liblocalfile.a

INCS_HEAD := . ../../../common   ../../../pandora/ ../../../message

LIB_PATH := -L ../../../lib
LIBS := -lmessage -lcommon -lmessage -lcommon -lpandora -lpthread -lrt 

SOURCE := $(wildcard  *.cpp )
OBJS := $(patsubst %.cpp, %.o, $(SOURCE))
LIBOBJS := $(filter-out ds_main.o , $(OBJS))

%.o:%.cpp
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) -c $< -o $@

../../../lib/liblocalfile.a : $(LIBOBJS) 
	$(AR) rcs  $@ $^

clean:
	-rm -f $(OBJS) 
	-rm -f $(OUTPUT)
