AR = ar
CC = g++
CFLAGS = -g -Wall -fPIC

OUTPUT := ../../lib/libnameserver.a ../bin/nameserver 

INCS_HEAD := ./ ./HA ./btree ../../pandora ../../common ../../message

LIB_PATH := -L ../../lib
LIBS := -lcommon -lmessage -lpandora -lpthread -lrt

SOURCE := $(wildcard  *.cpp ./btree/*.cpp ./HA/*.cpp)
OBJS := $(patsubst %.cpp, %.o, $(SOURCE))
LIBOBJS := $(filter-out nameserver_main.o , $(OBJS))


%.o:%.cpp
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) -c $< -o $@

all:$(OUTPUT)

../bin/nameserver : $(OBJS)
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS)

../../lib/libnameserver.a : $(LIBOBJS) 
	$(AR) rcs  $@ $^

clean:
	-rm -f $(OBJS)
#	-rm -f $(OUTPUT)
