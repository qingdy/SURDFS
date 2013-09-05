CC = g++
CFLAGS = -g -Wall -fPIC -DGTEST

DIR_BIN = .

INCS_HEAD := ../../external/gtest/ ./btree ./HA. ../src  ../src/HA ../../pandora ../../message ../../common ../src/btree 

LIB_PATH := -L ../../lib  -L ../../external/gtest/lib
LIBS := -lnameserver -lgtest_main -lmessage -lpandora -lcommon -lpthread -lrt 

SOURCE := $(wildcard *.cpp btree/*.cpp HA/*.cpp) 
OBJS := $(patsubst %.cpp, %.o, $(SOURCE))
ALL_BIN := $(patsubst %.o, bin/%, ${OBJS})

%.o:%.cpp
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) -c $< -o $@


all:${ALL_BIN}

bin/% : %.o
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS)

clean:
	-rm -f ${OBJS}
	-rm -f ${ALL_BIN}
