AR = ar
CC = g++
CFLAGS = -g -Wall -fPIC -DGTEST

DIR_BIN = .

INCS_HEAD := ../../../external/gtest/ .  ../../src  ../../src/localfile  ../../../common ../../../message ../../../pandora
LIB_PATH := -L ../../../lib  -L ../../../external/gtest/lib
LIBS := -ldataserver -lgtest_main  -lcommon -lmessage -lcommon -lpandora -lpthread -lrt 

#SOURCE := $(wildcard *.cpp localfile/*.cpp ) 
SOURCE := $(wildcard *.cpp ) 
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
