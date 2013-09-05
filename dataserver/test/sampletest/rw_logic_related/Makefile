AR = ar
CC = g++
CFLAGS = -g -Wall -fPIC

DIR_BIN = .


INCS_HEAD := ./   ../../../../pandora/  ../../../../common  ../../../../message  ../../../../client/src

LIB_PATH := -L ../../../../lib  
LIBS := -lclient   -lmessage  -lcommon -lpandora -lpthread -lrt 

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
