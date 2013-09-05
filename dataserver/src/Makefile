AR = ar
CC = g++
CFLAGS = -g -Wall -fPIC

OUTPUT :=   ../../lib/libdataserver.a  ../bin/bladestore_dataserver

INCS_HEAD := . .. ./localfile ../../pandora ../../common ../../message

LIB_PATH := -L ../../lib
LIBS :=   -lmessage -lcommon -lpandora -lpthread -lrt

SOURCE := $(wildcard  *.cpp ./localfile/*.cpp)
OBJS := $(patsubst %.cpp, %.o, $(SOURCE))
LIBOBJS := $(filter-out dataserver_main.o , $(OBJS))

%.o:%.cpp
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) -c $< -o $@


all:$(OUTPUT)

../bin/bladestore_dataserver: $(OBJS)
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS)


../../lib/libdataserver.a : $(LIBOBJS) 
	$(AR) rcs  $@ $^

clean:
	-rm -f $(OBJS) 
	-rm -f $(OUTPUT)
