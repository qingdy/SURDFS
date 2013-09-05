AR = ar
CC = g++
CFLAGS = -g -Wall -fPIC

DIR_BIN = .

INCS_HEAD :=  ../../external ./ ../src ../../pandora ../../message ../../common

LIB_PATH := -L ../../lib -L ../../external/gtest/lib
LIBS := -lclient -lgtest_main -lcommon -lmessage -lpandora -lpthread -lrt 

SOURCE := $(wildcard ./*.cpp) 
OBJS := $(patsubst %.cpp, %.o, $(SOURCE))
ALL_BIN=$(patsubst %.o, ${DIR_BIN}/%, ${OBJS})

%.o:%.cpp
	$(CC) $(CFLAGS) $(addprefix -I, $(INCS_HEAD)) -c $< -o $@


all: ${ALL_BIN}

.PHONY : all 

${DIR_BIN}/circleread: circleread.o 
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/circlewrite: circlewrite.o 
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/client_cache_node_unittest : client_cache_node_unittest.o 
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/client_conf_unittest : client_conf_unittest.o 
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/client_unittest : client_unittest.o 
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/createtest : createtest.o 
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/createtest2 : createtest2.o 
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/createtest3 : createtest3.o 
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/deletedir : deletedir.o
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/deletedirtest : deletedirtest.o
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/deletefiles : deletefiles.o 
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/deleterootfiles : deleterootfiles.o 
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/deletesomefiles : deletesomefiles.o 
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/func : func.o 
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/getfileinfo : getfileinfo.o 
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/getfileinfo2 : getfileinfo2.o 
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/getfileinfo3 : getfileinfo3.o 
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/getfileinfotest : getfileinfotest.o 
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/getlisting : getlisting.o 
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/mkfiletest : mkfiletest.o 
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/mkdir : mkdir.o 
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/mkdirtest : mkdirtest.o 
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/multiread : multiread.o 
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/opentest: opentest.o 
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/read : read.o 
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/readtest: readtest.o 
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/safewritetest: safewritetest.o 
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/statics_rwd : statics_rwd.o 
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/testgetpath : testgetpath.o 
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/testread_read64m : testread_read64m.o 
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/testread : testread.o 
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/thread_write : thread_write.o 
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/tuntu : tuntu.o 
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/writetest : writetest.o
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/writebigdatatest : writebigdatatest.o
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/writebeforeread : writebeforeread.o
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/writedir : writedir.o
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/readdir : readdir.o
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/rwd : rwd.o
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

${DIR_BIN}/saferwd : saferwd.o
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) $(LIB_PATH) -o $@ $^  $(LIBS) 

clean:
	-rm -f $(OBJS)
	-rm -f ${ALL_BIN}
