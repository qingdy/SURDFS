LIBS := pthread message gtest_main localfile common pandora
INCS :=. ../../../../dataserver/src/ ../../../../dataserver/src/localfile/  ../../../../pandora/ ../../../../common  ../../../../message /opt/bladestore/gtest/gtest-1.6.0/include/
LIBPATHS := /opt/bladestore/gtest/gtest-1.6.0/make ../../../../lib/
EXE := test_localfile.exe

USER_MARCOS :=  _FIRST_CHAR_ #WAVE_DEBUG_
CFLAGS := -g -w #-O2 
CC = g++

SOURCE := $(wildcard ./*.cpp)
OBJS := $(patsubst %.cpp, %.o, $(SOURCE))
ALL_BIN := $(patsubst %.o, %.exe, ${OBJS})

%.o : %.cpp
	$(CC) $(CFLAGS) $(addprefix -D, $(USER_MARCOS)) $(addprefix -I, $(INCS)) -c $< -o $@

all:${ALL_BIN}

%.exe:%.o
	$(CC) $(CFLAGS) $(addprefix -I,$(INCS)) $(addprefix -L, $(LIBPATHS)) -o $@ $^  $(addprefix -l, $(LIBS) )

clean:
	rm -rf $(OBJS)
	rm -rf $(ALL_BIN)

realclean: clean
	rm -rf $(ALL_BIN)
