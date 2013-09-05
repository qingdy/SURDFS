AR = ar
CC = g++
CFLAGS = -g -Wall -fPIC

FILENAME := ../../lib/libclient.a
OUTPUT := $(FILENAME)
OBJDIR := ../obj/

INCS_HEAD := ./ ../../common ../../pandora ../../message
INCS_SOURCE := ./

SOURCE := $(wildcard  *.cpp)
OBJS := $(patsubst %.cpp, %.o, $(SOURCE))

%.o:%.cpp
	@$(CC) $(CFLAGS) $(addprefix -I,$(INCS_HEAD)) -c $< -o $@

$(OUTPUT): $(OBJS)
	ar rcs  $@ $^
	ls -lrt $(OUTPUT)

clean:
	-rm -f $(OBJS)
	-rm -rf ../../lib/libclient.a
