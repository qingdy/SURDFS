LIBS := pthread message localfile common pandora rt
INCS :=. ../../../../dataserver/src/ ../../../../dataserver/src/localfile/ ../../../../pandora/ ../../../../common  ../../../../message 
			 
LIBPATHS :=  ../../../../lib/
EXE := test_localfile.exe

USER_MARCOS :=  _FIRST_CHAR_ #WAVE_DEBUG_
CFLAGS := -g -w #-O2 
CC = g++

SOURCE := $(wildcard ./*.cpp)
OBJS := $(patsubst %.cpp, %.o, $(SOURCE))

%.o: %.cpp
	$(CC) $(CFLAGS) $(addprefix -D, $(USER_MARCOS)) $(addprefix -I, $(INCS)) -c $< -o $@

$(EXE): $(OBJS)
	$(CC) $(CFLAGS) $(addprefix -L, $(LIBPATHS)) -o $(EXE) $(OBJS)  $(addprefix -l, $(LIBS) )

clean:
	rm -rf $(OBJS)

realclean: clean
	rm -rf $(EXE)
