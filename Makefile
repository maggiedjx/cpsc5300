# Makefile 
CCFLAGS     = -std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -c
COURSE      = /usr/local/db6
INCLUDE_DIR = $(COURSE)/include
LIB_DIR     = $(COURSE)/lib

# below is a list of all the compiled object files needed to build the shell executable
OBJS       = shell.o

# General rule for compilation
%.o: %.cpp
	g++ -I$(INCLUDE_DIR) $(CCFLAGS) -o "$@" "$<"

# Rule for linking to create the executable
# Note that this is the default target since it is the first non-generic one in the Makefile: $ make
sql5300: $(OBJS)
	g++ -L$(LIB_DIR) -o $@ $< -ldb_cxx -lsqlparser

# Rule for removing all non-source files (so they can get rebuilt from scratch)
# Note that since it is not the first target, you have to invoke it explicitly: $ make clean
clean:
	rm -f shell *.o