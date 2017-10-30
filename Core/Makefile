CC=g++-4.9
CFLAGS=-std=c++11 -c -g -Wall
INCLUDES=-I../
SOURCES=$(wildcard *.cc)
OBJECTS=$(SOURCES:.cc=.o)
HEADERS=$(wildcard *.h)
DEP=$(SOURCES:.cc=.d)
-include $(DEP)

all: $(SOURCES) $(OBJECTS)

%.o: %.cc
	$(CC) $(CFLAGS) $(INCLUDES) $< -o $@
	$(CC) -MMD $(CFLAGS) $(INCLUDES) $*.cc > $*.d

clean:
	$(RM) $(OBJECTS) $(DEP)