CC=g++-4.9
CFLAGS=-std=c++11 -c -g -Wall
INCLUDES=-I../
SOURCES=$(wildcard *.cc)
OBJECTS=$(SOURCES:.cc=.o)
HEADERS=$(wildcard *.h)
SUBDIRS=tabledb
DEP=$(SOURCES:.cc=.d)
-include $(DEP)

all: $(SUBDIRS) $(SOURCES) $(OBJECTS)

$(SUBDIRS):
	$(MAKE) -C $@

%.o: %.cc
	$(CC) $(CFLAGS) $(INCLUDES) $< -o $@
	$(CC) -MMD $(CFLAGS) $(INCLUDES) $*.cc > $*.d

clean:
	$(RM) $(OBJECTS) $(DEP)

.PHONY: $(SUBDIRS)