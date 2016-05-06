CC=g++-4.6
CFLAGS=-std=c++0x -c -g -Wall
INCLUDES=-I../
SOURCES=$(wildcard *.cc)
OBJECTS=$(SOURCES:.cc=.o)
HEADERS=$(wildcard *.h)

all: $(SOURCES) $(OBJECTS) $(HEADERS)

%.o: %.cc $(HEADERS)
	$(CC) $(CFLAGS) $(INCLUDES) $< -o $@

clean:
	$(RM) $(OBJECTS)

