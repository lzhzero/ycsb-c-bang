CC=g++-4.6
CFLAGS=-std=c++0x -c -g -Wall
SOURCES=$(wildcard *.cc)
OBJECTS=$(SOURCES:.cc=.o)
HEADERS=$(wildcard *.h)

all: $(SOURCES) $(OBJECTS)

%.o: %.cc $(HEADERS)
	$(CC) $(CFLAGS) $< -o $@

clean:
	$(RM) $(OBJECTS)
