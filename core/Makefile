CC=g++-4.9
CFLAGS=-std=c++11 -c -g -Wall
SOURCES=$(wildcard *.cc)
OBJECTS=$(SOURCES:.cc=.o)
HEADERS=$(wildcard *.h)

all: $(SOURCES) $(OBJECTS)

%.o: %.cc $(HEADERS)
	$(CC) $(CFLAGS) $< -o $@

clean:
	$(RM) $(OBJECTS)
