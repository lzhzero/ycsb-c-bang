CC=g++-4.9
CFLAGS=-std=c++11 -c -g -Wall -fgnu-tm
INCLUDES=-I../
SOURCES=$(wildcard *.cc)
OBJECTS=$(SOURCES:.cc=.o)
HEADERS=$(wildcard *.h)

all: $(SOURCES) $(OBJECTS) $(HEADERS)

%.o: %.cc $(HEADERS)
	$(CC) $(CFLAGS) $(INCLUDES) $< -o $@

clean:
	$(RM) $(OBJECTS)

