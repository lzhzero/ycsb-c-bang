CC=g++-4.9
CFLAGS=-std=c++11 -g -Wall
SOURCES=$(wildcard *.cc)
OBJECTS=$(SOURCES:.cc=.o)
EXEC=$(SOURCES:.cc=)

all: $(EXEC)

%.o: %.cc
	$(CC) $(CFLAGS) $< -c -o $@
	
$(EXEC): $(OBJECTS)
	$(CC) $(CFLAGS) $< -o $@

clean:
	$(RM) $(OBJECTS) $(EXEC)