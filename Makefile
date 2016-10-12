CC=g++-4.9
CFLAGS=-std=c++0x -rdynamic -Wl,--no-as-needed -pthread -g -Wall -pthread -I./
LDFLAGS= -lpthread -ltbb -ldtranx -lprotobuf -lzmq -lhyperdex-client -lboost_thread -lboost_system -lboost_chrono -lbtree
SUBDIRS=core db
SUBSRCS=$(wildcard core/*.cc) $(wildcard db/*.cc)
OBJECTS=$(SUBSRCS:.cc=.o)
EXEC=ycsbc

all: $(SUBDIRS) $(EXEC)

$(SUBDIRS):
	$(MAKE) -C $@

$(EXEC): $(wildcard *.cc) $(OBJECTS)
	$(CC) $(CFLAGS) $^ $(LDFLAGS) -o $@

clean:
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir $@; \
	done
	$(RM) $(EXEC)

.PHONY: $(SUBDIRS) $(EXEC)

