CC=g++-4.9
CFLAGS=-std=c++11 -rdynamic -Wl,--no-as-needed -pthread -g -Wall -pthread -I./
LDFLAGS= -ltbb -ldtranx -llogcabin -lpthread -lprotobuf -lzmq -lhyperdex-client\
 -lboost_thread -lboost_system -lboost_chrono -lbtree -lprofiler -lpmemlog -lleveldb -lcryptopp -lmetis
SUBDIRS=core db scripts
SUBSRCS=$(wildcard core/*.cc) $(wildcard db/*.cc)
SUBHEADERS=$(wildcard core/*.h) $(wildcard db/*.h)
OBJECTS=$(SUBSRCS:.cc=.o)
EXEC=ycsbc

all: $(SUBDIRS) $(EXEC)

$(SUBDIRS):
	$(MAKE) -C $@

$(EXEC): $(EXEC).cc $(OBJECTS) $(SUBHEADERS)
	$(CC) $(CFLAGS) $(EXEC).cc $(OBJECTS) $(LDFLAGS) -o $@

clean:
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir $@; \
	done
	$(RM) $(EXEC)

.PHONY: $(SUBDIRS)