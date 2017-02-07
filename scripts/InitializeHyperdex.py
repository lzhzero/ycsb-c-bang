#! /usr/bin/env python
import hyperdex.client
import sys
# TODO: organize the initialization for hyperdex tests
c = hyperdex.client.Client('192.168.0.1',7777)
SPACE = "ning"
if len(sys.argv) != 2:
	print "first argument should be filename"
	exit
f = open(sys.argv[1], 'r')
index = 1
for line in f:
	key = line.strip()
	value = "value_test"
	if index % 10000 == 0:
		print index
	index = index + 1
	c.put(SPACE, key, {'value': value})
	