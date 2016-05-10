#! /usr/bin/env python
import hyperdex.client
import sys
c = hyperdex.client.Client('128.104.222.31',7777)
SPACE = "ning"
if len(sys.argv) != 2:
	print "first argument should be filename"
f = open(sys.argv[1], 'r')
index = 1
for line in f:
	key = line.strip()
	value = "value_test"
	if index % 10000 == 0:
		print index
	index = index + 1
	c.put(SPACE, key, {'value': value})
	