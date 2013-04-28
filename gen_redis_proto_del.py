#!/usr/bin/env python -tt

import sys
from itertools import imap
import fileinput


def encode(value):
    "Return a bytestring representation of the value"
    if isinstance(value, bytes):
	return value
    if not isinstance(value, unicode):
	value = str(value)
    if isinstance(value, unicode):
	value = value.encode('utf-8', 'strict')
    return value


def gen_redis_proto(*cmd):
    proto = ""
    proto += "*" + str(len(cmd)) + "\r\n"
    for arg in imap(encode, cmd):
	proto += "$" + str(len(arg)) + "\r\n"
	proto += arg + "\r\n"
    return proto

if __name__ == '__main__':
    for line in fileinput.input():
	sys.stdout.write(gen_redis_proto('DEL', line.rstrip()))
