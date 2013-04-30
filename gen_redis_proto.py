#!/usr/bin/env python -tt
# -*- coding: UTF-8 -*-
"""
Generating Redis Protocol

Generate the Redis protocol, in raw format, in order to use 'redis-cli --pipe' command to massively insert/delete.... keys in a redis server
It accepts as input a pipe with redis commands formatted as "SET key value" or "DEL key"...

Usage:

      echo "SET mykey1 value1\nSET mykey2 value2" > data.txt
      cat data.txt | python gen_redis_proto.py | redis-cli --pipe

"""

__author__ = "Salimane Adjao Moustapha (me@salimane.com)"
__version__ = "$Revision: 1.0 $"
__date__ = "$Date: 2013/04/30 12:57:19 $"
__copyleft__ = "Copyleft (c) 2013 Salimane Adjao Moustapha"
__license__ = "MIT"

import sys
import fileinput
from itertools import imap


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
        sys.stdout.write(gen_redis_proto(*line.rstrip().split(' ')))
