#! /usr/bin/env python
# -*- coding: UTF-8 -*-
"""
Redis Memory Stats

A memory size analyzer that parses the output of the memory report of rdb <https://github.com/sripathikrishnan/redis-rdb-tools>
 for memory size stats about key patterns

At its core, RedisMemStats uses the output of the memory report of rdb, which echoes a csv row line for every key
stored to a Redis instance.
It parses these lines, and aggregates stats on the most memory consuming keys, prefixes, dbs and redis data structures.

Usage: rdb -c memory <REDIS dump.rdb TO ANALYZE> | ./redis-mem-stats.py [options]

      OR

      rdb -c memory <REDIS dump.rdb TO ANALYZE> > <OUTPUT CSV FILE>
      ./redis-mem-stats.py [options] <OUTPUT CSV FILE>

      options:
      --prefix-delimiter=...           String to split on for delimiting prefix and rest of key, if not provided `:` is the default . --prefix-delimiter=#

Examples:
  rdb -c memory /var/lib/redis/dump.rdb > /tmp/outfile.csv
  ./redis-mem-stats.py /tmp/outfile.csv

  or

  rdb -c memory /var/lib/redis/dump.rdb | ./redis-mem-stats.py


Dependencies: rdb (redis-rdb-tools: https://github.com/sripathikrishnan/redis-rdb-tools)

"""

__author__ = "Salimane Adjao Moustapha (me@salimane.com)"
__version__ = "$Revision: 1.0 $"
__date__ = "$Date: 2012/09/24 12:57:19 $"
__copyleft__ = "Copyleft (c) 2012-2013 Salimane Adjao Moustapha"
__license__ = "MIT"

import argparse
import sys
from collections import defaultdict


class RedisMemStats(object):
    """
    Analyze the output of the memory report of rdb
    """

    def __init__(self, prefix_delim=':'):
        self.line_count = 0
        self.skipped_lines = 0
        self.total_size = 0
        self.dbs = defaultdict(int)
        self.types = defaultdict(int)
        self.keys = defaultdict(int)
        self.prefixes = defaultdict(int)
        self.sizes = []
        self._cached_sorts = {}
        self.prefix_delim = prefix_delim

    def _record_size(self, entry):
        size = int(entry['size'])
        self.total_size += size
        self.dbs[entry['db']] += size
        self.types[entry['type']] += size
        self.keys[entry['key']] += size
        pos = entry['key'].rfind(self.prefix_delim)
        if pos is not -1:
            self.prefixes[entry['key'][0:pos]] += size
        #self.sizes.append((entry['size'], entry))

    def _record_columns(self, sizes):
        for size, entry in sizes:
            mem = int(size)
            self.dbs[entry['db']] += mem
            self.types[entry['type']] += mem
            self.keys[entry['key']] += mem
            pos = entry['key'].rfind(self.prefix_delim)
            if pos is not -1:
                self.prefixes[entry['key'][0:pos]] += mem

    def _get_or_sort_list(self, ls):
        key = id(ls)
        if not key in self._cached_sorts:
            sorted_items = sorted(ls)
            self._cached_sorts[key] = sorted_items
        return self._cached_sorts[key]

    def _general_stats(self):
        return (
            ("Lines Processed", self.line_count),
        )

    def process_entry(self, entry):
        self._record_size(entry)

    def _top_n(self, stat, n=30):
        sorted_items = sorted(
            stat.iteritems(), key=lambda x: x[1], reverse=True)
        return sorted_items[:n]

    def humanize_bytes(self, size):
        for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024.0:
                return "%3.1f%s" % (size, x)
            size /= 1024.0

    def _pretty_print(self, result, title, percentages=False):
        print title
        print '=' * 40
        if not result:
            print 'n/a\n'
            return

        max_key_len = max((len(x[0]) for x in result))
        max_val_len = max((len(str(x[1])) for x in result))
        for key, val in result:
            val_h = self.humanize_bytes(val)
            key_padding = max(max_key_len - len(key), 0) * ' '
            if percentages:
                val_padding = max(max_val_len - len(val_h), 0) * ' '
                val = '%s%s\t(%.2f%%)' % (
                    val_h, val_padding, (float(val) / self.total_size) * 100)
            print key, key_padding, '\t', val
        print

    def print_stats(self):
        self._pretty_print(self._general_stats(), 'Overall Stats')
        #self._record_columns(self.sizes)
        self._pretty_print(
            self._top_n(self.prefixes), 'Heaviest Prefixes', percentages=True)
        self._pretty_print(
            self._top_n(self.keys), 'Heaviest Keys', percentages=True)
        self._pretty_print(
            self._top_n(self.dbs), 'Heaviest Dbs', percentages=True)
        self._pretty_print(
            self._top_n(self.types), 'Heaviest Types', percentages=True)

    def process_input(self, input):
        for line in input:
            self.line_count += 1
            line = line.strip()
            parts = line.split(",")
            if len(parts) > 1:
                try:
                    size = int(parts[3])
                except ValueError as e:
                    self.skipped_lines += 1
                    continue
                self.process_entry({'db': parts[0], 'type': parts[1], 'key': parts[2].replace('"', ''), 'size': parts[3]})
            else:
                self.skipped_lines += 1
                continue


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'input',
        type=argparse.FileType('r'),
        default=sys.stdin,
        nargs='?',
        help="File to parse; will read from stdin otherwise")
    parser.add_argument(
        '--prefix-delimiter',
        type=str,
        default=':',
        help="String to split on for delimiting prefix and rest of key",
        required=False)
    args = parser.parse_args()
    counter = RedisMemStats(prefix_delim=args.prefix_delimiter)
    counter.process_input(args.input)
    counter.print_stats()
