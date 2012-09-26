#!/usr/bin/env python -tt
# -*- coding: UTF-8 -*-
"""
Redis Copy

Redis Copy the keys in a source redis server into another target redis server.
The script probably needs to be added to a cron job if the keys are a lot because it only copies a fix number of keys at a time
and continue from there on the next run. It does this until there is no more keys to copy

Usage: python redis-copy.py [options]

Options:
  -l ..., --limit=...         optional numbers of keys to copy per run, if not defined 10000 is the default . e.g. 1000
  -s ..., --source=...        source redis server "ip:port" to copy keys from. e.g. 192.168.0.99:6379
  -t ..., --target=...        target redis server "ip:port" to copy keys to. e.g. 192.168.0.101:6379
  -d ..., --databases=...     comma separated list of redis databases to select when copying. e.g. 2,5
  -h, --help                  show this help
  --clean                     clean all variables, temp lists created previously by the script

Dependencies: redis (redis-py: sudo pip install redis)

Examples:
  python redis-copy.py --help                                show this doc

  python redis-copy.py \
  --source=192.168.0.99:6379 \
  --target=192.168.0.101:6379 \
  --databases=2,5 --clean                                 clean all variables, temp lists created previously by the script

  python redis-copy.py \
  --source=192.168.0.99:6379 \
  --target=192.168.0.101:6379 \
  --databases=2,5                                         copy all keys in db 2 and 5 from server 192.168.0.99:6379 to server 192.168.0.101:6379
                                                          with the default limit of 10000 per script run

  python redis-copy.py --limit=1000 \
  --source=192.168.0.99:6379 \
  --target=192.168.0.101:6379 \
  --databases=2,5                                         copy all keys in db 2 and 5 from server 192.168.0.99:6379 to server 192.168.0.101:6379
                                                          with a limit of 1000 per script run

"""

__author__ = "Salimane Adjao Moustapha (salimane@gmail.com)"
__version__ = "$Revision: 1.0 $"
__date__ = "$Date: 2011/06/09 12:57:19 $"
__copyleft__ = "Copyleft (c) 2011 Salimane Adjao Moustapha"
__license__ = "MIT"


import redis
import time
import sys
import getopt


class RedisCopy:
    """A class for copying keys from one server to another.
    """

    #some key prefix for this script
    mprefix = 'mig:'
    keylistprefix = 'keylist:'
    hkeylistprefix = 'havekeylist:'

    # numbers of keys to copy on each iteration
    limit = 10000

    def __init__(self, source, target, dbs):
        self.source = source
        self.target = target
        self.dbs = dbs

    def save_keylists(self):
        """Function to save the keys' names of the source redis server into a list for later usage.
        """

        for db in self.dbs:
            servername = self.source['host'] + ":" + str(
                self.source['port']) + ":" + str(db)
            #get redis handle for server-db
            r = redis.StrictRedis(
                host=self.source['host'], port=self.source['port'], db=db)
            dbsize = r.dbsize()
            #check whether we already have the list, if not get it
            hkl = r.get(self.mprefix + self.hkeylistprefix + servername)
            if hkl is None or int(hkl) != 1:
                print "Saving the keys in %s to temp keylist...\n" % servername
                moved = 0
                r.delete(self.mprefix + self.keylistprefix + servername)
                for key in r.keys('*'):
                    moved += 1
                    r.rpush(
                        self.mprefix + self.keylistprefix + servername, key)
                    if moved % self.limit == 0:
                        print  "%d keys of %s inserted in temp keylist at %s...\n" % (moved, servername, time.strftime("%Y-%m-%d %I:%M:%S"))

                r.set(self.mprefix + self.hkeylistprefix + servername, 1)
            print "ALL %d keys of %s already inserted to temp keylist ...\n\n" % (dbsize - 1, servername)

    def copy_db(self, limit=None):
        """Function to copy all the keys from the source into the new target.
        - limit : optional numbers of keys to copy per run
        """

        #set the limit per run
        try:
            limit = int(limit)
        except (ValueError, TypeError):
            limit = None

        if limit is not None:
            self.limit = limit

        for db in self.dbs:
            servername = self.source['host'] + ":" + str(
                self.source['port']) + ":" + str(db)
            print "Processing keys copying of server %s at %s...\n" % (
                servername, time.strftime("%Y-%m-%d %I:%M:%S"))
            #get redis handle for current source server-db
            r = redis.StrictRedis(
                host=self.source['host'], port=self.source['port'], db=db)
            moved = 0
            dbsize = r.dbsize() - 1
            #get keys already moved
            keymoved = r.get(self.mprefix + "keymoved:" + servername)
            keymoved = 0 if keymoved is None else int(keymoved)
            #check if we already have all keys copied for current source server-db
            if dbsize < keymoved:
                print "ALL %d keys from %s have already been copied.\n" % (
                    dbsize, servername)
                continue

            print "Started copy of %s keys from %d to %d at %s...\n" % (servername, keymoved, dbsize, time.strftime("%Y-%m-%d %I:%M:%S"))

            #get redis handle for corresponding target server-db
            rr = redis.StrictRedis(
                host=self.target['host'], port=self.target['port'], db=db)

            #max index for lrange
            newkeymoved = keymoved + \
                self.limit if dbsize > keymoved + self.limit else dbsize

            for key in r.lrange(self.mprefix + self.keylistprefix + servername, keymoved, newkeymoved):
                #get key type
                ktype = r.type(key)
                #if undefined type go to next key
                if ktype == 'none':
                    continue

                #save key to target server-db
                if ktype == 'string':
                    rr.set(key, r.get(key))
                elif ktype == 'hash':
                    rr.hmset(key, r.hgetall(key))
                elif ktype == 'list':
                    if key == self.mprefix + "keylist:" + servername:
                        continue
                    #value = r.lrange(key, 0, -1)
                    #rr.rpush(key, *value)
                    for k in r.lrange(key, 0, -1):
                        rr.rpush(key, k)
                elif ktype == 'set':
                    #value = r.smembers(key)
                    #rr.sadd(key, *value)
                    for k in r.smembers(key):
                        rr.sadd(key, k)
                elif ktype == 'zset':
                    #value = r.zrange(key, 0, -1, withscores=True)
                    #rr.zadd(key, **dict(value))
                    for k, v in r.zrange(key, 0, -1, withscores=True):
                        rr.zadd(key, v, k)

                # Handle keys with an expire time set
                kttl = r.ttl(key)
                kttl = -1 if kttl is None else int(kttl)
                if kttl != -1:
                    rr.expire(key, kttl)

                moved += 1

                if moved % 10000 == 0:
                    print "%d keys have been copied on %s at %s...\n" % (
                        moved, servername, time.strftime("%Y-%m-%d %I:%M:%S"))

            r.set(self.mprefix + "keymoved:" + servername, newkeymoved)
            print "%d keys have been copied on %s at %s\n" % (
                newkeymoved, servername, time.strftime("%Y-%m-%d %I:%M:%S"))

    def flush_target(self):
        """Function to flush the target server.
        """
        for db in self.dbs:
            servername = self.target['host'] + ":" + str(
                self.target['port']) + ":" + str(db)
            print "Flushing server %s at %s...\n" % (
                servername, time.strftime("%Y-%m-%d %I:%M:%S"))
            r = redis.StrictRedis(
                host=self.target['host'], port=self.target['port'], db=db)
            r.flushdb()
            print "Flushed server %s at %s...\n" % (
                servername, time.strftime("%Y-%m-%d %I:%M:%S"))

    def clean(self):
        """Function to clean all variables, temp lists created previously by the script.
        """

        print "Cleaning all temp variables...\n"
        for db in self.dbs:
            servername = self.source['host'] + ":" + str(
                self.source['port']) + ":" + str(db)
            r = redis.StrictRedis(
                host=self.source['host'], port=self.source['port'], db=db)
            r.delete(self.mprefix + "keymoved:" + servername)
            r.delete(self.mprefix + self.keylistprefix + servername)
            r.delete(self.mprefix + self.hkeylistprefix + servername)
            r.delete(self.mprefix + "firstrun")
            r.delete(self.mprefix + 'run')
        print "Done.\n"


def main(source, target, databases, limit=None, clean=False):
    #getting source and target
    if (source == target):
        exit('The 2 servers adresses are the same. e.g. python redis-copy.py 127.0.0.1:6379 127.0.0.1:63791  0,1')
    so = source.split(':')
    if len(so) == 2:
        source_server = {'host': so[0], 'port': int(so[1])}
    else:
        exit('Supplied source address is wrong. e.g. python redis-copy.py 127.0.0.1:6379 127.0.0.1:63791  0,1')

    sn = target.split(':')
    if len(sn) == 2:
        target_server = {'host': sn[0], 'port': int(sn[1])}
    else:
        exit('Supplied target address is wrong. e.g. python redis-copy.py 127.0.0.1:6379 127.0.0.1:63791  0,1')

    #getting the dbs
    dbs = [int(k) for k in databases.split(',')]
    if len(dbs) < 1:
        exit('Supplied list of db is wrong. e.g. python redis-copy.py 127.0.0.1:6379 127.0.0.1:63791  0,1')

    try:
        r = redis.StrictRedis(
            host=source_server['host'], port=source_server['port'], db=dbs[0])
    except AttributeError as e:
        exit('Please this script requires redis-py >= 2.4.10, your current version is :' + redis.__version__)

    mig = RedisCopy(source_server, target_server, dbs)

    if clean == False:
        #check if script already running
        run = r.get(mig.mprefix + "run")
        if run is not None and int(run) == 1:
            exit('another process already running the script')
        r.set(mig.mprefix + 'run', 1)

        mig.save_keylists()

        firstrun = r.get(mig.mprefix + "firstrun")
        firstrun = 0 if firstrun is None else int(firstrun)
        if firstrun == 0:
            mig.flush_target()
            r.set(mig.mprefix + "firstrun", 1)

        mig.copy_db(limit)
    else:
        mig.clean()

    r.set(mig.mprefix + 'run', 0)


def usage():
    print __doc__


if __name__ == "__main__":
    clean = False
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hl:s:t:d:", ["help", "limit=", "source=", "target=", "databases=", "clean"])
    except getopt.GetoptError:
        usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            usage()
            sys.exit()
        elif opt == "--clean":
            clean = True
        elif opt in ("-l", "--limit"):
            limit = arg
        elif opt in ("-s", "--source"):
            source = arg
        elif opt in ("-t", "--target"):
            target = arg
        elif opt in ("-d", "--databases"):
            databases = arg

    try:
        limit = int(limit)
    except (NameError, TypeError, ValueError):
        limit = None

    try:
        main(source, target, databases, limit, clean)
    except NameError as e:
        usage()
