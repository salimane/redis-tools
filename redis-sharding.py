#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
Redis sharding

Reshard the keys in a number of source redis servers into another number of target cluster of redis servers
in order to scale an application.
The script probably needs to be added to a cron job if the keys are a lot because it only reshards a fix number of keys at a time
and continue from there on the next run. It does this until there is no more keys to reshard

Usage: python redis-sharding.py [options]

Options:
  -l ..., --limit=...         optional numbers of keys to reshard per run, if not defined 10000 is the default . e.g. 1000
  -s ..., --sources=...       comma separated list of source redis servers "ip:port" to fetch keys from. e.g. 192.168.0.99:6379,192.168.0.100:6379
  -t ..., --targets=...       comma separated list target redis servers "node_i#ip:port" to reshard the keys to. e.g. node_1#192.168.0.101:6379,node_2#192.168.0.102:6379,node_3#192.168.0.103:6379
  -d ..., --databases=...     comma separated list of redis databases to select when resharding. e.g. 2,5
  -h, --help                  show this help


IMPORTANT: This script assume your target redis cluster of servers is based on a  node system,
which is simply a host:port pair that points to a single redis-server instance.
Each node is given a symbolic node name "node_i" where i is the number gotten from this hashing system
"str((abs(binascii.crc32(key) & 0xffffffff) % len(targets)) + 1)"
to uniquely identify it in a way that doesn’t tie it to a specific host (or port).
e.g.
config = {
  'node_1':{'host':'192.168.0.101', 'port':6379},
  'node_2':{'host':'192.168.0.102', 'port':6379},
  'node_3':{'host':'192.168.0.103', 'port':6379},
}



Examples:
  python redis-sharding.py --help                                show this doc
  python redis-sharding.py \
  --sources=192.168.0.99:6379,192.168.0.100:6379 \
  --targets=node_1#192.168.0.101:6379,node_2#192.168.0.102:6379,node_3#192.168.0.103:6379 \
  --databases=2,5
  
  python redis-sharding.py --limit=1000 \
  --sources=192.168.0.99:6379,192.168.0.100:6379 \
  --targets=node_1#192.168.0.101:6379,node_2#192.168.0.102:6379,node_3#192.168.0.103:6379 \
  --databases=2,5

"""

__author__ = "Salimane Adjao Moustapha (salimane@gmail.com)"
__version__ = "$Revision: 1.0 $"
__date__ = "$Date: 2011/06/09 12:57:19 $"
__copyleft__ = "Copyleft (c) 2011 Salimane Adjao Moustapha"
__license__ = "Python"


import redis
from datetime import datetime
import binascii
import sys
import getopt

class RedisSharding:
  """A class for resharding the keys in a number of source redis servers into another number of target cluster of redis servers
  """

  #some key prefix for this script
  shardprefix = 'rsk:'
  keylistprefix = 'keylist:'
  hkeylistprefix = 'havekeylist:'

  # hold the redis handle of the targets cluster
  targets_redis = {}
  
  # numbers of keys to resharding on each iteration
  limit = 10000

  def __init__(self, sources, targets, dbs):
    self.sources = sources
    self.targets = targets
    self.len_targets = len(targets)
    self.dbs = dbs
    for node in self.targets:
      for db in self.dbs:
        self.targets_redis[node+'_'+str(db)] = redis.Redis(connection_pool=redis.ConnectionPool(host=self.targets[node]['host'], port=self.targets[node]['port'], db=db))


  def save_keylists(self):
    """Function for each server in the sources, save all its keys' names into a list for later usage.
    """
    
    for server in self.sources:
      for db in self.dbs:
        servername = server['host'] + ":" + str(server['port']) + ":" + str(db)
        print "Processing temp keylists on server %s at %s...\n" % (servername, datetime.now().strftime("%Y-%m-%d %I:%M:%S"))
        #get redis handle for server-db
        r = redis.Redis(connection_pool=redis.ConnectionPool(host=server['host'], port=server['port'], db=db))
        dbsize = r.dbsize()
        #check whether we already have the list, if not get it
        hkl = r.get(self.shardprefix + self.hkeylistprefix + servername)
        if hkl is None or int(hkl) != 1:
          print "Saving the keys in %s to temp keylist...\n" % servername
          moved = 0
          r.delete(self.shardprefix + self.keylistprefix + servername)
          for key in r.keys('*'):
            moved += 1
            r.rpush(self.shardprefix + self.keylistprefix + servername, key)
            if moved % self.limit == 0:
              print  "%d keys of %s inserted in temp keylist at %s...\n" % (moved, servername, datetime.now().strftime("%Y-%m-%d %I:%M:%S"))

          r.set(self.shardprefix + self.hkeylistprefix + servername, 1)
        print "ALL %d keys of %s already inserted to temp keylist ...\n\n" % (dbsize-1, servername)    


  def reshard_db(self, limit=None):
    """Function for each server in the sources, reshard all its keys into the new target cluster.
    - limit : optional numbers of keys to reshard per run
    """

    #set the limit per run
    try:
      limit = int(limit)
    except (ValueError, TypeError):
      limit = None

    if limit is not None: self.limit = limit          
    
    for server in self.sources:
      for db in self.dbs:
        servername = server['host'] + ":" + str(server['port']) + ":" + str(db)
        print "Processing keys resharding of server %s at %s...\n" % (servername, datetime.now().strftime("%Y-%m-%d %I:%M:%S"))
        #get redis handle for current source server-db
        r = redis.Redis(connection_pool=redis.ConnectionPool(host=server['host'], port=server['port'], db=db))
        moved = 0
        dbsize = r.dbsize() - 1
        #get keys already moved
        keymoved = r.get(self.shardprefix + "keymoved:" + servername)
        keymoved = 0 if keymoved is None else int(keymoved)
        #check if we already have all keys resharded for current source server-db
        if dbsize <= keymoved:
          print "ALL %d keys from %s have already been resharded.\n" % (dbsize, servername)
          #move to next source server-db in iteration
          continue

        print "Started resharding of %s keys from %d to %d at %s...\n" % (servername, keymoved, dbsize, datetime.now().strftime("%Y-%m-%d %I:%M:%S"))
        #max index for lrange
        newkeymoved = keymoved+self.limit if dbsize > keymoved+self.limit else dbsize

        for key in r.lrange(self.shardprefix + self.keylistprefix + servername, keymoved, newkeymoved):
          #calculate reshard node of key
          node = str((abs(binascii.crc32(key) & 0xffffffff) % self.len_targets) + 1)
          #get key type
          ktype = r.type(key)
          #if undefined type go to next key
          if ktype == 'none':
            continue

          #get redis handle for corresponding target server-db
          rr = self.targets_redis['node_'+node+'_'+str(db)]
          #save key to new cluster server-db
          if ktype == 'string' :
            rr.set(key, r.get(key))
          elif ktype == 'hash' :
            rr.hmset(key, r.hgetall(key))
          elif ktype == 'list' :
            if key == self.shardprefix + "keylist:" + servername:
              continue
            value = r.lrange(key, 0, -1)
            rr.rpush(key, *value)
          elif ktype == 'set' :
            value = r.smembers(key)
            rr.sadd(key, *value)
          elif ktype == 'zset' :
            value = r.zrange(key, 0, -1, withscores=True)
            rr.zadd(key, **dict(value))

          # Handle keys with an expire time set
          kttl = r.ttl(key)
          kttl = -1 if kttl is None else int(kttl)
          if kttl != -1:
            rr.expire(key, kttl)
          
          moved += 1

          if moved % 10000 == 0:
            print "%d keys have been resharded on %s at %s...\n" % (moved, servername, datetime.now().strftime("%Y-%m-%d %I:%M:%S"))

        r.set(self.shardprefix + "keymoved:" + servername, newkeymoved)
        print "%d keys have been resharded on %s at %s\n" % (newkeymoved, servername, datetime.now().strftime("%Y-%m-%d %I:%M:%S"))


  def flush_targets(self):
    """Function to flush all targets server in the new cluster.
    """
    
    for handle in self.targets_redis:
      server = handle[:handle.rfind('_')]
      db = handle[handle.rfind('_')+1:]
      servername = self.targets[server]['host'] + ":" + str(self.targets[server]['port']) + ":" + db
      print "Flushing server %s at %s...\n" % (servername, datetime.now().strftime("%Y-%m-%d %I:%M:%S"))
      r = self.targets_redis[handle]
      r.flushdb()
      print "Flushed server %s at %s...\n" % (servername, datetime.now().strftime("%Y-%m-%d %I:%M:%S"))


def main(sources, targets, databases, limit=None):
  sources_cluster = []
  for k in sources.split(','):
    so = k.split(':')
    if len(so) == 2:
      sources_cluster.append({'host':so[0], 'port':int(so[1])})
    else:
      exit("""Supplied sources addresses is wrong. e.g. python redis-sharding.py 127.0.0.1:6379,127.0.0.2:6379 node_1#127.0.0.1:63791,node_2#127.0.0.1:63792  0,1
      try : python redis-sharding.py --help""")

  targets_cluster = {}
  for k in targets.split(','):
    t = k.split('#')
    if len(t) == 2:
      so = t[1].split(':')
      if len(so) == 2:
        targets_cluster[t[0]] = {'host':so[0], 'port':int(so[1])}
      else:
        exit("""Supplied target addresses is wrong. e.g. python redis-sharding.py 127.0.0.1:6379,127.0.0.2:6379 node_1#127.0.0.1:63791,node_2#127.0.0.1:63792  0,1
        try : python redis-sharding.py --help""")
    else:
      exit("""Supplied target cluster format is wrong. e.g. python redis-sharding.py 127.0.0.1:6379,127.0.0.2:6379 node_1#127.0.0.1:63791,node_2#127.0.0.1:63792  0,1
      try : python redis-sharding.py --help""")

  dbs = [int(k) for k in databases.split(',')]
  if len(dbs) < 1:
    exit("""Supplied list of db is wrong. e.g. python redis-sharding.py 127.0.0.1:6379,127.0.0.2:6379 node_1#127.0.0.1:63791,node_2#127.0.0.1:63792  0,1
    try : python redis-sharding.py --help""")

  r = redis.Redis(connection_pool=redis.ConnectionPool(host=sources_cluster[0]['host'], port=sources_cluster[0]['port'], db=dbs[0]))

  rsd = RedisSharding(sources_cluster, targets_cluster, dbs)

  #check if script already running
  run = r.get(rsd.shardprefix + "run")
  if run is not None and int(run) == 1:
    exit('another process already running the script')

  r.set(rsd.shardprefix + 'run', 1)
  
  rsd.save_keylists()
  firstrun = r.get(rsd.shardprefix + "firstrun")
  firstrun = 0 if firstrun is None else int(firstrun)
  if firstrun == 0:
    rsd.flush_targets()
    r.set(rsd.shardprefix + "firstrun", 1)
  rsd.reshard_db(limit)

  r.set(rsd.shardprefix + 'run', 0)


def usage():
  print __doc__


if __name__ == "__main__":
  try:
    opts, args = getopt.getopt(sys.argv[1:], "hl:s:t:d:", ["help", "limit=", "sources=", "targets=", "databases="])
  except getopt.GetoptError:
    usage()
    sys.exit(2)
  for opt, arg in opts:
    if opt in ("-h", "--help"):
      usage()
      sys.exit()
    elif opt in ("-l", "--limit"): limit = arg
    elif opt in ("-s", "--sources"): sources = arg
    elif opt in ("-t", "--targets"): targets = arg
    elif opt in ("-d", "--databases"): databases = arg

  try:
    limit = int(limit)
  except (NameError, TypeError, ValueError):
    limit = None

  try:
    main(sources, targets, databases, limit)
  except NameError as e:
    usage()
