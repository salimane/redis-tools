#!/usr/bin/env python
import redis
from datetime import datetime
import binascii
import sys

class Resharding:
  """A class for resharding keys from some servers to a new cluster
  python resharding.py """
  shardprefix = 'rsk:'
  keylistprefix = 'keylist:'
  hkeylistprefix = 'havekeylist:'
  limit = 100000

  def __init__(self, cluster_old, cluster_new, dbs):
    self.cluster_old = cluster_old
    self.cluster_new = cluster_new
    self.len_cluster_new = len(cluster_new)
    self.dbs = dbs

  def save_keylists(self):
    for server in self.cluster_old:
      for db in self.dbs:
        servername = server['host'] + ":" + str(server['port']) + ":" + str(db)
        print "Processing temp keylists on server %s at %s...\n" % (servername, datetime.now().strftime("%Y-%m-%d %I:%M:%S"))
        pool = redis.ConnectionPool(host=server['host'], port=server['port'], db=db)
        r = redis.Redis(connection_pool=pool)
        dbsize = r.dbsize()
        hkl = r.get(self.shardprefix + self.hkeylistprefix + servername)
        if hkl == None or int(hkl) != 1:
          self.keys_to_list(r, servername)
        print "ALL %d keys of %s already inserted to temp keylist ...\n\n" % (dbsize-1, servername)


  def keys_to_list(self, oldredis, servername):
    print "Saving the keys in %s to temp keylist...\n" % servername
    moved = 0
    oldredis.delete(self.shardprefix + self.keylistprefix + servername)
    for key in oldredis.keys('*'):
      moved += 1
      oldredis.rpush(self.shardprefix + self.keylistprefix + servername, key)
      if moved % self.limit == 0:
        print  "%d keys of %s inserted in temp keylist at %s...\n" % (moved, servername, datetime.now().strftime("%Y-%m-%d %I:%M:%S"))

    oldredis.set(self.shardprefix + self.hkeylistprefix + servername, 1)


  def reshard_db(self):
    for server in self.cluster_old:
      for db in self.dbs:
        servername = server['host'] + ":" + str(server['port']) + ":" + str(db)
        print "Processing keys resharding of server %s at %s...\n" % (servername, datetime.now().strftime("%Y-%m-%d %I:%M:%S"))
        pool = redis.ConnectionPool(host=server['host'], port=server['port'], db=db)
        oldredis = redis.Redis(connection_pool=pool)
        moved = 0
        dbsize = oldredis.dbsize() - 1
        keymoved = oldredis.get(self.shardprefix + "keymoved:" + servername)
        keymoved = 0 if keymoved == None else int(keymoved)
        #check if we already have all usernames in tt
        if dbsize <= keymoved:
          print "ALL %d keys from %s have already been resharded.\n" % (dbsize, servername)
          continue

        print "Started resharding of %s keys from %d to %d at %s...\n" % (servername, keymoved, dbsize, datetime.now().strftime("%Y-%m-%d %I:%M:%S"))
        newkeymoved = keymoved+self.limit if dbsize > keymoved+self.limit else dbsize
        for key in oldredis.lrange(self.shardprefix + self.keylistprefix + servername, keymoved, newkeymoved):
          ktype = oldredis.type(key)
          if ktype == 'string' :
            value = oldredis.get(key)
          elif ktype == 'hash' :
            value = oldredis.hgetall(key)
          elif ktype == 'list' :
            if key == self.shardprefix + "keylist:" + servername:
              continue
            value = oldredis.lrange(key, 0, -1)
          elif ktype == 'set' :
            value = oldredis.smembers(key)
          elif ktype == 'zset' :
            value = oldredis.zrange(key, 0, -1, withscores=True)
          elif ktype == 'none' :
            continue

          self.reshard_key(ktype, key, value, db)
          moved += 1

          if moved % 10000 == 0:
            print "%d keys have been resharded on %s at %s...\n" % (moved, servername, datetime.now().strftime("%Y-%m-%d %I:%M:%S"))

        oldredis.set(self.shardprefix + "keymoved:" + servername, newkeymoved)
        print "%d keys have been resharded on %s at %s\n" % (newkeymoved, servername, datetime.now().strftime("%Y-%m-%d %I:%M:%S"))


  def reshard_key(self, type, key, value, db):
    node = str((abs(binascii.crc32(key) & 0xffffffff) % self.len_cluster_new) + 1)
    poolnew = redis.ConnectionPool(host=self.cluster_new['node_'+node]['host'], port=self.cluster_new['node_'+node]['port'], db=db)
    newredis = redis.Redis(connection_pool=poolnew)
    if type == 'string' :
      newredis.set(key, value)
    elif type == 'hash' :
      newredis.hmset(key, value)
    elif type == 'list' :
      newredis.rpush(key, *value)
    elif type == 'set' :
      newredis.sadd(key, *value)
    elif type == 'zset' :
      for k,v in value:
        newredis.zadd(key, k, v)


  def flush_cluster_new(self):
    for server in self.cluster_new:
      for db in self.dbs:
        servername = self.cluster_new[server]['host'] + ":" + str(self.cluster_new[server]['port']) + ":" + str(db)
        print "Flushing server %s at %s...\n" % (servername, datetime.now().strftime("%Y-%m-%d %I:%M:%S"))
        pool = redis.ConnectionPool(host=self.cluster_new[server]['host'], port=self.cluster_new[server]['port'], db=db)
        newredis = redis.Redis(connection_pool=pool)
        newredis.flushdb()
        print "Flushed server %s at %s...\n" % (servername, datetime.now().strftime("%Y-%m-%d %I:%M:%S"))


def main(old, new, db):
  cluster_old = []
  for k in old.split(','):
    so = k.split(':')
    if len(so) == 2:
      cluster_old.append({'host':so[0], 'port':int(so[1])})
    else:
      exit('Supplied old server address is wrong. e.g. python resharding.py 127.0.0.1:6379,127.0.0.2:6379 node_1#127.0.0.1:63791,node_2#127.0.0.1:63792  0,1')

  cluster_new = {}
  for k in new.split(','):
    t = k.split('#')
    if len(t) == 2:
      so = t[1].split(':')
      if len(so) == 2:
        cluster_new[t[0]] = {'host':so[0], 'port':int(so[1])}
      else:
        exit('Supplied old server address is wrong. e.g. python resharding.py 127.0.0.1:6379,127.0.0.2:6379 node_1#127.0.0.1:63791,node_2#127.0.0.1:63792  0,1')
    else:
      exit('Supplied new cluster format is wrong. e.g. python resharding.py 127.0.0.1:6379,127.0.0.2:6379 node_1#127.0.0.1:63791,node_2#127.0.0.1:63792  0,1')

  dbs = [int(k) for k in db.split(',')]
  if len(dbs) < 1:
    exit('Supplied list of db is wrong. e.g. python resharding.py 127.0.0.1:6379,127.0.0.2:6379 node_1#127.0.0.1:63791,node_2#127.0.0.1:63792  0,1')

  pool = redis.ConnectionPool(host=cluster_old[0]['host'], port=cluster_old[0]['port'], db=dbs[0])
  r = redis.Redis(connection_pool=pool)

  rsd = Resharding(cluster_old, cluster_new, dbs)

  #check if script already running
  run = r.get(rsd.shardprefix + "run")
  if run != None and int(run) == 1:
    exit('another process already running the script')

  rsd.save_keylists()
  firstrun = r.get(rsd.shardprefix + "firstrun")
  firstrun = 0 if firstrun == None else int(firstrun)
  if firstrun == 0:
    rsd.flush_cluster_new()
    r.set(rsd.shardprefix + "firstrun", 1)
  rsd.reshard_db()

  r.set(rsd.shardprefix + 'run', 0)


if __name__ == "__main__":
  args = sys.argv[1:]
  if len(args) != 3:
    exit('Provide exactly 3 araguments. e.g. python resharding.py 127.0.0.1:6379,127.0.0.2:6379 node_1#127.0.0.1:63791,node_2#127.0.0.1:63792  0,1')
  old, new, db = args[0], args[1], args[2]
  main(old, new, db)