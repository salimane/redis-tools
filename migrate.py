#!/usr/bin/env python
import redis
from datetime import datetime
import sys

class Migrate:
  """A class for migrating keys from one server to another.
  python migrate.py <ip:port> <ip:port>  <db,db,..>
  python migrate.py 127.0.0.1:6379 127.0.0.1:6379  0,1
  """
  mprefix = 'mig:'
  keylistprefix = 'keylist:'
  hkeylistprefix = 'havekeylist:'
  limit = 100000

  def __init__(self, server_old, server_new, dbs):
    self.server_old = server_old
    self.server_new = server_new
    self.dbs = dbs

  def save_keylists(self):
    for db in self.dbs:
      servername = self.server_old['host'] + ":" + str(self.server_old['port']) + ":" + str(db)
      print "Processing temp keylists on server %s at %s...\n" % (servername, datetime.now().strftime("%Y-%m-%d %I:%M:%S"))
      pool = redis.ConnectionPool(host=self.server_old['host'], port=self.server_old['port'], db=db)
      r = redis.Redis(connection_pool=pool)
      dbsize = r.dbsize()
      hkl = r.get(self.mprefix + self.hkeylistprefix + servername)
      if hkl == None or int(hkl) != 1:
        self.keys_to_list(r, servername)
      print "ALL %d keys of %s already inserted to temp keylist ...\n\n" % (dbsize-1, servername)


  def keys_to_list(self, oldredis, servername):
    print "Saving the keys in %s to temp keylist...\n" % servername
    moved = 0
    oldredis.delete(self.mprefix + self.keylistprefix + servername)
    for key in oldredis.keys('*'):
      moved += 1
      oldredis.rpush(self.mprefix + self.keylistprefix + servername, key)
      if moved % self.limit == 0:
        print  "%d keys of %s inserted in temp keylist at %s...\n" % (moved, servername, datetime.now().strftime("%Y-%m-%d %I:%M:%S"))

    oldredis.set(self.mprefix + self.hkeylistprefix + servername, 1)


  def migrate_db(self):
      for db in self.dbs:
        servername = self.server_old['host'] + ":" + str(self.server_old['port']) + ":" + str(db)
        print "Processing keys migration of server %s at %s...\n" % (servername, datetime.now().strftime("%Y-%m-%d %I:%M:%S"))
        pool = redis.ConnectionPool(host=self.server_old['host'], port=self.server_old['port'], db=db)
        oldredis = redis.Redis(connection_pool=pool)
        moved = 0
        dbsize = oldredis.dbsize() - 1
        keymoved = oldredis.get(self.mprefix + "keymoved:" + servername)
        keymoved = 0 if keymoved == None else int(keymoved)
        #check if we already have all usernames in tt
        if dbsize < keymoved:
          print "ALL %d keys from %s have already been migrated.\n" % (dbsize, servername)
          continue


        print "Started migration of %s keys from %d to %d at %s...\n" % (servername, keymoved, dbsize, datetime.now().strftime("%Y-%m-%d %I:%M:%S"))

        poolnew = redis.ConnectionPool(host=self.server_new['host'], port=self.server_new['port'], db=db)
        newredis = redis.Redis(connection_pool=poolnew)

        newkeymoved = keymoved+self.limit if dbsize > keymoved+self.limit else dbsize
        for key in oldredis.lrange(self.mprefix + self.keylistprefix + servername, keymoved, newkeymoved):
          ktype = oldredis.type(key)
          if ktype == 'string' :
            value = oldredis.get(key)
          elif ktype == 'hash' :
            value = oldredis.hgetall(key)
          elif ktype == 'list' :
            if key == self.mprefix + "keylist:" + servername:
              continue
            value = oldredis.lrange(key, 0, -1)
          elif ktype == 'set' :
            value = oldredis.smembers(key)
          elif ktype == 'zset' :
            value = oldredis.zrange(key, 0, -1, withscores=True)
          elif ktype == 'none' :
            continue

          self.migrate_key(ktype, key, value, newredis)
          moved += 1

          if moved % 10000 == 0:
            print "%d keys have been migrated on %s at %s...\n" % (moved, servername, datetime.now().strftime("%Y-%m-%d %I:%M:%S"))

        oldredis.set(self.mprefix + "keymoved:" + servername, newkeymoved)
        print "%d keys have been migrated on %s at %s\n" % (newkeymoved, servername, datetime.now().strftime("%Y-%m-%d %I:%M:%S"))


  def migrate_key(self, type, key, value, newredis):
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


  def flush_server_new(self):
    for db in self.dbs:
      servername = self.server_new['host'] + ":" + str(self.server_new['port']) + ":" + str(db)
      print "Flushing server %s at %s...\n" % (servername, datetime.now().strftime("%Y-%m-%d %I:%M:%S"))
      pool = redis.ConnectionPool(host=self.server_new['host'], port=self.server_new['port'], db=db)
      newredis = redis.Redis(connection_pool=pool)
      newredis.flushdb()
      print "Flushed server %s at %s...\n" % (servername, datetime.now().strftime("%Y-%m-%d %I:%M:%S"))


def main(old, new, db):
  if (old == new):
    exit('The 2 servers adresses are the same. e.g. python migrate.py 127.0.0.1:6379 127.0.0.1:63791  0,1')
  so = old.split(':')
  if len(so) == 2:
    server_old = {'host':so[0], 'port':int(so[1])}
  else:
    exit('Supplied old server address is wrong. e.g. python migrate.py 127.0.0.1:6379 127.0.0.1:63791  0,1')
  sn = new.split(':')
  if len(sn) == 2:
    server_new = {'host':sn[0], 'port':int(sn[1])}
  else:
    exit('Supplied new server address is wrong. e.g. python migrate.py 127.0.0.1:6379 127.0.0.1:63791  0,1')
  dbs = [int(k) for k in db.split(',')]
  if len(dbs) < 1:
    exit('Supplied list of db is wrong. e.g. python migrate.py 127.0.0.1:6379 127.0.0.1:63791  0,1')

  pool = redis.ConnectionPool(host=server_old['host'], port=server_old['port'], db=dbs[0])
  r = redis.Redis(connection_pool=pool)

  mig = Migrate(server_old, server_new, dbs)

  #check if script already running
  run = r.get(mig.mprefix + "run")
  if run != None and int(run) == 1:
    exit('another process already running the script')

  mig.save_keylists()

  firstrun = r.get(mig.mprefix + "firstrun")
  firstrun = 0 if firstrun == None else int(firstrun)
  if firstrun == 0:
    mig.flush_server_new()
    r.set(mig.mprefix + "firstrun", 1)

  mig.migrate_db()

  r.set(mig.mprefix + 'run', 0)

if __name__ == "__main__":
  args = sys.argv[1:]
  if len(args) != 3:
    exit('Provide exactly 3 araguments. e.g. python migrate.py 127.0.0.1:6379 127.0.0.1:63791  0,1')
  old, new, db = args[0], args[1], args[2]
  main(old, new, db)