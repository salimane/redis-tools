#**Redis Copy**

Redis Copy the keys in a source redis server into another target redis server.
The script probably needs to be added to a cron job if the keys are a lot because it only copies a fix number of keys at a time
and continue from there on the next run. It does this until there is no more keys to copy

####Usage:

    python redis-copy.py [options]

####Options:

    -l ..., --limit=...         optional numbers of keys to copy per run, if not defined 10000 is the default . e.g. 1000
    -s ..., --source=...        source redis server "ip:port" to copy keys from. e.g. 192.168.0.99:6379
    -t ..., --target=...        target redis server "ip:port" to copy keys to. e.g. 192.168.0.101:6379
    -d ..., --databases=...     comma separated list of redis databases to select when copying. e.g. 2,5
    -h, --help                  show this help
    --clean                     clean all variables, temp lists created previously by the script


####Examples:

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




#**Redis sharding**

Reshard the keys in a number of source redis servers into another number of target cluster of redis servers
in order to scale an application.
The script probably needs to be added to a cron job if the keys are a lot because it only reshards a fix number of keys at a time
and continue from there on the next run. It does this until there is no more keys to reshard

####Usage:

    python redis-sharding.py [options]

####Options:

    -l ..., --limit=...         optional numbers of keys to reshard per run, if not defined 10000 is the default . e.g. 1000
    -s ..., --sources=...       comma separated list of source redis servers "ip:port" to fetch keys from. e.g. 192.168.0.99:6379,192.168.0.100:6379
    -t ..., --targets=...       comma separated list target redis servers "node_i#ip:port" to reshard the keys to. e.g. node_1#192.168.0.101:6379,node_2#192.168.0.102:6379,node_3#192.168.0.103:6379
    -d ..., --databases=...     comma separated list of redis databases to select when resharding. e.g. 2,5
    -h, --help                  show this help


####IMPORTANT:

This script assume your target redis cluster of servers is based on a  node system,
which is simply a host:port pair that points to a single redis-server instance.
Each node is given a symbolic node name "node_i" where i is the number gotten from this hashing system

    str((abs(binascii.crc32(key) & 0xffffffff) % len(targets)) + 1)
to uniquely identify it in a way that doesn’t tie it to a specific host (or port).
e.g.

    config = {
      'node_1':{'host':'192.168.0.101', 'port':6379},
      'node_2':{'host':'192.168.0.102', 'port':6379},
      'node_3':{'host':'192.168.0.103', 'port':6379},
    }



####Examples:

    python redis-sharding.py --help                                show this doc
    python redis-sharding.py \
    --sources=192.168.0.99:6379,192.168.0.100:6379 \
    --targets=node_1#192.168.0.101:6379,node_2#192.168.0.102:6379,node_3#192.168.0.103:6379 \
    --databases=2,5

    python redis-sharding.py --limit=1000 \
    --sources=192.168.0.99:6379,192.168.0.100:6379 \
    --targets=node_1#192.168.0.101:6379,node_2#192.168.0.102:6379,node_3#192.168.0.103:6379 \
    --databases=2,5
