<?php
/**
 * Redis database connection class
 *
 */
class SmartRedisCluster {

  /**
   * servers array used during construct
   * @var array
   * @access public
   */
  public $servers;

  /**
   * number of servers array used during construct
   * @var array
   * @access public
   */
  public $no_servers;

  /**
	 * Collection of Redis objects attached to Redis servers
	 * @var array
	 * @access private
	 */
	private $redises;

  /**
   * client connection to the Redis server
   * @var resource
   * @access private
   */
  private $__redis;



  /**
	 * The commands that are not subject to hashing
	 * @var array
	 * @access private
	 */
	private static $read_keys = array(
		'debug', 'object', 'exists', 'getbit',
    'get', 'getrange', 'hexists', 'hget',
    'hgetall', 'hkeys', 'hlen', 'hmget',
    'hvals', 'keys', 'lindex', 'llen',
    'lrange', 'mget', 'object', 'psubscribe',
    'scard', 'sismember', 'smembers',
    'srandmember', 'strlen', 'ttl', 'type',
    'zcard', 'zcount', 'zrange', 'zrangebyscore',
    'zrank', 'zrevrange', 'zrevrangebyscore',
    'zrevrank', 'zscore',

    'getMultiple', 'lSize', 'lGetRange',
    'sContains', 'sSize', 'sGetMembers',
    'getKeys', 'zSize',
	);

  /**
	 * The commands that are not subject to hashing
	 * @var array
	 * @access private
	 */
	private static $write_keys = array(
    'append', 'blpop', 'brpop', 'brpoplpush',
    'decr', 'decrby', 'del',
    'expire', 'expireat', 'getset', 'hdel',
    'hincrby', 'hmset', 'hset', 'hsetnx',
    'incr', 'incrby', 'linsert', 'lpop',
    'lpush', 'lpushx', 'lrem', 'lset',
    'ltrim', 'move', 'mset', 'msetnx',
    'persist', 'publish', 'punsubscribe', 'rename',
    'renamenx', 'rpop', 'rpoplpush', 'rpush',
    'rpushx', 'sadd', 'sdiff', 'sdiffstore',
    'set', 'setbit', 'setex', 'setnx',
    'setrange', 'sinter', 'sinterstore', 'smove',
    'sort', 'spop', 'srem', 'subscribe',
    'sunion', 'sunionstore', 'unsubscribe', 'unwatch',
    'watch', 'zadd', 'zincrby', 'zinterstore',
    'zrem', 'zremrangebyrank', 'zremrangebyscore', 'zunionstore',

    'del', 'listTrim', 'lRemove', 'sRemove',
    'renameKey', 'setTimeout', 'zDelete',
    'zDeleteRangeByScore', 'zDeleteRangeByRank',
	);

    /**
	 * The commands that are not subject to hashing
	 * @var array
	 * @access private
	 */
	private static $dont_hash = array(
		'auth', 'bgrewriteaof', 'bgsave', 'config',
    'dbsize', 'flushall', 'flushdb', 'info',
    'lastsave', 'monitor', 'ping', 'quit',
    'randomkey', 'save', 'select', 'shutdown',
    'slaveof', 'slowlog', 'sync',

    'discard', 'echo', 'exec', 'multi',

    'setOption', 'getOption'
	);


  /**
	 * Creates a Redis interface to a cluster of Redis servers
	 * @param array $servers The Redis servers in the cluster.
   * should be in the format array (
                                  # node names
                                  'nodes' => array(
                                    'node_1' => array('host' => '127.0.0.1', 'port' => '63790'),
                                    'node_2' => array('host' => '127.0.0.1', 'port' => '63791'),
                                  ),

                                  # replication information
                                  'master_of' => array(
                                    'node_1' => 'node_2',
                                  )
                                ),
	 */
	function __construct($servers, $redisdb = 0) {
    if(empty($servers['nodes']) || empty($servers['master_of'])) {
      error_log("SmartRedisCluster: Please set a correct array of redis servers.", 0); die();
    }
		$this->__redis = new Redis();
    $this->servers = $servers;
    $this->no_servers = count($servers['master_of']);
    foreach ($servers['nodes'] as $alias => $server) {
      try {
        $this->__redis->connect($server['host'], $server['port'], 3);
        $this->__redis->select($redisdb);
        $this->redises[$alias] =  $this->__redis;
      } catch(RedisException $e) {
        $ms = array_search($alias, $this->servers['master_of']);
        if(!empty($ms)) {
          $this->__redis->connect($servers['nodes'][$ms]['host'], $servers['nodes'][$ms]['port'], 3);
          $this->__redis->select($redisdb);
          $this->redises[$alias] =  $this->__redis;
        } else {
          error_log("SmartRedisCluster cannot connect to: " . $server['host'] .':'. $server['port'], 0); die();
        }
      }
		}
	}


  /**
	 * Routes a command to a specific Redis server aliased by {$alias}.
	 * @param string $alias The alias of the Redis server
	 * @return Redis The Redis object attached to the Redis server
	 */
	function to($alias) {
		if (isset($this->redises[$alias])) {
			return $this->redises[$alias];
		}
		else {
      error_log("SmartRedisCluster: That Redis node does not exist.", 0); die();
		}
	}



  /**
	 * Magic method to handle all function requests and prefix key based
	 * operations with the 'resque:' key prefix.
	 *
	 * @param string $name The name of the method called.
	 * @param array $args Array of supplied arguments to the method.
	 * @return mixed Return value from Redis::call() based on the command.
	 */
  function __call($name, $args){
    /* Pick a server node to send the command to */
    $hkey = $args[0];
    if (is_array($args[0])) { //take care key tags $redis->get(array('userinfo', "age:$uid"))
      $hkey = $args[0][0];
      $args[0] = $args[0][1];
    }
    $node = (crc32($hkey) % $this->no_servers) + 1;
    $redisent = $this->redises[$this->servers['default_node']];
    if (in_array($name, self::$write_keys)) {
      $redisent = $this->redises['node_' . $node];
    } else if (in_array($name, self::$read_keys)) {
      $redisent = $this->redises[$this->servers['master_of']['node_' . $node]];
    }
		// Execute the command on the server
    return call_user_func_array(array($redisent, $name), $args);
  }
}
