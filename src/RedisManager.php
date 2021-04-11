<?php

namespace Encore\Admin\RedisManager;

use Encore\Admin\Extension;
use Encore\Admin\RedisManager\DataType\DataType;
use Encore\Admin\RedisManager\DataType\Hashes;
use Encore\Admin\RedisManager\DataType\Lists;
use Encore\Admin\RedisManager\DataType\Sets;
use Encore\Admin\RedisManager\DataType\SortedSets;
use Encore\Admin\RedisManager\DataType\Strings;
use Illuminate\Http\Request;
use Illuminate\Redis\Connections\Connection;
use Illuminate\Redis\Connections\PhpRedisConnection;
use Illuminate\Redis\Connections\PredisConnection;
use Illuminate\Support\Arr;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Redis;
use Predis\Collection\Iterator\Keyspace;
use Predis\Pipeline\Pipeline;
use Redis as PhpRedis;

/**
 * Class RedisManager.
 */
class RedisManager extends Extension
{
    use BootExtension;

    /**
     * @var array
     */
    public static $typeColor = [
        'string' => 'primary',
        'list'   => 'info',
        'zset'   => 'danger',
        'hash'   => 'warning',
        'set'    => 'success',
    ];

    /**
     * @var array
     */
    protected $dataTyps = [
        'string' => Strings::class,
        'hash'   => Hashes::class,
        'set'    => Sets::class,
        'zset'   => SortedSets::class,
        'list'   => Lists::class,
    ];

    /**
     * @var array
     */
    protected $dataTypePhpRedis;

    /**
     * @var RedisManager
     */
    protected static $instance;

    /**
     * @var string
     */
    protected $connection;

    /**
     * @var string
     */
    protected $prefix;

    /**
     * Get instance of redis manager.
     *
     * @param string $connection
     *
     * @return RedisManager
     */
    public static function instance($connection = 'default')
    {
        if (!static::$instance instanceof self) {
            static::$instance = new static($connection);
        }

        return static::$instance;
    }

    /**
     * RedisManager constructor.
     *
     * @param string $connection
     */
    public function __construct($connection = 'default')
    {
        $this->connection = $connection;

        if ($this->isPhpRedis()) {
            $this->dataTypePhpRedis = $this->getDataTypePhpRedis();
        }
    }

    /**
     * @return string
     */
    public function getPrefix()
    {
        if (!isset($this->prefix) || is_null($this->prefix)) {
            $this->prefix = config('database.redis.options.prefix', '');
        }

        return $this->prefix;
    }

    public function getDataTypePhpRedis()
    {
        return [
            PhpRedis::REDIS_STRING => 'string',
            PhpRedis::REDIS_SET => 'set',
            PhpRedis::REDIS_LIST => 'list',
            PhpRedis::REDIS_ZSET => 'zset',
            PhpRedis::REDIS_HASH => 'hash',
            PhpRedis::REDIS_STREAM => 'stream',
            PhpRedis::REDIS_NOT_FOUND => 'none',
        ];
    }

    /**
     * @param string $key
     *
     * @return array|string|null
     */
    public function trimPrefix($key)
    {
        if (!$key) {
            return $key;
        }

        $prefix = $this->getPrefix();

        return preg_replace("/^$prefix/", '', $key);
    }

    /**
     * @return Lists
     */
    public function list()
    {
        return new Lists($this->getConnection());
    }

    /**
     * @return Strings
     */
    public function string()
    {
        return new Strings($this->getConnection());
    }

    /**
     * @return Hashes
     */
    public function hash()
    {
        return new Hashes($this->getConnection());
    }

    /**
     * @return Sets
     */
    public function set()
    {
        return new Sets($this->getConnection());
    }

    /**
     * @return SortedSets
     */
    public function zset()
    {
        return new SortedSets($this->getConnection());
    }

    /**
     * Get connection collections.
     *
     * @return Collection
     */
    public function getConnections()
    {
        return collect(config('database.redis'))->filter(function ($conn) {
            return is_array($conn);
        });
    }

    /**
     * Get a registered connection instance.
     *
     * @param string $connection
     *
     * @return Connection
     */
    public function getConnection($connection = null)
    {
        if ($connection) {
            $this->connection = $connection;
        }

        return Redis::connection($this->connection);
    }

    /**
     * Judge the registered connection instance is phpredis or not.
     *
     * @return bool
     */
    public function isPhpRedis()
    {
        return $this->getConnection() instanceof PhpRedisConnection;
    }

    /**
     * Judge the registered connection instance is predis or not.
     *
     * @return bool
     */
    public function isPredis()
    {
        return $this->getConnection() instanceof PredisConnection;
    }

    /**
     * Get information of redis instance.
     *
     * @return array
     */
    public function getInformation()
    {
        if ($this->isPhpRedis()) {
            $info = [];
            $info['Server'] = $this->getConnection()->info('SERVER');
            $info['Clients'] = $this->getConnection()->info('CLIENTS');
            $info['Memory'] = $this->getConnection()->info('MEMORY');
            $info['Persistence'] = $this->getConnection()->info('PERSISTENCE');
            $info['Stats'] = $this->getConnection()->info('STATS');
            $info['Replication'] = $this->getConnection()->info('REPLICATION');
            $info['CPU'] = $this->getConnection()->info('CPU');
            $info['Cluster'] = $this->getConnection()->info('CLUSTER');
            $info['Keyspace'] = $this->getConnection()->info('KEYSPACE');

            return $info;
        }

        return $this->getConnection()->info();
    }

    /**
     * Scan keys in redis by giving pattern.
     *
     * @param string $pattern
     * @param int    $count
     *
     * @return array|\Predis\Pipeline\Pipeline
     */
    public function scan($pattern = '*', $count = 100)
    {
        $client = $this->getConnection();
        $keys = [];

        $pattern = $this->getPrefix().$pattern;
        if ($this->isPredis()) {
            foreach (new Keyspace($client->client(), $pattern) as $item) {
                $keys[] = $item;

                if (count($keys) == $count) {
                    break;
                }
            }
        } else {
            $iterator = null;
            $keys = $client->client()->scan($iterator, $pattern, $count);
        }

        $script = <<<'LUA'
        local type = redis.call('type', KEYS[1])
        local ttl = redis.call('ttl', KEYS[1])

        return {KEYS[1], type, ttl}
LUA;

        if ($this->isPredis()) {
            $keys = $client->pipeline(function (Pipeline $pipe) use ($keys, $script) {
                foreach ($keys as $key) {
                    $key = $this->trimPrefix($key);
                    $pipe->eval($script, 1, $key);
                }
            });
            $keys = array_map(function ($key) {
                $key[1] = $key[1]->getPayload();
                return $key;
            }, $keys);
        } else {
            $keys = array_map(function ($key) use ($client) {
                $key = $this->trimPrefix($key);
                return [
                    '0' => $this->getPrefix() . $key,
                    '1' => Arr::get($this->dataTypePhpRedis, $client->type($key)),
                    '2' => $client->ttl($key)
                ];
            }, $keys);
        }

        return $keys;
    }

    /**
     * Fetch value of a giving key.
     *
     * @param string $key
     *
     * @return array
     */
    public function fetch($key)
    {
        $key = $this->trimPrefix($key);
        if (!$this->getConnection()->exists($key)) {
            return [];
        }

        if ($this->isPhpRedis()) {
            $type = Arr::get($this->dataTypePhpRedis, $this->getConnection()->type($key));
        } else {
            $type = $this->getConnection()->type($key)->__toString();
        }

        /** @var DataType $class */
        $class = $this->{$type}();

        $value = $class->fetch($key);
        $ttl = $class->ttl($key);
        $key = $this->getPrefix().$key;

        return compact('key', 'value', 'ttl', 'type');
    }

    /**
     * Update a specified key.
     *
     * @param Request $request
     *
     * @return bool
     */
    public function update(Request $request)
    {
        $key = $request->get('key');
        $type = $request->get('type');

        $key = $this->trimPrefix($key);
        $params = $request->all();
        $params['key'] = $key;

        /** @var DataType $class */
        $class = $this->{$type}();

        $class->update($params);
        $class->setTtl($key, $request->get('ttl'));
    }

    /**
     * Remove the specified key.
     *
     * @param string $key
     *
     * @return int
     */
    public function del($key)
    {
        if (is_string($key)) {
            $key = [$key];
        }

        if (is_array($key)) {
            foreach ($key as $index => $key_item) {
                $key[$index] = $this->trimPrefix($key_item);
            }
        }

        return $this->getConnection()->del($key);
    }

    /**
     * 运行redis命令.
     *
     * @param string $command
     *
     * @return mixed
     */
    public function execute($command)
    {
        $command = explode(' ', $command);

        return $this->getConnection()->executeRaw($command);
    }

    /**
     * @param string $type
     *
     * @return mixed
     */
    public static function typeColor($type)
    {
        return Arr::get(static::$typeColor, $type, 'default');
    }
}
