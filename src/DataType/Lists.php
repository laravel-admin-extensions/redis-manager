<?php

namespace Encore\Admin\RedisManager\DataType;

use Illuminate\Support\Arr;

class Lists extends DataType
{
    /**
     * {@inheritdoc}
     */
    public function fetch(string $key)
    {
        return $this->getConnection()->lrange($key, 0, -1);
    }

    /**
     * {@inheritdoc}
     */
    public function update(array $params)
    {
        $key = Arr::get($params, 'key');

        if (Arr::has($params, 'push')) {
            $item = Arr::get($params, 'item');
            $command = $params['push'] == 'left' ? 'lpush' : 'rpush';

            $this->getConnection()->{$command}($key, $item);
        }

        if (Arr::has($params, '_editable')) {
            $value = Arr::get($params, 'value');
            $index = Arr::get($params, 'pk');

            $this->getConnection()->lset($key, $index, $value);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function store(array $params)
    {
        $key = Arr::get($params, 'key');
        $item = Arr::get($params, 'item');
        $ttl = Arr::get($params, 'ttl');

        $this->getConnection()->rpush($key, [$item]);

        if ($ttl > 0) {
            $this->getConnection()->expire($key, $ttl);
        }

        return redirect(route('redis-edit-key', [
            'conn' => request('conn'),
            'key'  => $key,
        ]));
    }

    /**
     * Remove a member from list by index.
     *
     * @param array $params
     *
     * @return mixed
     */
    public function remove(array $params)
    {
        $key = Arr::get($params, 'key');
        $index = Arr::get($params, 'index');

        $lua = <<<'LUA'
redis.call('lset', KEYS[1], ARGV[1], '__DELETED__');
redis.call('lrem', KEYS[1], 1, '__DELETED__');
LUA;

        return $this->getConnection()->eval($lua, 1, $key, $index);
    }
}
