<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2019/1/23
 * Time: 14:52
 */

namespace rabbit\db\clickhouse;

use rabbit\helper\ArrayHelper;

/**
 * Class Manager
 * @package rabbit\db\clickhouse
 */
class Manager
{
    /** @var PdoPool[] */
    private $connections = [];

    /**
     * Manager constructor.
     * @param array $configs
     */
    public function __construct(array $configs)
    {
        $this->addConnection($configs);
    }

    /**
     * @param array $configs
     */
    public function addConnection(array $configs): void
    {
        foreach ($configs as $name => $connection) {
            if (!isset($this->connections[$name])) {
                $this->connections[$name] = $connection;
            }
        }
    }

    /**
     * @param string $name
     * @return Connection
     */
    public function getConnection(string $name = 'db'): Connection
    {
        if(!isset($this->connections[$name])){
            return null;
        }
        return $this->connections[$name];
    }

    /**
     * @param string $name
     * @return bool
     */
    public function hasConnection(string $name): bool
    {
        return isset($this->connections[$name]);
    }
}