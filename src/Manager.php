<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2019/1/23
 * Time: 14:52
 */

namespace rabbit\db\clickhouse;

/**
 * Class Manager
 * @package rabbit\db\clickhouse
 */
class Manager extends \rabbit\db\Manager
{
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
}
