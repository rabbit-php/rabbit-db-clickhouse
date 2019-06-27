<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2019/1/23
 * Time: 14:52
 */

namespace rabbit\db\clickhouse;

use rabbit\core\ObjectFactory;
use rabbit\db\ConnectionInterface;
use rabbit\db\Exception;
use rabbit\helper\ArrayHelper;

/**
 * Class Manager
 * @package rabbit\db\clickhouse
 */
class Manager
{
    /** @var PdoPool[] */
    private $connections = [];
    /** @var array */
    private $yamlList = [];

    /**
     * Manager constructor.
     * @param array $configs
     */
    public function __construct(array $configs = [])
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
    public function getConnection(string $name = 'db'): ?Connection
    {
        if (!isset($this->connections[$name])) {
            if (empty($this->yamlList)) {
                return null;
            }
            $this->createByYaml();
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

    private function createByYaml(): void
    {
        foreach ($this->yamlList as $fileName) {
            foreach (yaml_parse_file($fileName) as $name => $dbconfig) {
                if (!isset($dbconfig['class']) || !class_exists($dbconfig['class']) || !$dbconfig['class'] instanceof ConnectionInterface) {
                    $dbconfig['class'] = Connection::class;
                }
                if (!isset($dbconfig['dsn'])) {
                    throw new Exception("The dsn must be set current class in $fileName");
                }
                $conn = [
                    'class' => $dbconfig['class'],
                    'dsn' => $dbconfig['dsn'],
                ];
                if (is_array(ArrayHelper::getValue($dbconfig, 'config'))) {
                    foreach ($config as $key => $value) {
                        $conn[$key] = $value;
                    }
                }
                $this->connections[$name] = ObjectFactory::createObject($conn, [], false);
            }
        }
    }
}