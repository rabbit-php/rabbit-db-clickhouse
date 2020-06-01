<?php

namespace rabbit\db\clickhouse;

use rabbit\App;
use rabbit\core\ObjectFactory;
use rabbit\db\ConnectionTrait;
use rabbit\db\Exception;
use rabbit\db\Expression;
use rabbit\exception\InvalidArgumentException;
use rabbit\helper\ArrayHelper;
use rabbit\pool\PoolInterface;
use rabbit\pool\PoolManager;
use rabbit\pool\PoolProperties;
use rabbit\socket\HttpClient;
use rabbit\socket\pool\SocketPool;

/**
 * Class Connection
 * @package rabbit\db\clickhouse
 */
class Connection extends \rabbit\db\Connection
{
    /**
     * @var string
     */
    protected $commandClass = Command::class;
    protected $schemaClass = Schema::class;

    public $schemaMap = [
        'clickhouse' => Schema::class
    ];

    /**
     * Connection constructor.
     * @param string $dsn
     * @param array $options
     */
    public function __construct($dsn)
    {
        if (is_string($dsn)) {
            $pool = ObjectFactory::createObject([
                'class' => SocketPool::class,
                'client' => HttpClient::class,
                'poolConfig' => ObjectFactory::createObject([
                    'class' => PoolProperties::class,
                    'maxWait' => 0,
                    'minActive' => 30,
                    'maxActive' => 36,
                    'timeout' => 120,
                    'uri' => [$dsn]
                ], [], false)
            ], [], false);
            $this->dsn = $dsn;
        } else {
            $pool = $dsn;
            $this->dsn = $pool->getConnectionAddress();
        }
        if (!$pool instanceof PoolInterface) {
            throw new InvalidArgumentException("Property pool not ensure PoolInterface");
        }
        $this->poolKey = $pool->getPoolConfig()->getName();
    }

    /**
     * @return HttpClient
     */
    public function getTransport(): HttpClient
    {
        return PoolManager::getPool($this->poolKey)->getConnection();
    }


    public function getIsActive()
    {
        return false;
    }

    /**
     * @param string $str
     * @return string
     * @throws \DI\DependencyException
     * @throws \DI\NotFoundException
     */
    public function quoteValue($str)
    {
        return $this->getSchema()->quoteValue($str);
    }

    public function quoteSql($sql)
    {
        return $sql;
    }


    public function ping()
    {
        $query = 'SELECT 1';
        /** @var HttpClient $client */
        $client = PoolManager::getPool($this->poolKey)->getConnection();
        $result = trim($client->post('/', $query)->getBody()) == '1';
        return $result;
    }


    /**
     * Closes the connection when this component is being serialized.
     * @return array
     */
    public function __sleep()
    {
        $this->close();
        return array_keys(get_object_vars($this));
    }


    /**
     * Closes the currently active DB connection.
     * It does nothing if the connection is already closed.
     */
    public function close()
    {
        if ($this->getIsActive()) {
            App::warning('Closing DB connection: ' . $this->shortDsn, 'clickhouse');
        }
    }

    /**
     * @return mixed|\rabbit\db\Schema
     * @throws \DI\DependencyException
     * @throws \DI\NotFoundException
     */
    public function getSchema()
    {
        if ($this->_schema !== null) {
            return $this->_schema;
        }
        return $this->_schema = new $this->schemaClass($this);
    }

    public function quoteTableName($name)
    {
        return $name;
    }

    public function getDriverName()
    {
        return 'clickhouse';
    }

    public function quoteColumnName($name)
    {
        return $name;
    }

    /**
     * @return \rabbit\db\QueryBuilder
     * @throws \DI\DependencyException
     * @throws \DI\NotFoundException
     */
    public function getQueryBuilder()
    {
        return $this->getSchema()->getQueryBuilder();
    }

    /**
     * @param ActiveRecord $model
     * @param array $array_columns
     * @return int
     * @throws \DI\DependencyException
     * @throws \DI\NotFoundException
     * @throws \rabbit\db\Exception
     */
    public function insertSeveral(ActiveRecord $model, array $array_columns): int
    {
        $sql = '';
        $params = array();
        $i = 0;
        if (ArrayHelper::isAssociative($array_columns)) {
            $array_columns = [$array_columns];
        }
        $keys = $model::primaryKey();
        if ($keys && !is_array($keys)) {
            $keys = [$keys];
        }
        foreach ($array_columns as $item) {
            $table = clone $model;
            //关联模型
            foreach ($table->getRelations() as $child => $val) {
                $key = explode("\\", $child);
                $key = strtolower(end($key));
                if (isset($item[$key])) {
                    $child_model = new $child();
                    if (!isset($item[$key][0])) {
                        $item[$key] = [$item[$key]];
                    }
                    foreach ($val as $c_attr => $p_attr) {
                        foreach ($item[$key] as $index => $params) {
                            $item[$key][$index][$c_attr] = $table->{$p_attr};
                        }
                    }
                    if ($this->updateSeveral($child_model, $item[$key]) === false) {
                        return false;
                    }
                }
            }
            $names = array();
            $placeholders = array();
            $table->load($item, '');
            $table->isNewRecord = false;
            if (!$table->validate()) {
                throw new Exception(implode(BREAKS, $table->getFirstErrors()));
            }
            if ($keys) {
                foreach ($keys as $key) {
                    if (isset($item[$key])) {
                        $table->$key = $item[$key];
                    }
                }
            }
            foreach ($table->toArray() as $name => $value) {
                if (!$i) {
                    $names[] = $this->quoteColumnName($name);
                    $updates[] = $this->quoteColumnName($name) . "=values(" . $this->quoteColumnName($name) . ")";
                }
                if ($value instanceof Expression) {
                    $placeholders[] = $value->expression;
                    foreach ($value->params as $n => $v) {
                        $params[$n] = $v;
                    }
                } else {
                    $placeholders[] = ':' . $name . $i;
                    $params[':' . $name . $i] = $value;
                }
            }
            if (!$i) {
                $sql = 'INSERT INTO ' . $this->quoteTableName($table::tableName())
                    . ' (' . implode(', ', $names) . ') VALUES ('
                    . implode(', ', $placeholders) . ')';
            } else {
                $sql .= ',(' . implode(', ', $placeholders) . ')';
            }
            $i++;
        }
        $table::getDb()->createCommand($sql, $params)->execute();
        return count($array_columns);
    }

    /**
     * @param ActiveRecord $model
     * @param array $array_columns
     * @return int
     * @throws \DI\DependencyException
     * @throws \DI\NotFoundException
     * @throws \rabbit\db\Exception
     */
    public function updateSeveral(ActiveRecord $model, array $array_columns): int
    {
        $params = array();
        if (ArrayHelper::isAssociative($array_columns)) {
            $array_columns = [$array_columns];
        }
        $keys = $model::primaryKey();
        if (empty($keys)) {
            throw new Exception("The table " . $model::tableName() . ' must have one or more primarykey to call updateSeveral function!');
        }
        if (!is_array($keys)) {
            $keys = [$keys];
        }
        $sql = 'ALTER TABLE ' . $this->quoteTableName($model::tableName()) . ' update ';
        $columns = array_keys(current($array_columns));
        $sets = [];
        $setQ = [];
        $bindings = [];
        $whereIn = [];
        $whereVal = [];
        $i = 0;
        foreach ($array_columns as $item) {
            $table = clone $model;
            //关联模型
            foreach ($table->getRelations() as $child => $val) {
                $key = explode("\\", $child);
                $key = strtolower(end($key));
                if (isset($item[$key])) {
                    $child_model = new $child();
                    if (!isset($item[$key][0])) {
                        $item[$key] = [$item[$key]];
                    }
                    foreach ($val as $c_attr => $p_attr) {
                        foreach ($item[$key] as $index => $param) {
                            $item[$key][$index][$c_attr] = $table->{$p_attr};
                        }
                    }
                    if ($this->updateSeveral($child_model, $item[$key]) === false) {
                        return false;
                    }
                }
            }
            $table->load($item, '');
            if (!$table->validate($columns)) {
                throw new Exception(implode(BREAKS, $table->getFirstErrors()));
            }
            foreach ($item as $name => $value) {
                if (in_array($name, $keys)) {
                    $whereIn[$name][] = '?';
                    $whereVal[] = $value;
                } else {
                    $i === 0 && $sets[$name] = " `$name`=multiIf(";
                    foreach ($keys as $key) {
                        $bindings[] = $item[$key];
                        $sets[$name] .= "`$key`==? and ";
                    }
                    $sets[$name] = rtrim($sets[$name], ' and ') . ',?,';
                    $bindings[] = $value;
                }
            }
            $i++;
        }
        foreach ($sets as $name => $value) {
            $sql .= $value . "`$name`)";
        }
        $sql .= " where ";
        foreach ($keys as $key) {
            $sql .= " `$key` in (" . implode(',', $whereIn[$key]) . ') and ';
        }
        $sql = rtrim($sql, "and ");
        $params = array_merge($bindings, $whereVal);
        $table::getDb()->createCommand($sql, $params)->execute();
        return count($array_columns);
    }

    /**
     * @param ActiveRecord $table
     * @param array $array_columns
     * @return int
     * @throws \rabbit\db\Exception
     */
    public function deleteSeveral(ActiveRecord $table, array $array_columns): int
    {
        $keys = $table::primaryKey();
        if (!is_array($keys)) {
            $keys = [$keys];
        }
        $condition = [];
        if (ArrayHelper::isAssociative($array_columns)) {
            $array_columns = [$array_columns];
        }
        foreach ($array_columns as $item) {
            $table->load($item, '');
            foreach ($table->getRelations() as $child => $val) {
                $key = explode("\\", $child);
                $key = strtolower(end($key));
                if (isset($item[$key])) {
                    $child_model = new $child();
                    if ($item[$key]) {
                        if ($this->deleteSeveral($child_model, $item[$key]) === false) {
                            return false;
                        }
                    }
                }
            }
            if ($keys) {
                foreach ($keys as $key) {
                    if (isset($item[$key])) {
                        $condition[$key][] = $item[$key];
                    }
                }
            }
        }
        if ($condition) {
            $table->deleteAll($condition);
            return count($array_columns);
        }
        return 0;
    }


}
