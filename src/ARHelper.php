<?php

declare(strict_types=1);

namespace Rabbit\DB\ClickHouse;

use Rabbit\ActiveRecord\BaseActiveRecord;
use Rabbit\Base\Core\Context;
use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Exception\NotSupportedException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\DB\DBHelper;
use Rabbit\DB\Exception;
use Rabbit\DB\Expression;
use Rabbit\Pool\ConnectionInterface;
use ReflectionException;
use Throwable;

/**
 * Class ARHelper
 * @package Rabbit\DB\ClickHouse
 */
class ARHelper extends \Rabbit\ActiveRecord\ARHelper
{
    /**
     * @param BaseActiveRecord $model
     * @param array $array_columns
     * @return int
     * @throws Exception
     * @throws InvalidConfigException
     * @throws ReflectionException
     * @throws Throwable
     */
    public static function insertSeveral(BaseActiveRecord $model, array $array_columns): int
    {
        $sql = '';
        $params = array();
        $i = 0;
        if (ArrayHelper::isAssociative($array_columns)) {
            $array_columns = [$array_columns];
        }
        $keys = $model->primaryKey();
        $conn = $model->getDb();
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
                    if (self::updateSeveral($child_model, $item[$key]) === false) {
                        return 0;
                    }
                }
            }
            $names = array();
            $placeholders = array();
            $table->load($item, '');
            $table->isNewRecord = false;
            if (!$table->validate()) {
                throw new Exception(implode(PHP_EOL, $table->getFirstErrors()));
            }
            if ($keys) {
                foreach ($keys as $key) {
                    if (isset($item[$key])) {
                        $table->$key = $item[$key];
                    }
                }
            }
            $tableArray = $table->toArray();
            ksort($tableArray);
            foreach ($tableArray as $name => $value) {
                if (!$i) {
                    $names[] = $conn->quoteColumnName($name);
                    $updates[] = $conn->quoteColumnName($name) . "=values(" . $conn->quoteColumnName($name) . ")";
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
                $sql = 'INSERT INTO ' . $conn->quoteTableName($table::tableName())
                    . ' (' . implode(', ', $names) . ') VALUES ('
                    . implode(', ', $placeholders) . ')';
            } else {
                $sql .= ',(' . implode(', ', $placeholders) . ')';
            }
            $i++;
        }
        $conn->createCommand($sql, $params)->execute();
        return count($array_columns);
    }

    /**
     * @param BaseActiveRecord $model
     * @param array $array_columns
     * @return int
     * @throws Exception
     * @throws InvalidConfigException
     * @throws ReflectionException
     */
    public static function updateSeveral(BaseActiveRecord $model, array $array_columns, array $when = null): int
    {
        if (ArrayHelper::isAssociative($array_columns)) {
            $array_columns = [$array_columns];
        }
        $keys = $model->primaryKey();
        $conn = $model->getDb();
        if (empty($keys)) {
            throw new Exception("The table " . $model->tableName() . ' must have one or more primarykey to call updateSeveral function!');
        }
        if (!is_array($keys)) {
            $keys = [$keys];
        }
        $sql = 'ALTER TABLE ' . $conn->quoteTableName($model->tableName()) . ' update ';
        $columns = array_keys(current($array_columns));
        $sets = [];
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
                    if (self::updateSeveral($child_model, $item[$key]) === false) {
                        return 0;
                    }
                }
            }
            $table->load($item, '');
            if (!$table->validate($columns)) {
                throw new Exception(implode(PHP_EOL, $table->getFirstErrors()));
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
        $conn->createCommand($sql, $params)->execute();
        return count($array_columns);
    }

    /**
     * @param BaseActiveRecord $model
     * @param array $body
     * @param bool $batch
     * @return array|int[]
     * @throws InvalidConfigException
     * @throws ReflectionException
     * @throws Throwable
     */
    public static function create(BaseActiveRecord $model, array $body, bool $batch = true): array
    {
        if (!ArrayHelper::isIndexed($body)) {
            $body = [$body];
        }
        $result = self::insertSeveral($model, $body);
        return is_array($result) ? $result : [$result];
    }

    /**
     * @param BaseActiveRecord $model
     * @param array $body
     * @param bool $batch
     * @param bool $useOrm
     * @return array
     * @throws Exception
     * @throws NotSupportedException
     * @throws Throwable
     */
    public static function update(BaseActiveRecord $model, array $body, bool $onlyUpdate = false, array $when = null, bool $batch = true): array
    {
        if (isset($body['condition']) && $body['condition']) {
            $result = $model->updateAll($body['edit'], DBHelper::Search((new Query()), $body['condition'])->where);
            if ($result === false) {
                throw new Exception('Failed to update the object for unknown reason.');
            }
        } else {
            if (!ArrayHelper::isIndexed($body)) {
                $body = [$body];
            }
            if (!$batch) {
                $result = [];
                $exists = self::findExists($model, $body);
                foreach ($body as $params) {
                    $res = self::updateModel(clone $model, $params, self::checkExist($params, $exists, $model->primaryKey()));
                    $result[] = $res;
                }
            } else {
                $result = self::updateSeveral($model, $body);
            }
        }
        return is_array($result) ? $result : [$result];
    }

    /**
     * @param BaseActiveRecord $model
     * @param array $body
     * @param bool $useOrm
     * @return int
     * @throws Exception
     * @throws InvalidConfigException
     * @throws NotSupportedException
     * @throws ReflectionException
     * @throws Throwable
     */
    public static function delete(BaseActiveRecord $model, array $body, bool $useOrm = false): int
    {
        if (ArrayHelper::isIndexed($body)) {
            $result = self::deleteSeveral($model, $body);
        } else {
            $result = $model->deleteAll(DBHelper::Search((new Query()), $body)->where);
        }
        if ($result) {
            throw new Exception('Failed to delete the object for unknown reason.');
        }
        return $result;
    }

    public static function getModel(string $table, string|ConnectionInterface $db): BaseActiveRecord
    {
        return new class($table, $db) extends ActiveRecord
        {
            public function __construct(string $tableName, string|ConnectionInterface $dbName)
            {
                $this->tableName = $tableName;
                $this->db = is_string($dbName) ? getDI('db')->get($dbName) : $dbName;
            }
        };
    }
}
