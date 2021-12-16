<?php

declare(strict_types=1);

namespace Rabbit\DB\ClickHouse;

use Rabbit\ActiveRecord\BaseActiveRecord;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\DB\DBHelper;
use Rabbit\DB\Exception;
use Rabbit\Pool\ConnectionInterface;

class ARHelper extends \Rabbit\ActiveRecord\ARHelper
{
    public static function saveSeveral(BaseActiveRecord $model, array &$array_columns, bool $withUpdate = false, array $exclude = []): int
    {
        return parent::saveSeveral($model, $array_columns, false, $exclude);
    }

    public static function updateSeveral(BaseActiveRecord $model, array &$array_columns, array $when = null): int
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

    public static function create(BaseActiveRecord $model, array &$body, bool $batch = true): array
    {
        if (!ArrayHelper::isIndexed($body)) {
            $body = [$body];
        }
        $result = self::insertSeveral($model, $body);
        return is_array($result) ? $result : [$result];
    }

    public static function update(BaseActiveRecord $model, array &$body, bool $onlyUpdate = false, array $when = null, bool $batch = true): array
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

    public static function delete(BaseActiveRecord $model, array &$body, bool $useOrm = false): int
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
