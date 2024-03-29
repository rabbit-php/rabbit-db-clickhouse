<?php

declare(strict_types=1);

namespace Rabbit\DB\ClickHouse;

use Rabbit\ActiveRecord\BaseActiveRecord;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Base\Helper\JsonHelper;
use Rabbit\DB\DBHelper;
use Rabbit\DB\Exception;
use Rabbit\DB\JsonExpression;

class ARHelper extends \Rabbit\ActiveRecord\ARHelper
{
    const DEFAULT_DB = 'clickhouse';

    public static function saveSeveral(BaseActiveRecord $model, array &$array_columns, bool $withUpdate = false, array $exclude = [], bool $trans = true): int
    {
        return parent::saveSeveral($model, $array_columns, false, $exclude);
    }

    public static function updateSeveral(BaseActiveRecord $model, array &$array_columns, array $when = null): int
    {
        if (!array_is_list($array_columns)) {
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

        $schema = $conn->getSchema();
        $tableSchema = $schema->getTableSchema($model->tableName());
        $columnSchemas = $tableSchema !== null ? $tableSchema->columns : [];

        foreach ($array_columns as $item) {
            $table = clone $model;
            $table->load($item, '');
            if (!$table->validate($columns)) {
                throw new Exception(implode(PHP_EOL, $table->getFirstErrors()));
            }
            ksort($item);
            foreach ($item as $name => $value) {
                if (!($columnSchemas[$name] ?? false)) {
                    continue;
                }
                $value =  $columnSchemas[$name]->dbTypecast($value);
                if (in_array($name, $keys)) {
                    $whereIn[$name][] = '?';
                    $whereVal[] = $value;
                } else {
                    $i === 0 && $sets[$name] = " `$name`=multiIf(";
                    foreach ($keys as $key) {
                        $bindings[] = $item[$key];
                        $sets[$name] .= "`$key`=? and ";
                    }
                    $sets[$name] = rtrim($sets[$name], ' and ') . ',?,';
                    if ($value instanceof JsonExpression) {
                        $bindings[] = is_string($value->getValue()) ? $value->getValue() : JsonHelper::encode($value->getValue());
                    } else {
                        $bindings[] = $value;
                    }
                }
            }
            $i++;
        }
        foreach ($sets as $name => $value) {
            $sql .= $value . "`$name`),";
        }
        $sql = rtrim($sql, ",");
        $sql .= " where ";
        foreach ($keys as $key) {
            $sql .= " `$key` in (" . implode(',', $whereIn[$key]) . ') and ';
        }
        $sql = rtrim($sql, "and ");
        $params = [...$bindings, ...$whereVal];
        $conn->createCommand($sql, $params)->execute();
        return count($array_columns);
    }

    public static function create(BaseActiveRecord $model, array &$body, bool $batch = true): array
    {
        if (!ArrayHelper::isIndexed($body)) {
            $body = [$body];
        }
        $result = parent::typeInsert($model, $body);
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
}
