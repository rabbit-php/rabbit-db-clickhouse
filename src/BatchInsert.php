<?php

declare(strict_types=1);

namespace Rabbit\DB\ClickHouse;

use Rabbit\Base\Helper\StringHelper;

class BatchInsert extends \Rabbit\DB\BatchInsert
{
    public function addColumns(array $columns = []): bool
    {
        if ($this->columns) {
            return false;
        }
        if (($tableSchema = $this->schema->getTableSchema($this->table)) !== null) {
            $this->columnSchemas = $tableSchema->columns;
        }

        $tmp = [];

        if (empty($columns)) {
            $tmp = array_keys($this->columnSchemas);
        } else {
            foreach ($columns as $name) {
                if ($this->columnSchemas[$name] ?? false) {
                    $tmp[] = $name;
                }
            }
        }
        $this->bindColumns($tmp);
        $this->columns = $tmp;
        return true;
    }

    public function addRow(array $rows, bool $checkFields = true): bool
    {
        if (empty($rows)) {
            return false;
        }
        $this->hasRows++;
        foreach (array_diff(array_keys($rows), $this->columns) as $name) {
            unset($rows[$name]);
        }
        if ($checkFields) {
            $tmps = [];
            foreach ($this->columns as $name) {
                $columnSchema = $this->columnSchemas[$name];
                $value = $columnSchema->dbTypecast($rows[$name] ?? null);
                if (is_string($value)) {
                    switch (true) {
                        case $columnSchema->dbType === "DateTime":
                        case $columnSchema->dbType === "Date":
                            $value = strtotime($value);
                            break;
                        case strpos($columnSchema->dbType, "Int") !== false:
                            if (strtolower($value) === 'false') {
                                $value = 0;
                            } elseif (strtolower($value) === 'true') {
                                $value = 1;
                            } else {
                                $value = $columnSchema->type === Schema::TYPE_BIGINT ? $value : intval($value);
                            }
                            break;
                        case strpos($columnSchema->dbType, "Float") !== false:
                        case strpos($columnSchema->dbType, "Decimal") !== false:
                            if (strtolower($value) === 'false') {
                                $value = 0;
                            } elseif (strtolower($value) === 'true') {
                                $value = 1;
                            } else {
                                $value = floatval($value);
                            }
                            break;
                        default:
                            $value = $this->quoteValue($value);
                    }
                } elseif (is_float($value)) {
                    $value = StringHelper::floatToString($value);
                } elseif ($value === false) {
                    $value = 0;
                } elseif ($value === null) {
                    $dbType = $columnSchema->dbType;
                    switch (true) {
                        case strpos($dbType, "Int") !== false:
                        case strpos($dbType, "Float") !== false:
                        case strpos($dbType, "Decimal") !== false:
                        case $dbType === "DateTime":
                        case $dbType === "Date":
                            $value = 0;
                            break;
                        case strpos($dbType, "String") !== false:
                        default:
                            $value = $this->quoteValue("");
                    }
                } elseif (is_array($value)) {
                    switch (true) {
                        case strpos($columnSchema->dbType, "Int") !== false:
                            foreach ($value as $index => $v) {
                                $value[$index] = $columnSchema->type === Schema::TYPE_BIGINT ? $value : intval($value);
                            }
                            $value = str_replace('"', "", json_encode($value, JSON_UNESCAPED_UNICODE));
                            break;
                        case strpos($columnSchema->dbType, "Float") !== false:
                        case strpos($columnSchema->dbType, "Decimal") !== false:
                            foreach ($value as $index => $v) {
                                $value[$index] = floatval($v);
                            }
                            $value = str_replace('"', "", json_encode($value, JSON_UNESCAPED_UNICODE));
                            break;
                        default:
                            $value = str_replace('"', "'", json_encode($value, JSON_UNESCAPED_UNICODE));
                    }
                }
                $tmps[$name] = $value;
            }
            $rows = $tmps;
        }
        $this->bindRows($rows);
        return true;
    }

    protected function bindColumns(array $columns): void
    {
        $this->sql .= ' (' . implode(', ', $columns) . ') VALUES ';
    }

    protected function bindRows(array $rows): void
    {
        $this->sql .= '(' . implode(', ', $rows) . '),';
    }

    protected function quoteValue(string $str): string
    {
        return $this->schema->quoteValue($str);
    }
}
