<?php

declare(strict_types=1);

namespace Rabbit\DB\ClickHouse;

use Rabbit\Base\Helper\ArrayHelper;

/**
 * Class Schema
 * @package Rabbit\DB\ClickHouse
 */
class Schema extends \Rabbit\DB\Schema
{
    protected string $builderClass = QueryBuilder::class;

    public static array $typeMap = [
        'UInt8' => self::TYPE_SMALLINT,
        'UInt16' => self::TYPE_INTEGER,
        'UInt32' => self::TYPE_INTEGER,
        'UInt64' => self::TYPE_BIGINT,
        'Int8' => self::TYPE_SMALLINT,
        'Int16' => self::TYPE_INTEGER,
        'Int32' => self::TYPE_INTEGER,
        'Int64' => self::TYPE_BIGINT,
        'Float32' => self::TYPE_FLOAT,
        'Float64' => self::TYPE_FLOAT,
        'String' => self::TYPE_STRING,
        'FixedString' => self::TYPE_CHAR,
        'Date' => self::TYPE_DATE,
        'DateTime' => self::TYPE_DATETIME,
        'Enum' => self::TYPE_STRING,
        'Enum8' => self::TYPE_STRING,
        'Enum16' => self::TYPE_STRING,
        'JSON' => self::TYPE_JSON,

        'Nullable(UInt8)' => self::TYPE_SMALLINT,
        'Nullable(UInt16)' => self::TYPE_INTEGER,
        'Nullable(UInt32)' => self::TYPE_INTEGER,
        'Nullable(UInt64)' => self::TYPE_BIGINT,
        'Nullable(Int8)' => self::TYPE_SMALLINT,
        'Nullable(Int16)' => self::TYPE_INTEGER,
        'Nullable(Int32)' => self::TYPE_INTEGER,
        'Nullable(Int64)' => self::TYPE_BIGINT,
        'Nullable(Float32)' => self::TYPE_FLOAT,
        'Nullable(Float64)' => self::TYPE_FLOAT,
        'Nullable(String)' => self::TYPE_STRING,
        'Nullable(FixedString)' => self::TYPE_CHAR,
        'Nullable(Date)' => self::TYPE_DATE,
        'Nullable(DateTime)' => self::TYPE_DATETIME,
        'Nullable(Enum)' => self::TYPE_STRING,
        'Nullable(Enum8)' => self::TYPE_STRING,
        'Nullable(Enum16)' => self::TYPE_STRING,

        //'Array' => null,
        //'Tuple' => null,
        //'Nested' => null,
    ];

    public function insert(string $table, array $columns): ?array
    {
        $columns = $this->hardTypeCastValue($table, $columns);
        return parent::insert($table, $columns);
    }

    protected function hardTypeCastValue(string $table, array $columns): array
    {
        $tableSchema = $this->getTableSchema($table);
        foreach ($columns as $name => $value) {
            $columns[$name] = $tableSchema->columns[$name]->phpTypecast($value);
        }
        return $columns;
    }

    public function quoteSimpleTableName(string $name): string
    {
        return strpos($name, "`") !== false ? $name : "`" . $name . "`";
    }

    public function quoteSimpleColumnName(string $name): string
    {
        return strpos($name, '`') !== false || $name === '*' ? $name : '`' . $name . '`';
    }

    public function createColumnSchemaBuilder(string $type, int|string|array $length = null): \Rabbit\DB\ColumnSchemaBuilder
    {
        return new ColumnSchemaBuilder($type, $length, $this->db);
    }

    public function quoteValue(string $str): string
    {
        return "'" . addcslashes(str_replace("'", "''", $str), "\000\n\r\\\032\047") . "'";
    }

    public function quoteColumnName(string $name): string
    {
        if (strpos($name, '(') !== false || strpos($name, '[[') !== false || strrpos($name, '.') !== false) {
            return $name;
        }
        $prefix = '';
        if (strpos($name, '{{') !== false) {
            return $name;
        }

        return $prefix . $this->quoteSimpleColumnName($name);
    }

    public function findTableNames(string $schema = ''): ?array
    {
        return ArrayHelper::getColumn($this->db->createCommand('SHOW TABLES')->queryAll(), 'name');
    }

    protected function loadTableSchema(string $name): ?\Rabbit\DB\TableSchema
    {
        $database = $this->db->database === null ? 'default' : $this->db->database;
        if (stripos($name, '.')) {
            $schemaData = explode('.', $name);
            $database = $schemaData[0];
            $name = $schemaData[1];
        }

        $sql = 'SELECT * FROM system.columns WHERE `table`=? and `database`=? FORMAT JSON';
        $result = $this->db->createCommand($sql, [
            $name,
            $database
        ])->queryAll();

        if ($result && isset($result[0])) {
            $table = new TableSchema();
            $table->schemaName = $result[0]['database'];
            $table->name = $name;
            $table->fullName = $table->schemaName . '.' . $table->name;

            foreach ($result as $info) {
                $column = $this->loadColumnSchema($info);
                $table->columns[$column->name] = $column;
            }
            return $table;
        }

        return null;
    }

    protected function loadColumnSchema(array $info): \Rabbit\DB\ColumnSchema
    {
        $column = $this->createColumnSchema();
        $column->name = $info['name'];
        $column->dbType = $info['type'];
        $column->type = isset(self::$typeMap[$column->dbType]) ? self::$typeMap[$column->dbType] : self::TYPE_STRING;


        if (preg_match('/^([\w ]+)(?:\(([^\)]+)\))?$/', $column->dbType, $matches)) {
            $type = $matches[1];
            $column->dbType = $matches[1] . (isset($matches[2]) ? "({$matches[2]})" : '');
            if (isset(self::$typeMap[$type])) {
                $column->type = self::$typeMap[$type];
            }
        }

        $unsignedTypes = ['UInt8', 'UInt16', 'UInt32', 'UInt64'];
        if (in_array($column->dbType, $unsignedTypes)) {
            $column->unsigned = true;
        }

        $column->phpType = $this->getColumnPhpType($column);
        if (empty($info['default_type'])) {
            $column->defaultValue = $info['default_expression'];
        }
        return $column;
    }
}
