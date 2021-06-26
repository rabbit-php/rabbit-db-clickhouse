<?php

declare(strict_types=1);

namespace Rabbit\DB\ClickHouse;


use DI\DependencyException;
use DI\NotFoundException;
use Psr\SimpleCache\InvalidArgumentException;
use Rabbit\Base\Exception\NotSupportedException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\DB\Exception;
use ReflectionException;
use Throwable;

/**
 * Class Schema
 * @package Rabbit\DB\ClickHouse
 */
class Schema extends \Rabbit\DB\Schema
{
    /** @var string */
    public string $columnSchemaClass = ColumnSchema::class;
    /** @var string */
    protected string $builderClass = QueryBuilder::class;
    /** @var array */
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

    /**
     * @param string $table
     * @param array $columns
     * @return array|null
     * @throws DependencyException
     * @throws NotFoundException
     * @throws InvalidArgumentException
     * @throws NotSupportedException
     * @throws Exception
     * @throws Throwable
     */
    public function insert(string $table, array $columns): ?array
    {
        $columns = $this->hardTypeCastValue($table, $columns);
        return parent::insert($table, $columns);
    }

    /**
     * @param string $table
     * @param array $columns
     * @return array
     * @throws Throwable
     */
    protected function hardTypeCastValue(string $table, array $columns): array
    {
        $tableSchema = $this->getTableSchema($table);
        foreach ($columns as $name => $value) {
            $columns[$name] = $tableSchema->columns[$name]->phpTypecast($value);
        }
        return $columns;
    }

    /**
     * Quotes a simple table name for use in a query.
     * A simple table name should contain the table name only without any schema prefix.
     * If the table name is already quoted, this method will do nothing.
     * @param string $name table name
     * @return string the properly quoted table name
     */
    public function quoteSimpleTableName(string $name): string
    {
        return strpos($name, "`") !== false ? $name : "`" . $name . "`";
    }

    /**
     * Quotes a simple column name for use in a query.
     * A simple column name should contain the column name only without any prefix.
     * If the column name is already quoted or is the asterisk character '*', this method will do nothing.
     * @param string $name column name
     * @return string the properly quoted column name
     */
    public function quoteSimpleColumnName(string $name): string
    {
        return strpos($name, '`') !== false || $name === '*' ? $name : '`' . $name . '`';
    }

    /**
     * @inheritdoc
     */
    public function createColumnSchemaBuilder(string $type, $length = null): \Rabbit\DB\ColumnSchemaBuilder
    {
        return new ColumnSchemaBuilder($type, $length, $this->db);
    }

    /**
     * @param string $str
     * @return string
     */
    public function quoteValue(string $str): string
    {
        if (!is_string($str)) {
            return $str;
        }
        return "'" . addcslashes(str_replace("'", "''", $str), "\000\n\r\\\032\047") . "'";
    }

    /**
     * Quotes a column name for use in a query.
     * If the column name contains prefix, the prefix will also be properly quoted.
     * If the column name is already quoted or contains '(', '[[' or '{{',
     * then this method will do nothing.
     * @param string $name column name
     * @return string the properly quoted column name
     * @see quoteSimpleColumnName()
     */
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

    /**
     * @param string $schema
     * @return array|null
     * @throws DependencyException
     * @throws InvalidArgumentException
     * @throws NotFoundException
     * @throws Throwable
     */
    public function findTableNames(string $schema = ''): ?array
    {
        return ArrayHelper::getColumn($this->db->createCommand('SHOW TABLES')->queryAll(), 'name');
    }

    /**
     * @param string $name
     * @return \Rabbit\DB\TableSchema|null
     * @throws DependencyException
     * @throws InvalidArgumentException
     * @throws NotFoundException
     * @throws Throwable
     */
    protected function loadTableSchema(string $name): ?\Rabbit\DB\TableSchema
    {
        $database = $this->db->database === null ? 'default' : $this->db->database;
        if (stripos($name, '.')) {
            $schemaData = explode('.', $name);
            $database = $schemaData[0];
            $name = $schemaData[1];
        }

        $sql = 'SELECT * FROM system.columns WHERE `table`=:name and `database`=:database FORMAT JSON';
        $result = $this->db->createCommand($sql, [
            ':name' => $name,
            ':database' => $database
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

    /**
     * @param array $info
     * @return ColumnSchema
     * @throws DependencyException
     * @throws NotFoundException|ReflectionException
     */
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
