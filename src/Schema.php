<?php

namespace rabbit\db\clickhouse;

use rabbit\core\ObjectFactory;
use rabbit\db\TableSchema;
use rabbit\helper\ArrayHelper;

/**
 * Class Schema
 * @package rabbit\db\clickhouse
 */
class Schema extends \rabbit\db\Schema
{
    /** @var $db Connection */
    public $db;

    public $typeMap = [
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
        'Nullable(UInt16' => self::TYPE_INTEGER,
        'Nullable(UInt32' => self::TYPE_INTEGER,
        'Nullable(UInt64' => self::TYPE_BIGINT,
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


    private $_builder;

    /**
     * Executes the INSERT command, returning primary key values.
     * @param string $table the table that new rows will be inserted into.
     * @param array $columns the column data (name => value) to be inserted into the table.
     * @return array primary key values or false if the command fails
     * @since 2.0.4
     */
    public function insert($table, $columns)
    {
        $columns = $this->hardTypeCastValue($table, $columns);
        return parent::insert($table, $columns);
    }

    /**
     * ClickHouse Strong typing data cast
     * @param $table
     * @param $columns
     * @return mixed
     */
    protected function hardTypeCastValue($table, $columns)
    {
        $tableSchema = $this->getTableSchema($table);
        foreach ($columns as $name => $value) {
            $columns[$name] = $tableSchema->columns[$name]->phpTypecast($value);
        }
        return $columns;
    }

    /**
     * @return QueryBuilder the query builder for this connection.
     */
    public function getQueryBuilder()
    {
        if ($this->_builder === null) {
            $this->_builder = $this->createQueryBuilder();
        }
        return $this->_builder;
    }

    public function createQueryBuilder()
    {
        return new QueryBuilder($this->db);
    }

    /**
     * Quotes a simple table name for use in a query.
     * A simple table name should contain the table name only without any schema prefix.
     * If the table name is already quoted, this method will do nothing.
     * @param string $name table name
     * @return string the properly quoted table name
     */
    public function quoteSimpleTableName($name)
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
    public function quoteSimpleColumnName($name)
    {
        return strpos($name, '`') !== false || $name === '*' ? $name : '`' . $name . '`';
    }

    /**
     * @inheritdoc
     */
    public function createColumnSchemaBuilder($type, $length = null)
    {
        return new ColumnSchemaBuilder($type, $length, $this->db);
    }

    /**
     * @param string $str
     * @return string
     */
    public function quoteValue($str)
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
    public function quoteColumnName($name)
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
     * @return array
     * @throws \DI\DependencyException
     * @throws \DI\NotFoundException
     * @throws \rabbit\db\Exception
     */
    public function findTableNames($schema = '')
    {
        return ArrayHelper::getColumn($this->db->createCommand('SHOW TABLES')->queryAll(), 'name');
    }

    /**
     * @param string $name
     * @return TableSchema|null
     * @throws \DI\DependencyException
     * @throws \DI\NotFoundException
     * @throws \rabbit\db\Exception
     */
    protected function loadTableSchema($name)
    {
        $sql = 'SELECT * FROM system.columns WHERE `table`=:name and `database`=:database FORMAT JSON';
        $database = $this->db->getTransport()->database;
        $result = $this->db->createCommand($sql, [
            ':name' => $name,
            ':database' => $database === null ? 'default' : $database
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
     * @param $info
     * @return mixed|\rabbit\db\ColumnSchema
     * @throws \DI\DependencyException
     * @throws \DI\NotFoundException
     */
    protected function loadColumnSchema($info)
    {
        $column = $this->createColumnSchema();
        $column->name = $info['name'];
        $column->dbType = $info['type'];
        $column->type = isset($this->typeMap[$column->dbType]) ? $this->typeMap[$column->dbType] : self::TYPE_STRING;


        if (preg_match('/^([\w ]+)(?:\(([^\)]+)\))?$/', $column->dbType, $matches)) {
            $type = $matches[1];
            $column->dbType = $matches[1] . (isset($matches[2]) ? "({$matches[2]})" : '');
            if (isset($this->typeMap[$type])) {
                $column->type = $this->typeMap[$type];
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

    /**
     * @return mixed|\rabbit\db\ColumnSchema
     * @throws \DI\DependencyException
     * @throws \DI\NotFoundException
     */
    protected function createColumnSchema()
    {
        return ObjectFactory::createObject(ColumnSchema::class, [], false);
    }
}
