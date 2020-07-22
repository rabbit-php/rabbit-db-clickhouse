<?php
declare(strict_types=1);

namespace Rabbit\DB\ClickHouse;

/**
 * Class SchemaMap
 * @package Rabbit\DB\ClickHouse
 */
class SchemaMap
{
    /**
     * @var array|\string[][]
     */
    public static array $map = [
        'mysql' => [
            'UNSIGNED TINYINT' => 'UInt8',
            'TINYINT' => 'Int8',
            'UNSIGNED SMALLINT' => 'UInt16',
            'SMALLINT' => 'Int16',
            'UNSIGNED INT' => 'UInt32',
            'UNSIGNED MEDIUMINT' => 'UInt32',
            'INT' => 'Int32',
            'MEDIUMINT' => 'Int32',
            'UNSIGNED BIGINT' => 'UInt64',
            'BIGINT' => 'Int64',
            'VARCHAR' => 'String',
            'FLOAT' => 'Float32',
            'DOUBLE' => 'Float64',
            'DATE' => 'Date',
            'DATETIME' => 'DateTime',
            'TIMESTAMP' => 'DateTime',
            'BINARY' => 'String'
        ]
    ];
}