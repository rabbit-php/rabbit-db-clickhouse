<?php

declare(strict_types=1);

namespace Rabbit\DB\ClickHouse;

use Psr\SimpleCache\InvalidArgumentException;
use Rabbit\Base\App;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\DB\Exception;
use Rabbit\DB\QueryBuilder;
use Rabbit\DB\QueryInterface;
use Rabbit\HttpClient\Client;
use Throwable;

/**
 * Class Connection
 * @package Rabbit\DB\ClickHouse
 */
class Connection extends \Rabbit\DB\Connection
{
    protected string $commandClass = Command::class;
    protected string $schemaClass = Schema::class;

    public array $schemaMap = [
        'clickhouse' => Schema::class
    ];

    protected Client $client;

    protected array $query = [];

    public function __construct(protected string $dsn)
    {
        parent::__construct($dsn);
        $this->createPdoInstance();
        $this->driver = 'clickhouse';
        $this->canTransaction = false;
    }

    public function createPdoInstance(): object
    {
        $parsed = $this->parseDsn;
        if (!in_array($parsed['scheme'], ['clickhouse', 'clickhouses'])) {
            throw new Exception("clickhouse only support scheme clickhouse & clickhouses");
        }

        if (!isset($parsed['path'])) {
            $parsed['path'] = '/';
        }

        isset($parsed['query']) ? parse_str($parsed['query'], $query) : $query = [];
        $query['database'] = $this->database = (string)ArrayHelper::remove($query, 'dbname', 'default');
        $this->query = $query;
        $size = ArrayHelper::remove($query, 'size', false);
        $retry = ArrayHelper::remove($query, 'retry', 0);
        $timeout = ArrayHelper::remove($query, 'timeout', 5);

        $parsed['scheme'] = str_replace('clickhouse', 'http', $parsed['scheme']);
        $scheme = isset($parsed['scheme']) ? $parsed['scheme'] . '://' : '';
        $host = isset($parsed['host']) ? $parsed['host'] : '';
        $port = isset($parsed['port']) ? ':' . $parsed['port'] : '';
        $path = isset($parsed['path']) ? $parsed['path'] : '';

        $options = [
            'base_uri' => "$scheme$host$port$path",
            'use_pool' => $size,
            'retry_time' => $retry,
            'timeout' => $timeout
        ];
        isset($parsed['user']) && $options['auth']['username'] = $parsed['user'];
        isset($parsed['pass']) && $options['auth']['password'] = $parsed['pass'];
        $this->client = new Client($options);
        return $this->client;
    }

    public function getConn(): Client
    {
        return clone $this->client;
    }


    public function getIsActive(): bool
    {
        return false;
    }

    public function quoteValue(string $str): string
    {
        return $this->getSchema()->quoteValue($str);
    }

    public function quoteSql(string $sql): string
    {
        return $sql;
    }

    public function __sleep()
    {
        $this->close();
        return array_keys(get_object_vars($this));
    }

    public function close(): void
    {
        if ($this->getIsActive()) {
            App::warning('Closing DB connection: ' . $this->shortDsn, 'clickhouse');
        }
    }

    public function getSchema(): \Rabbit\DB\Schema
    {
        if ($this->schema !== null) {
            return $this->schema;
        }
        return $this->schema = new $this->schemaClass($this);
    }

    public function quoteTableName(string $name): string
    {
        return $name;
    }

    public function quoteColumnName(string $name): string
    {
        return $name;
    }

    public function getQueryBuilder(): QueryBuilder
    {
        return $this->getSchema()->getQueryBuilder();
    }

    public function getQueryString(array $query = []): string
    {
        return '?' . http_build_query([...$this->query, ...$query]);
    }

    public function buildQuery(): QueryInterface
    {
        return new Query($this);
    }
}
