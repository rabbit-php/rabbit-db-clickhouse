<?php
declare(strict_types=1);

namespace Rabbit\DB\ClickHouse;

use Psr\SimpleCache\InvalidArgumentException;
use Rabbit\Base\App;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\DB\Exception;
use Rabbit\DB\QueryBuilder;
use Rabbit\HttpClient\Client;
use Throwable;

/**
 * Class Connection
 * @package Rabbit\DB\ClickHouse
 */
class Connection extends \Rabbit\DB\Connection
{
    /**
     * @var string
     */
    protected string $commandClass = Command::class;
    protected string $schemaClass = Schema::class;

    public array $schemaMap = [
        'clickhouse' => Schema::class
    ];
    /** @var Client */
    protected Client $client;
    /** @var string */
    public string $database = 'default';
    /** @var array */
    protected array $query = [];

    /**
     * Connection constructor.
     * @param string $dsn
     * @throws Exception
     */
    public function __construct(string $dsn)
    {
        parent::__construct($dsn);
        $this->createPdoInstance();
    }

    /**
     * @return void
     * @throws Exception
     */
    public function createPdoInstance()
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
    }

    /**
     * @return Client
     */
    public function getConn(): Client
    {
        return $this->client;
    }


    public function getIsActive(): bool
    {
        return false;
    }

    /**
     * @param string $str
     * @return string
     * @throws InvalidArgumentException
     * @throws Throwable
     */
    public function quoteValue(string $str): string
    {
        return $this->getSchema()->quoteValue($str);
    }

    public function quoteSql(string $sql): string
    {
        return $sql;
    }

    /**
     * Closes the connection when this component is being serialized.
     * @return array
     * @throws Throwable
     */
    public function __sleep()
    {
        $this->close();
        return array_keys(get_object_vars($this));
    }


    /**
     * @throws Throwable
     */
    public function close(): void
    {
        if ($this->getIsActive()) {
            App::warning('Closing DB connection: ' . $this->shortDsn, 'clickhouse');
        }
    }

    /**
     * @return Schema
     */
    public function getSchema(): Schema
    {
        if ($this->schema !== null) {
            return $this->schema;
        }
        return $this->schema = new $this->schemaClass($this);
    }

    /**
     * @param string $name
     * @return string
     */
    public function quoteTableName(string $name): string
    {
        return $name;
    }

    /**
     * @param string $name
     * @return string
     */
    public function quoteColumnName(string $name): string
    {
        return $name;
    }

    /**
     * @return QueryBuilder
     */
    public function getQueryBuilder(): QueryBuilder
    {
        return $this->getSchema()->getQueryBuilder();
    }

    /**
     * @param array $query
     * @return string
     */
    public function getQueryString(array $query = []): string
    {
        return '?' . http_build_query(array_merge($this->query, $query));
    }
}
