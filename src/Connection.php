<?php

namespace rabbit\db\clickhouse;

use rabbit\App;
use rabbit\core\ObjectFactory;
use rabbit\db\ConnectionInterface;
use rabbit\httpclient\Client;

/**
 * Class Connection
 * @package rabbit\db\clickhouse
 */
class Connection extends \rabbit\db\Connection implements ConnectionInterface
{
    /**
     * @var string name use database default use value  "default"
     */
    public $database = 'default';

    /**
     * @var string the hostname or ip address to use for connecting to the click-house server. Defaults to 'localhost'.
     */
    public $dsn = 'localhost';

    public $timeout = 3;

    public $usePool = false;

    public $reTrytimes = 3;

    public $clientDriver = 'saber';
    /**
     * @var string
     */
    public $commandClass = Command::class;
    public $schemaClass = Schema::class;

    public $schemaMap = [
        'clickhouse' => Schema::class
    ];

    /** @var Client */
    private $_transport = false;

    private $_schema;

    /**
     * Connection constructor.
     * @param string $dsn
     * @param string $driver
     */
    public function __construct(string $dsn, string $driver = 'saber')
    {
        $this->dsn = $dsn;
        $this->clientDriver = $driver;

        $parsed = parse_url($this->dsn);
        isset($parsed['query']) ? parse_str($parsed['query'], $parsed['query']) : $parsed['query'] = [];
        if (isset($parsed['query']['database'])) {
            $this->database = $parsed['query']['database'];
        }

        $this->_transport = new Client([
            'base_uri' => $this->dsn,
            'use_pool' => false,
            'timeout' => $this->timeout,
            'retry_time' => $this->reTrytimes
        ], $this->clientDriver);
    }

    /**
     * @param null $sql
     * @param array $params
     * @return Command|\rabbit\db\Command
     * @throws \DI\DependencyException
     * @throws \DI\NotFoundException
     * @throws \rabbit\db\Exception
     */
    public function createCommand($sql = null, $params = [])
    {
        $this->open();
        App::debug("Executing ClickHouse: {$sql}", 'clickhouse');

        /** @var Command $command */
        $command = ObjectFactory::createObject($this->commandClass, [
            'db' => $this,
            'sql' => $sql,
        ], false);

        return $command->bindValues($params);
    }


    /**
     * @return Client
     */
    public function getTransport(): Client
    {
        return $this->_transport;
    }


    public function getIsActive()
    {
        return $this->_transport !== false;
    }

    public function open()
    {
        if ($this->getIsActive()) {
            return;
        }
    }

    /**
     * @param array $data
     * @return string
     */
    public function buildUrl($data = [])
    {
        if (empty($data)) {
            return $this->dsn;
        }
        $parsed = parse_url($this->dsn);
        isset($parsed['query']) ? parse_str($parsed['query'], $parsed['query']) : $parsed['query'] = [];
        $params = isset($parsed['query']) ? array_merge($parsed['query'], $data) : $data;

        $parsed['query'] = !empty($params) ? '?' . http_build_query($params) : '';
        if (!isset($parsed['path'])) {
            $parsed['path'] = '/';
        }

        $auth = (!empty($parsed['user']) ? $parsed['user'] : '') . (!empty($parsed['pass']) ? ':' . $parsed['pass'] : '');
        $defaultScheme = 'http';

        return (isset($parsed['scheme']) ? $parsed['scheme'] : $defaultScheme)
            . '://'
            . (!empty($auth) ? $auth . '@' : '')
            . $parsed['host']
            . (!empty($parsed['port']) ? ':' . $parsed['port'] : '')
            . $parsed['path']
            . $parsed['query'];
    }


    /**
     * @param string $str
     * @return string
     * @throws \DI\DependencyException
     * @throws \DI\NotFoundException
     */
    public function quoteValue($str)
    {
        return $this->getSchema()->quoteValue($str);
    }

    public function quoteSql($sql)
    {
        return $sql;
    }


    public function ping()
    {
        $this->open();
        $query = 'SELECT 1';
        $response = $this->_transport->post('', [
            'body' => $query,
            'headers' => [
                'Content-Type' => 'application/x-www-form-urlencoded'
            ]
        ]);

        return trim((string)$response->getBody()) == '1';
    }


    /**
     * Closes the connection when this component is being serialized.
     * @return array
     */
    public function __sleep()
    {
        $this->close();
        return array_keys(get_object_vars($this));
    }


    /**
     * Closes the currently active DB connection.
     * It does nothing if the connection is already closed.
     */
    public function close()
    {
        if ($this->getIsActive()) {
            $connection = ($this->dsn . ':' . $this->port);
            App::debug('Closing DB connection: ' . $connection, __METHOD__);
        }
    }

    /**
     * Initializes the DB connection.
     * This method is invoked right after the DB connection is established.
     * The default implementation triggers an [[EVENT_AFTER_OPEN]] event.
     */
    protected function initConnection()
    {
    }

    /**
     * @return mixed|\rabbit\db\Schema
     * @throws \DI\DependencyException
     * @throws \DI\NotFoundException
     */
    public function getSchema()
    {
        return $this->_schema = ObjectFactory::createObject([
            'class' => $this->schemaClass,
            'db' => $this
        ]);
    }

    public function quoteTableName($name)
    {
        return $name;
    }

    public function getDriverName()
    {
        return 'clickhouse';
    }

    public function quoteColumnName($name)
    {
        return $name;
    }

    /**
     * @return \rabbit\db\QueryBuilder
     * @throws \DI\DependencyException
     * @throws \DI\NotFoundException
     */
    public function getQueryBuilder()
    {
        return $this->getSchema()->getQueryBuilder();
    }
}
