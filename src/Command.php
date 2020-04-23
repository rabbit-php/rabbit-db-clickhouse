<?php

namespace rabbit\db\clickhouse;

use Psr\Http\Message\ResponseInterface;
use Psr\SimpleCache\CacheInterface;
use rabbit\App;
use rabbit\db\Command as BaseCommand;
use rabbit\db\Exception as DbException;
use rabbit\helper\ArrayHelper;
use rabbit\socket\HttpClient;

/**
 * Class Command
 * @package rabbit\db\clickhouse
 * @property $db \rabbit\db\clickhouse\Connection
 */
class Command extends BaseCommand
{
    const FETCH = 'fetch';
    const FETCH_ALL = 'fetchAll';
    const FETCH_COLUMN = 'fetchColumn';
    const FETCH_SCALAR = 'fetchScalar';

    const FETCH_MODE_TOTAL = 7;
    const FETCH_MODE_ALL = 8;

    /** @var int fetch type result */
    public $fetchMode = 0;

    protected $_format = null;

    protected $_is_result;

    protected $_options = [];

    /**
     * @var
     */
    protected $_meta;
    /**
     * @var
     */
    protected $_data;
    /**
     * @var
     */
    protected $_totals;
    /**
     * @var array
     */
    protected $_extremes;
    /**
     * @var int
     */
    protected $_rows;
    /**
     * @var array
     */
    protected $_statistics;
    /**
     * @var
     */
    protected $_rows_before_limit_at_least;


    /**
     * @return null
     */
    public function getFormat()
    {
        return $this->_format;
    }

    /**
     * @param string|null $format
     * @return $this
     */
    public function setFormat(?string $format)
    {
        $this->_format = $format;
        return $this;
    }

    /**
     * @return array
     */
    public function getOptions()
    {
        return $this->_options;
    }

    /**
     * @param array $options
     * @return $this
     */
    public function setOptions($options)
    {
        $this->_options = $options;
        return $this;
    }

    /**
     * Adds more options to already defined ones.
     * Please refer to [[setOptions()]] on how to specify options.
     * @param array $options additional options
     * @return $this self reference.
     */
    public function addOptions(array $options)
    {
        foreach ($options as $key => $value) {
            if (is_array($value) && isset($this->_options[$key])) {
                $value = ArrayHelper::merge($this->_options[$key], $value);
            }
            $this->_options[$key] = $value;
        }
        return $this;
    }

    public function bindValues($values)
    {
        if (empty($values)) {
            return $this;
        }
        //$schema = $this->db->getSchema();
        foreach ($values as $name => $value) {
            if (is_array($value)) {
                $this->_pendingParams[$name] = $value;
                $this->params[$name] = $value[0];
            } else {
                $this->params[$name] = $value;
            }
        }

        return $this;
    }


    public function execute()
    {
        $rawSql = $this->getRawSql();

        if (strlen($rawSql) < $this->db->maxLog) {
            $this->logQuery($rawSql, 'clickhouse');
        }
        $client = $this->db->getTransport();
        $response = $client->post($client->getQueryString(), $rawSql);

        $this->checkResponseStatus($response);

        $data = $this->parseResponse($response);
        return $data;
    }


    /**
     * @return array|mixed|null
     * @throws DbException
     * @throws \Psr\SimpleCache\InvalidArgumentException
     */
    public function queryColumn()
    {
        return $this->queryInternal(self::FETCH_COLUMN);
    }

    /**
     * @return array|false|int|mixed|string|null
     * @throws DbException
     * @throws \Psr\SimpleCache\InvalidArgumentException
     */
    public function queryScalar()
    {
        $result = $this->queryInternal(self::FETCH_SCALAR, 0);
        if (is_array($result)) {
            return current($result);
        } else {
            return $result;
        }
    }

    public function getRawSql()
    {
        if (empty($this->params)) {
            return $this->getSql();
        }
        $params = [];
        foreach ($this->params as $name => $value) {
            if (is_string($name) && strncmp(':', $name, 1)) {
                $name = ':' . $name;
            }
            if (is_string($value)) {
                $params[$name] = $this->db->quoteValue($value);
            } elseif (is_bool($value)) {
                $params[$name] = ($value ? 'TRUE' : 'FALSE');
            } elseif ($value === null) {
                $params[$name] = 'NULL';
            } elseif (!is_object($value) && !is_resource($value)) {
                $params[$name] = $value;
            }
        }
        if (!isset($params[0])) {
            return strtr($this->getSql(), $params);
        }
        $sql = '';
        foreach (explode('?', $this->getSql()) as $i => $part) {
            $sql .= $part . (isset($params[$i]) ? $params[$i] : '');
        }
        return $sql;
    }

    /**
     * @param string $method
     * @param null $fetchMode
     * @return array|mixed|null
     * @throws DbException
     * @throws \Psr\SimpleCache\InvalidArgumentException
     */
    protected function queryInternal($method, $fetchMode = null)
    {
        $rawSql = $this->getRawSql();
        if ($method == self::FETCH) {
            if (preg_match('#^SELECT#is', $rawSql) && !preg_match('#LIMIT#is', $rawSql)) {
                $rawSql .= ' LIMIT 1';
            }
        }
        if ($this->getFormat() === null && strpos($rawSql, 'FORMAT ') === false) {
            $rawSql .= ' FORMAT JSON';
        }

        if ($method !== '') {
            $info = $this->db->getQueryCacheInfo($this->queryCacheDuration, $this->cache);
            if (is_array($info)) {
                /** @var CacheInterface $cache */
                $cache = $info[0];
                $cacheKey = array_filter([
                    __CLASS__,
                    $method,
                    $fetchMode,
                    $this->db->dsn,
                    $rawSql,
                ]);
                $result = unserialize($cache->get($cacheKey));
                if (is_array($result) && isset($result[0])) {
                    $this->logQuery($rawSql . '; [Query result served from cache]', 'clickhouse');
                    return $this->prepareResult($result[0], $method, $fetchMode);
                }
            }
        }

        $this->logQuery($rawSql);

        try {
            $client = $this->db->getTransport();
            $response = $client->post($client->getQueryString(), $rawSql);

            $this->checkResponseStatus($response);

            $data = $this->parseResponse($response);
            $result = $this->prepareResult($data, $method, $fetchMode);
        } catch (\Exception $e) {
            throw new DbException("Query error: " . $e->getMessage());
        }

        if (isset($cache, $cacheKey, $info)) {
            $cache->set($cacheKey, serialize([$data]), $info[1]) && App::debug(
                'Saved query result in cache',
                'clickhouse'
            );
        }

        return $result;
    }

    /**
     * @param string|null $path
     * @return string
     * @throws DbException
     */
    public function download(?string $path = null): string
    {
        $rawSql = $this->getRawSql();
        $rawSql .= ' FORMAT CSV';
        $this->logQuery($rawSql, 'clickhouse');
        if ($path === null) {
            $client = $this->db->getTransport();
            try {
                $response = $client->post($client->getQueryString(), $rawSql);
                $this->checkResponseStatus($response);
                $result = (string)$response->getBody();
            } catch (\Exception $e) {
                throw new DbException("Download error: " . $e->getMessage());
            }
        } else {
            $fileName = [
                __CLASS__,
                $this->db->dsn,
                $this->db->username,
                $rawSql,
            ];
            if (\extension_loaded('igbinary')) {
                $fileName = md5(igbinary_serialize($fileName));
            } else {
                $fileName = md5(serialize($fileName));
            }

            $dlFileName = "$path/{$fileName}.download";
            $fileName = "$path/{$fileName}.csv";

            if (file_exists($fileName)) {
                return $fileName;
            }

            try {
                $client = $this->db->getTransport();
                $client->setMethod('POST');
                $client->setData($rawSql);
                $response = $client->download(
                    $client->getQueryString(),
                    $dlFileName,
                    file_exists($dlFileName) ? @filesize($dlFileName) : 0
                );
                $this->checkResponseStatus($response);

                if (file_exists($dlFileName)) {
                    @rename(
                        $dlFileName,
                        $fileName
                    );
                } else {
                    throw new DbException("{$rawSql} download failed!");
                }
                if ($this->queryCacheDuration > 0) {
                    \Swoole\Timer::after($this->queryCacheDuration * 1000, function (string $path) {
                        @unlink($path);
                    }, $fileName);
                }
                $result = $fileName;
            } catch (\Throwable $e) {
                if (file_exists($dlFileName)) {
                    @unlink($dlFileName);
                }
                throw new DbException("Download error: " . $e->getMessage());
            }
        }

        return $result;
    }

    /**
     * @param $result
     * @return array
     */
    protected function getStatementData($result)
    {
        return [
            'meta' => $this->getMeta(),
            'data' => $result,
            'rows' => $this->getRows(),
            'countAll' => $this->getCountAll(),
            'totals' => $this->getTotals(),
            'statistics' => $this->getStatistics(),
            'extremes' => $this->getExtremes(),
        ];
    }

    /**
     * @param ResponseInterface $client
     * @throws DbException
     */
    public function checkResponseStatus(ResponseInterface $client)
    {
        if ($client->getStatusCode() !== 200) {
            throw new DbException((string)$client->getBody());
        }
    }

    /**
     * @param $result
     * @param null $method
     * @param null $fetchMode
     * @return array|mixed|null
     */
    private function prepareResult($result, $method = null, $fetchMode = null)
    {
        $this->prepareResponseData($result);
        $result = ArrayHelper::getValue($result, 'data', []);
        switch ($method) {
            case self::FETCH_COLUMN:
                return array_map(function ($a) {
                    return array_values($a)[0];
                }, $result);
                break;
            case self::FETCH_SCALAR:
                if (array_key_exists(0, $result)) {
                    return current($result[0]);
                }
                break;
            case self::FETCH:
                return is_array($result) ? array_shift($result) : $result;
                break;
        }

        if ($fetchMode == self::FETCH_MODE_ALL) {
            return $this->getStatementData($result);
        }

        if ($fetchMode == self::FETCH_MODE_TOTAL) {
            return $this->getTotals();
        }

        return $result;
    }


    /**
     * @param ResponseInterface $client
     * @return mixed|string
     */
    private function parseResponse(ResponseInterface $client)
    {
        $contentType = $client->getHeaderLine(strtolower('Content-Type'));

        list($type) = explode(';', $contentType);

        $type = strtolower($type);

        $hash = [
            'application/json'
        ];

        $result = in_array($type, $hash) ? json_decode(
            (string)$client->getBody(),
            true
        ) : (string)$client->getBody();
        return $result;
    }

    /**
     * @param $result
     */
    private function prepareResponseData($result)
    {
        if (!is_array($result)) {
            return;
        }
        $this->_is_result = true;
        foreach (['meta', 'data', 'totals', 'extremes', 'rows', 'rows_before_limit_at_least', 'statistics'] as $key) {
            if (isset($result[$key])) {
                $attr = "_" . $key;
                $this->{$attr} = $result[$key];
            }
        }
    }

    private function ensureQueryExecuted()
    {
        if (true !== $this->_is_result) {
            throw new DbException('Query was not executed yet');
        }
    }

    /**
     * get meta columns information
     * @return mixed
     */
    public function getMeta()
    {
        $this->ensureQueryExecuted();
        return $this->_meta;
    }

    /**
     * get all data result
     * @return mixed|array
     */
    public function getData()
    {
        if ($this->_is_result === null && !empty($this->getSql())) {
            $this->queryInternal(null);
        }
        $this->ensureQueryExecuted();
        return $this->_data;
    }

    /**
     * Generation sql `create table` for meta (only select query)
     *
     * ```php
     * $sql = 'SELECT sum(click) as sum_click, event_date FROM table_name GROUP BY event_date LIMIT 10';
     * $command = $connection->createCommand($sql);
     * $data = $command->queryAll();
     * $schemaSql = $command->getSchemaQuery();
     * ```
     *
     * @return string
     * @throws DbException
     */
    public function getSchemaQuery()
    {
        $sql = $this->getSql();
        $meta = $this->getMeta();
        if (!preg_match('#^SELECT#is', $sql)) {
            throw new DbException('Query was not SELECT type');
        }
        $table = "CREATE TABLE x (\n    ";
        $columns = [];
        foreach ($meta as $item) {
            $columns[] = '`' . $item['name'] . '` ' . $item['type'];
        }
        $table .= implode(",\n    ", $columns);
        $table .= "\n)";

        return $table;
    }

    /**
     * @return mixed
     */
    public function getTotals()
    {
        $this->ensureQueryExecuted();
        return $this->_totals;
    }


    /**
     * @return mixed
     */
    public function getExtremes()
    {
        $this->ensureQueryExecuted();
        return $this->_extremes;
    }

    /**
     *  get count result items
     * @return mixed
     */
    public function getRows()
    {
        $this->ensureQueryExecuted();
        return $this->_rows;
    }

    /**
     * max count result items
     * @return mixed
     */
    public function getCountAll()
    {
        $this->ensureQueryExecuted();
        return $this->_rows_before_limit_at_least;
    }

    /**
     * @return mixed
     */
    public function getStatistics()
    {
        $this->ensureQueryExecuted();
        return $this->_statistics;
    }

    /**
     * Creates an INSERT command.
     * For example,
     *
     * ```php
     * $connection->createCommand()->insert('user', [
     *     'name' => 'Sam',
     *     'age' => 30,
     * ])->execute();
     * ```
     *
     * The method will properly escape the column names, and bind the values to be inserted.
     *
     * Note that the created command is not executed until [[execute()]] is called.
     *
     * @param string $table the table that new rows will be inserted into.
     * @param array $columns the column data (name => value) to be inserted into the table.
     * @return $this the command object itself
     */
    public function insert($table, $columns)
    {
        $params = [];
        $sql = $this->db->getQueryBuilder()->insert($table, $columns, $params);
        return $this->setSql($sql)->bindValues($params);
    }

    /**
     * @param string $table
     * @param array $rows
     */
    public function insertJsonRows(string $table, string &$rows)
    {
        $sql = 'INSERT INTO ' . $this->db->getSchema()->quoteTableName($table) . ' FORMAT JSONEachRow';
        $categoryLog = 'clickhouse';
        $this->logQuery($sql, $categoryLog);
        /** @var HttpClient $client */
        $client = $this->db->getTransport();
        $client->setHeaders([
            'Content-Type' => 'application/x-ndjson'
        ]);
        $response = $client->post($client->getQueryString([
            'query' => $sql,
        ]), $rows);
        $body = $response->getBody();
        return $body;
    }

    /**
     * @param string $table
     * @param array|null $columns
     * @param string $file
     * @param string $format
     */
    public function insertFile(string $table, array $columns = null, string $file = '', string $format = 'CSV')
    {
        $categoryLog = 'clickhouse';
        if ($columns === null) {
            $columns = $this->db->getSchema()->getTableSchema($table)->columnNames;
        }
        $sql = 'INSERT INTO ' . $this->db->getSchema()->quoteTableName($table) . ' (' . implode(
                ', ',
                $columns
            ) . ')' . ' FORMAT ' . $format;

        $this->logQuery($sql, $categoryLog);
        /** @var HttpClient $client */
        $client = $this->db->getTransport();
        $response = $client->post($client->getQueryString([
            'query' => $sql
        ]), \Co::readFile($file));
        $this->checkResponseStatus($response);
        $body = $response->getBody();
        return $body;
    }

    /**
     * @param $table
     * @param null $columns
     * @param array $files
     * @param string $format
     * @return array
     * @throws \rabbit\exception\NotSupportedException
     */
    public function batchInsertFiles($table, $columns = null, $files = [], $format = 'CSV')
    {
        $categoryLog = 'clickhouse';
        if ($columns === null) {
            $columns = $this->db->getSchema()->getTableSchema($table)->columnNames;
        }
        $sql = 'INSERT INTO ' . $this->db->getSchema()->quoteTableName($table) . ' (' . implode(
                ', ',
                $columns
            ) . ')' . ' FORMAT ' . $format;

        $this->logQuery($sql, $categoryLog);
        $responses = [];
        /** @var HttpClient $client */
        $client = $this->db->getTransport();
        foreach ($files as $file) {
            $responses[] = $client->post($client->getQueryString([
                'query' => $sql,
            ]), \Co::readFile($file));
        }
        return $responses;
    }

    /**
     * Creates a batch INSERT command.
     * For example,
     *
     * ```php
     * $connection->createCommand()->batchInsert('user', ['name', 'age'], [
     *     ['Tom', 30],
     *     ['Jane', 20],
     *     ['Linda', 25],
     * ])->execute();
     * ```
     */
    public function batchInsert($table, $columns, $rows)
    {
        $sql = $this->db->getQueryBuilder()->batchInsert($table, $columns, $rows);
        return $this->setSql($sql);
    }

    public function query()
    {
        throw new DbException('Clichouse unsupport cursor');
    }
}
