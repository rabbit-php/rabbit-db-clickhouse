<?php

declare(strict_types=1);

namespace Rabbit\DB\ClickHouse;

use Throwable;
use Rabbit\DB\Exception;
use Rabbit\DB\DataReader;
use Rabbit\Base\Core\Timer;
use Psr\SimpleCache\CacheInterface;
use Rabbit\Base\Helper\ArrayHelper;
use Psr\Http\Message\ResponseInterface;
use Psr\SimpleCache\InvalidArgumentException;
use Rabbit\Base\App;
use Rabbit\Base\Exception\NotSupportedException;

/**
 * Class Command
 * @package Rabbit\DB\ClickHouse
 */
class Command extends \Rabbit\DB\Command
{
    const FETCH = 'fetch';
    const FETCH_ALL = 'fetchAll';
    const FETCH_COLUMN = 'fetchColumn';
    const FETCH_SCALAR = 'fetchScalar';

    const FETCH_MODE_TOTAL = 7;
    const FETCH_MODE_ALL = 8;

    public int $fetchMode = 0;
    protected ?string $format = null;
    protected bool $isResult = false;
    protected array $options = [];
    protected ?array $meta = null;
    protected ?array $data = null;
    protected int $totals = 0;
    protected array $extremes = [];
    protected int $rows = 0;
    protected array $statistics = [];
    protected int $rows_before_limit_at_least = 0;

    public function __destruct()
    {
    }

    /**
     * @return string
     */
    public function getFormat(): ?string
    {
        return $this->format;
    }

    /**
     * @param string|null $format
     * @return $this
     */
    public function setFormat(?string $format): self
    {
        $this->format = $format;
        return $this;
    }

    /**
     * @return array
     */
    public function getOptions(): array
    {
        return $this->options;
    }

    /**
     * @param array $options
     * @return $this
     */
    public function setOptions(array $options): self
    {
        $this->options = $options;
        return $this;
    }

    /**
     * Adds more options to already defined ones.
     * Please refer to [[setOptions()]] on how to specify options.
     * @param array $options additional options
     * @return $this self reference.
     */
    public function addOptions(array $options): self
    {
        foreach ($options as $key => $value) {
            if (is_array($value) && isset($this->options[$key])) {
                $value = ArrayHelper::merge($this->options[$key], $value);
            }
            $this->options[$key] = $value;
        }
        return $this;
    }

    public function bindValues(array $values): self
    {
        if (empty($values)) {
            return $this;
        }
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


    public function execute(): int
    {
        $rawSql = $this->getRawSql();

        $this->logQuery($rawSql, 'clickhouse');
        $client = $this->db->getConn();
        $response = $client->post($this->db->getQueryString(), ['data' => &$rawSql]);
        return $this->parseResponse($response) === true ? 1 : 0;
    }


    /**
     * @return array|null
     * @throws InvalidArgumentException
     * @throws Throwable
     */
    public function queryColumn(): ?array
    {
        return $this->queryInternal(self::FETCH_COLUMN);
    }

    /**
     * @return string|null
     * @throws InvalidArgumentException
     * @throws Throwable
     */
    public function queryScalar()
    {
        return $this->queryInternal(self::FETCH_SCALAR, 0);
    }

    /**
     * @param string $method
     * @param int $fetchMode
     * @return array|mixed|null
     * @throws InvalidArgumentException
     * @throws Throwable
     */
    protected function queryInternal(?string $method, int $fetchMode = null)
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
        $share = $this->share ?? $this->db->share;
        $func = function () use ($method, &$rawSql, $fetchMode) {
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
                    if (!empty($ret = $cache->get($cacheKey))) {
                        $result = unserialize($ret);
                        if (is_array($result) && isset($result[0])) {
                            $rawSql .= '; [Query result read from cache]';
                            $this->logQuery($rawSql, 'clickhouse');
                            return $this->prepareResult($result[0], $method, $fetchMode);
                        }
                    }
                }
            }

            $this->logQuery($rawSql, 'clickhouse');

            try {
                $client = $this->db->getConn();
                $response = $client->post($this->db->getQueryString(), ['data' => &$rawSql]);
                $data = $this->parseResponse($response);
                $result = $this->prepareResult($data, $method, $fetchMode);
            } catch (\Throwable $e) {
                throw new Exception("Query error: " . $e->getMessage());
            }

            if (isset($cache, $cacheKey, $info)) {
                !$cache->has($cacheKey) && $cache->set($cacheKey, serialize([$data]), $info[1]) && App::debug(
                    'Saved query result in cache',
                    'clickhouse'
                );
            }

            return $result;
        };
        if ($share > 0) {
            $cacheKey = array_filter([
                __CLASS__,
                $method,
                $fetchMode,
                $this->db->dsn,
                $rawSql,
            ]);
            $key = extension_loaded('igbinary') ? igbinary_serialize($cacheKey) : serialize($cacheKey);
            $key = md5($key);
            $s = share($key, $func, $share);
            if ($s->getStatus() === SWOOLE_CHANNEL_CLOSED) {
                $rawSql .= '; [Query result read from share]';
                $this->logQuery($rawSql, 'clickhouse');
            }
            return $s->result;
        }
        return $func();
    }

    /**
     * @param string|null $path
     * @return string
     * @throws Throwable
     */
    public function download(?string $path = null): string
    {
        $rawSql = $this->getRawSql();
        $rawSql .= ' FORMAT CSV';
        $this->logQuery($rawSql, 'clickhouse');
        if ($path === null) {
            $client = $this->db->getConn();
            try {
                $response = $client->post($this->db->getQueryString(), ['data' => &$rawSql]);
                $result = $this->parseResponse($response);
            } catch (\Throwable $e) {
                throw new Exception("Download error: " . $e->getMessage());
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
                $client = $this->db->getConn();
                $response = $client->request([
                    'uri' => $this->db->getQueryString(),
                    'method' => 'POST',
                    'data' => $rawSql,
                    'download_dir' => $dlFileName,
                    'download_offset' => file_exists($dlFileName) ? @filesize($dlFileName) : 0
                ]);
                $this->parseResponse($response);

                if (file_exists($dlFileName)) {
                    @rename(
                        $dlFileName,
                        $fileName
                    );
                } else {
                    throw new Exception("{$rawSql} download failed!");
                }
                if ($this->queryCacheDuration > 0) {
                    Timer::addAfterTimer($this->queryCacheDuration * 1000, function () use ($fileName) {
                        @unlink($fileName);
                    }, 'download.' . $fileName);
                }
                $result = $fileName;
            } catch (Throwable $e) {
                if (file_exists($dlFileName)) {
                    @unlink($dlFileName);
                }
                throw new Exception("Download error: " . $e->getMessage());
            }
        }

        return $result;
    }

    /**
     * @param $result
     * @return array
     * @throws Exception
     */
    protected function getStatementData(array $result): array
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
     * @param array $result
     * @param string $method
     * @param int $fetchMode
     * @return array|mixed|null
     * @throws Exception
     */
    private function prepareResult(array $result, string $method = null, int $fetchMode = null)
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
     * @throws Exception
     */
    private function parseResponse(ResponseInterface $client)
    {
        if ($client->getStatusCode() !== 200) {
            throw new Exception((string)$client->getBody());
        }
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
        return $result === "" ? true : $result;
    }

    /**
     * @param $result
     */
    private function prepareResponseData(array $result): void
    {
        $this->isResult = true;
        foreach (['meta', 'data', 'totals', 'extremes', 'rows', 'rows_before_limit_at_least', 'statistics'] as $key) {
            if (isset($result[$key])) {
                $this->{$key} = $result[$key];
            }
        }
    }

    /**
     * @throws Exception
     */
    private function ensureQueryExecuted()
    {
        if (true !== $this->isResult) {
            throw new Exception('Query was not executed yet');
        }
    }

    /**
     * get meta columns information
     * @return mixed
     * @throws Exception
     */
    public function getMeta()
    {
        $this->ensureQueryExecuted();
        return $this->meta;
    }

    /**
     * get all data result
     * @return mixed|array
     * @throws Exception
     * @throws InvalidArgumentException
     * @throws Throwable
     */
    public function getData()
    {
        if ($this->isResult === null && !empty($this->sql)) {
            $this->queryInternal(null);
        }
        $this->ensureQueryExecuted();
        return $this->data;
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
     * @throws Exception
     */
    public function getSchemaQuery(): string
    {
        $sql = $this->sql;
        $meta = $this->getMeta();
        if (!preg_match('#^SELECT#is', $sql)) {
            throw new Exception('Query was not SELECT type');
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
     * @return int
     * @throws Exception
     */
    public function getTotals(): int
    {
        $this->ensureQueryExecuted();
        return $this->totals;
    }


    /**
     * @return array
     * @throws Exception
     */
    public function getExtremes(): array
    {
        $this->ensureQueryExecuted();
        return $this->extremes;
    }

    /**
     * @return int
     * @throws Exception
     */
    public function getRows(): int
    {
        $this->ensureQueryExecuted();
        return $this->rows;
    }

    /**
     * @return int
     * @throws Exception
     */
    public function getCountAll(): int
    {
        $this->ensureQueryExecuted();
        return $this->rows_before_limit_at_least;
    }

    /**
     * @return array
     * @throws Exception
     */
    public function getStatistics(): array
    {
        $this->ensureQueryExecuted();
        return $this->statistics;
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
     * @throws InvalidArgumentException
     * @throws Throwable
     * @throws NotSupportedException
     */
    public function insert(string $table, $columns): self
    {
        $params = [];
        $sql = $this->db->getQueryBuilder()->insert($table, $columns, $params);
        return $this->setSql($sql)->bindValues($params);
    }

    /**
     * @param string $table
     * @param string $rows
     * @return bool|mixed|string
     * @throws Exception
     * @throws Throwable
     */
    public function insertJsonRows(string $table, string &$rows)
    {
        $sql = 'INSERT INTO ' . $this->db->getSchema()->quoteTableName($table) . ' FORMAT JSONEachRow';
        $this->logQuery($sql, 'clickhouse');
        $client = $this->db->getConn();
        $response = $client->post(
            $this->db->getQueryString(['query' => $sql]),
            [
                'data' => $rows,
                'headers' => [
                    'Content-Type' => 'application/x-ndjson'
                ]
            ]
        );
        return $this->parseResponse($response);
    }

    /**
     * @param string $table
     * @param array|null $columns
     * @param string $file
     * @param string $format
     * @return bool|mixed|string
     * @throws Throwable
     */
    public function insertFile(string $table, array $columns = null, string $file = '', string $format = 'CSV')
    {
        if ($columns === null) {
            $columns = $this->db->getSchema()->getTableSchema($table)->columnNames;
        }
        $sql = 'INSERT INTO ' . $this->db->getSchema()->quoteTableName($table) . ' (' . implode(
            ', ',
            $columns
        ) . ')' . ' FORMAT ' . $format;

        $this->logQuery($sql, 'clickhouse');
        $client = $this->db->getConn();
        $response = $client->post($this->db->getQueryString([
            'query' => $sql
        ]), ['data' => file_get_contents($file)]);
        return $this->parseResponse($response);
    }

    /**
     * @param string $table
     * @param array|null $columns
     * @param array $files
     * @param string $format
     * @return array
     * @throws Throwable
     */
    public function batchInsertFiles(string $table, ?array $columns = null, array $files = [], string $format = 'CSV')
    {
        if ($columns === null) {
            $columns = $this->db->getSchema()->getTableSchema($table)->columnNames;
        }
        $sql = 'INSERT INTO ' . $this->db->getSchema()->quoteTableName($table) . ' (' . implode(
            ', ',
            $columns
        ) . ')' . ' FORMAT ' . $format;

        $this->logQuery($sql, 'clickhouse');
        $responses = [];
        $client = $this->db->getConn();
        foreach ($files as $file) {
            $responses[] = $client->post($this->db->getQueryString([
                'query' => $sql,
            ]), ['data' => file_get_contents($file)]);
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
     * @param string $table
     * @param array $columns
     * @param $rows
     * @return Command
     */
    public function batchInsert(string $table, array $columns, $rows): self
    {
        $sql = $this->db->getQueryBuilder()->batchInsert($table, $columns, $rows);
        return $this->setSql($sql);
    }

    /**
     * @return DataReader
     * @throws Exception
     */
    public function query(): DataReader
    {
        throw new Exception('Clichouse unsupport cursor');
    }
}
