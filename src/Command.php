<?php

declare(strict_types=1);

namespace Rabbit\DB\ClickHouse;

use Generator;
use Throwable;
use Rabbit\DB\Exception;
use Rabbit\DB\DataReader;
use Rabbit\Base\Core\Timer;
use Psr\SimpleCache\CacheInterface;
use Rabbit\Base\Helper\ArrayHelper;
use Psr\Http\Message\ResponseInterface;
use Rabbit\Base\App;
use Rabbit\DB\Query;
use Rabbit\Server\ProcessShare;

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

    public function getFormat(): ?string
    {
        return $this->format;
    }

    public function setFormat(?string $format): self
    {
        $this->format = $format;
        return $this;
    }

    public function getOptions(): array
    {
        return $this->options;
    }

    public function setOptions(array $options): self
    {
        $this->options = $options;
        return $this;
    }

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

    public function queryColumn(): ?array
    {
        return $this->queryInternal(self::FETCH_COLUMN);
    }

    public function queryScalar(): null|string|bool|int|float|array
    {
        return $this->queryInternal(self::FETCH_SCALAR, 0);
    }

    protected function queryInternal(?string $method, int $fetchMode = null): null|string|bool|int|float|array|DataReader
    {
        $rawSql = $this->getRawSql();
        if ($method === self::FETCH) {
            if (preg_match('#^SELECT#is', $rawSql) && !preg_match('#LIMIT#is', $rawSql)) {
                $rawSql .= ' LIMIT 1';
            }
        }
        if ($this->getFormat() === null && strpos($rawSql, 'FORMAT ') === false) {
            $rawSql .= ' FORMAT JSON';
        }
        $share = $this->share ?? $this->db->share;
        $func = function () use ($method, &$rawSql, $fetchMode): mixed {
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
                    $cacheKey = extension_loaded('msgpack') ? \msgpack_pack($cacheKey) : serialize($cacheKey);
                    $cacheKey = md5($cacheKey);
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
            $cacheKey = extension_loaded('msgpack') ? \msgpack_pack($cacheKey) : serialize($cacheKey);
            $cacheKey = md5($cacheKey);
            $type = $this->shareType;
            $s = $type($cacheKey, $func, $share, $this->db->shareCache);
            $status = $s->getStatus();
            if ($status === SWOOLE_CHANNEL_CLOSED) {
                $rawSql .= '; [Query result read from channel share]';
                $this->logQuery($rawSql);
            } elseif ($status === ProcessShare::STATUS_PROCESS) {
                $rawSql .= '; [Query result read from process share]';
                $this->logQuery($rawSql);
            } elseif ($status === ProcessShare::STATUS_CHANNEL) {
                $rawSql .= '; [Query result read from process channel share]';
                $this->logQuery($rawSql);
            }
            return $s->result;
        }
        return $func();
    }

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
            if (\extension_loaded('msgpack')) {
                $fileName = md5(\msgpack_pack($fileName));
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
                    Timer::addAfterTimer($this->queryCacheDuration * 1000, function () use ($fileName): void {
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

    protected function getStatementData(): array
    {
        return [
            'meta' => $this->getMeta(),
            'data' => $this->getData(),
            'rows' => $this->getRows(),
            'countAll' => $this->getCountAll(),
            'totals' => $this->getTotals(),
            'statistics' => $this->getStatistics(),
            'extremes' => $this->getExtremes(),
        ];
    }

    private function prepareResult(array $result, string $method = null, int $fetchMode = null): null|string|bool|int|float|array
    {
        $this->prepareResponseData($result);
        $result = ArrayHelper::getValue($result, 'data', []);
        switch ($method) {
            case self::FETCH_COLUMN:
                return array_map(function (array $a) {
                    return array_values($a)[0];
                }, $result);
                break;
            case self::FETCH_SCALAR:
                return current($result[0] ?? []);
            case self::FETCH:
                return is_array($result) ? array_shift($result) : $result;
        }

        if ($fetchMode === self::FETCH_MODE_ALL) {
            return $this->getStatementData();
        }

        if ($fetchMode === self::FETCH_MODE_TOTAL) {
            return $this->getTotals();
        }

        return $result;
    }

    private function parseResponse(ResponseInterface $client): bool|string|array
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

    private function prepareResponseData(array $result): void
    {
        $this->isResult = true;
        foreach (['meta', 'data', 'totals', 'extremes', 'rows', 'rows_before_limit_at_least', 'statistics'] as $key) {
            if (isset($result[$key])) {
                $this->{$key} = $result[$key];
            }
        }
    }

    private function ensureQueryExecuted(): void
    {
        if (true !== $this->isResult) {
            throw new Exception('Query was not executed yet');
        }
    }

    public function getMeta(): ?array
    {
        $this->ensureQueryExecuted();
        return $this->meta;
    }

    public function getData(): ?array
    {
        if ($this->isResult === null && !empty($this->sql)) {
            $this->queryInternal(null);
        }
        $this->ensureQueryExecuted();
        return $this->data;
    }

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

    public function getTotals(): int
    {
        $this->ensureQueryExecuted();
        return $this->totals;
    }

    public function getExtremes(): array
    {
        $this->ensureQueryExecuted();
        return $this->extremes;
    }

    public function getRows(): int
    {
        $this->ensureQueryExecuted();
        return $this->rows;
    }

    public function getCountAll(): int
    {
        $this->ensureQueryExecuted();
        return $this->rows_before_limit_at_least;
    }

    public function getStatistics(): array
    {
        $this->ensureQueryExecuted();
        return $this->statistics;
    }

    public function insert(string $table, array|Query $columns, bool $withUpdate = false): self
    {
        $params = [];
        $sql = $this->db->getQueryBuilder()->insert($table, $columns, $params);
        return $this->setSql($sql)->bindValues($params);
    }

    public function insertJsonRows(string $table, string &$rows): bool|string|array
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

    public function insertFile(string $table, array $columns = null, string $file = '', string $format = 'CSV'): bool|string|array
    {
        if ($columns === null) {
            $columns = $this->db->getSchema()->getTableSchema($table)->getColumnNames();
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

    public function batchInsertFiles(string $table, ?array $columns = null, array $files = [], string $format = 'CSV'): array
    {
        if ($columns === null) {
            $columns = $this->db->getSchema()->getTableSchema($table)->getColumnNames();
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

    public function batchInsert(string $table, array $columns, array|Generator $rows): self
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
