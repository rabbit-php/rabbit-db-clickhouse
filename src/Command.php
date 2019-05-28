<?php

namespace rabbit\db\clickhouse;

use Psr\Http\Message\ResponseInterface;
use Psr\SimpleCache\CacheInterface;
use rabbit\App;
use rabbit\db\Command as BaseCommand;
use rabbit\db\Exception;
use rabbit\db\Exception as DbException;
use rabbit\helper\ArrayHelper;
use rabbit\helper\CoroHelper;

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

    private $_format = null;

    private $_pendingParams = [];

    private $_is_result;

    private $_options = [];

    /**
     * @var
     */
    private $_meta;
    /**
     * @var
     */
    private $_data;
    /**
     * @var
     */
    private $_totals;
    /**
     * @var array
     */
    private $_extremes;
    /**
     * @var int
     */
    private $_rows;
    /**
     * @var array
     */
    private $_statistics;
    /**
     * @var
     */
    private $_rows_before_limit_at_least;


    /**
     * @return null
     */
    public function getFormat()
    {
        return $this->_format;
    }

    /**
     * @param null $format
     * @return $this
     */
    public function setFormat($format)
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
        $response = $this->db->getTransport()->post($this->getBaseUrl(), [
            'body' => $rawSql
        ]);

        $this->checkResponseStatus($response);

        return $this->parseResponse($response);
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
        return (is_numeric($result)) ? ($result + 0) : $result;
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
        if (!isset($params[1])) {
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
        App::info($rawSql, 'clickhouse');

        if ($method !== '') {
            $info = $this->db->getQueryCacheInfo($this->queryCacheDuration);
            if (is_array($info)) {
                /** @var CacheInterface $cache */
                $cache = $info[0];
                $cacheKey = [
                    __CLASS__,
                    $method,
                    $fetchMode,
                    $this->db->dsn,
                    $this->db->username,
                    $rawSql,
                ];
                $result = $cache->get($cacheKey);
                if (is_array($result) && isset($result[0])) {
                    App::debug('Query result served from cache', 'clickhouse');
                    return $this->prepareResult($result[0], $method, $fetchMode);
                }
            }
        }

        try {
            $response = $this->db->getTransport()->post($this->getBaseUrl(), [
                'body' => $rawSql
            ]);

            $this->checkResponseStatus($response);

            $data = $this->parseResponse($response);
            $result = $this->prepareResult($data, $method, $fetchMode);
        } catch (\Exception $e) {
            throw new Exception("Query error: " . $e->getMessage());
        }

        if (isset($cache, $cacheKey, $info)) {
            $cache->set($cacheKey, [$data], $info[1], $info[2]);
            App::debug('Saved query result in cache', 'clickhouse');
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


    protected function getBaseUrl()
    {
        return $this->db->buildUrl($this->getOptions());
    }

    /**
     * @param ResponseInterface $response
     * @throws DbException
     */
    public function checkResponseStatus(ResponseInterface $response)
    {
        if ($response->getStatusCode() != 200) {
            throw new DbException((string)$response->getBody());
        }
    }


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
     * @param ResponseInterface $response
     * @return mixed
     */
    private function parseResponse(ResponseInterface $response)
    {
        $contentType = $response->getHeaderLine('Content-Type');

        list($type) = explode(';', $contentType);

        $type = strtolower($type);
        $hash = [
            'application/json' => 'jsonArray'
        ];

        $result = (isset($hash[$type])) ? $response->{$hash[$type]}() : (string)$response->getBody();
        return $result;
    }

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
     * @param $table
     * @param null $columns columns default columns get schema table
     * @param array $files list files
     * @param string $format file format
     * @return \yii\httpclient\Response[]
     */
    public function batchInsertFiles($table, $columns = null, $files = [], $format = 'CSV')
    {
        $categoryLog = 'clickhouse';
        if ($columns === null) {
            $columns = $this->db->getSchema()->getTableSchema($table)->columnNames;
        }
        $sql = 'INSERT INTO ' . $this->db->getSchema()->quoteTableName($table) . ' (' . implode(', ',
                $columns) . ')' . ' FORMAT ' . $format;

        App::debug($sql, $categoryLog);

        $urlBase = $this->db->transport->baseUrl;
        $requests = [];
        $url = $this->db->buildUrl($urlBase, [
            'database' => $this->db->database,
            'query' => $sql,
        ]);

        $group = CoroHelper::createGroup();
        foreach ($files as $key => $file) {
            $group->add($key, function () use ($url, $file) {
                return $this->db->getTransport()->post($url, [
                    'body' => file_get_contents($file)
                ]);
            });
        }

        $responses = $group->wait(600);

        return $responses;
    }

    /**
     * @param $table
     * @param null $columns
     * @param array $files
     * @param string $format
     * @param int $size
     * @return ResponseInterface[]
     */
    public function batchInsertFilesDataSize($table, $columns = null, $files = [], $format = 'CSV', $size = 10000)
    {
        $categoryLog = 'clickhouse';
        if ($columns === null) {
            $columns = $this->db->getSchema()->getTableSchema($table)->columnNames;
        }
        $sql = 'INSERT INTO ' . $this->db->getSchema()->quoteTableName($table) . ' (' . implode(', ',
                $columns) . ')' . ' FORMAT ' . $format;

        App::debug($sql, $categoryLog);

        $urlBase = $this->db->getTransport()->baseUrl;
        $responses = [];
        $url = $this->db->buildUrl($urlBase, [
            'database' => $this->db->database,
            'query' => $sql,
        ]);
        $group = CoroHelper::createGroup();
        foreach ($files as $key => $file) {
            rgo(function () use ($key, $file) {
                if (($handle = fopen($file, 'r')) !== false) {
                    $buffer = '';
                    $count = $part = 0;
                    while (($line = fgets($handle)) !== false) {
                        $buffer .= $line;
                        $count++;
                        if ($count >= $size) {
                            $group->add($key, function () use ($part) {
                                return [
                                    'part_' . ($part++) => $this->db->getTransport()->post($url, [
                                        'body' => $buffer
                                    ])
                                ];
                            });
                            $buffer = '';
                            $count = 0;
                        }
                    }
                    if (!empty($buffer)) {
                        $group->add($key, function () use ($part) {
                            return [
                                'part_' . ($part++) => $this->db->getTransport()->post($url, [
                                    'body' => $buffer
                                ])
                            ];
                        });
                    }
                    fclose($handle);
                }
            });
        }

        return $group->wait(600);
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
        throw new Exception('Clichouse unsupport cursor');
    }
}