<?php
declare(strict_types=1);

namespace Rabbit\DB\ClickHouse;

use DI\DependencyException;
use DI\NotFoundException;
use Rabbit\DB\BatchQueryResult;
use Rabbit\DB\Command;
use Rabbit\DB\ConnectionInterface;
use Rabbit\DB\Exception;
use Rabbit\DB\Expression;
use Throwable;

/**
 * Class Query
 * @package Rabbit\DB\ClickHouse
 * @method getCountAll() int
 * @method getTotals() array
 * @method getData() array
 * @method getExtremes() array
 * @method getRows() int
 * @method getMeta() array
 */
class Query extends \Rabbit\DB\Query
{

    /** @var Command */
    private ?Command $command = null;
    /** @var bool */
    private bool $withTotals = false;
    /** @var float|null */
    public ?float $sample = null;
    public $preWhere = null;
    public ?array $limitBy = null;

    /**
     * @param ConnectionInterface|null $db
     * @return Command
     * @throws Throwable
     */
    public function createCommand(ConnectionInterface $db = null): Command
    {
        if ($db === null) {
            $db = getDI('clickhouse')->get();
        }
        list($sql, $params) = $db->getQueryBuilder()->build($this);


        $this->command = $db->createCommand($sql, $params);
        return $this->command;
    }

    /**
     * set section query SAMPLE
     * @param $n float|int  set value 0.1 .. 1 percent or int 1 .. 1m value
     * @return $this the query object itself
     */
    public function sample(float $n): self
    {
        $this->sample = $n;
        return $this;
    }


    /**
     * Sets the PREWHERE part of the query.
     *
     * The method requires a `$condition` parameter, and optionally a `$params` parameter
     * specifying the values to be bound to the query.
     *
     * The `$condition` parameter should be either a string (e.g. `'id=1'`) or an array.
     *
     * @inheritdoc
     *
     * @param string|array|Expression $condition the conditions that should be put in the WHERE part.
     * @param array $params the parameters (name => value) to be bound to the query.
     * @return $this the query object itself
     *** see andWhere()
     *** see orWhere()
     */
    public function preWhere($condition, array $params = []): self
    {
        $this->preWhere = $condition;
        $this->addParams($params);
        return $this;
    }


    /**
     * Adds an additional PREWHERE condition to the existing one.
     * The new condition and the existing one will be joined using the 'AND' operator.
     * @param string|array|Expression $condition the new WHERE condition. Please refer to [[where()]]
     * on how to specify this parameter.
     * @param array $params the parameters (name => value) to be bound to the query.
     * @return $this the query object itself
     * @see preWhere()
     * @see orPreWhere()
     */
    public function andPreWhere($condition, array $params = []): self
    {
        if ($this->preWhere === null) {
            $this->preWhere = $condition;
        } else {
            $this->preWhere = ['and', $this->preWhere, $condition];
        }
        $this->addParams($params);
        return $this;
    }

    /**
     * Adds an additional PREWHERE condition to the existing one.
     * The new condition and the existing one will be joined using the 'OR' operator.
     * @param string|array|Expression $condition the new WHERE condition. Please refer to [[where()]]
     * on how to specify this parameter.
     * @param array $params the parameters (name => value) to be bound to the query.
     * @return $this the query object itself
     * @see preWhere()
     * @see andPreWhere()
     */
    public function orPreWhere($condition, array $params = []): self
    {
        if ($this->preWhere === null) {
            $this->preWhere = $condition;
        } else {
            $this->preWhere = ['or', $this->preWhere, $condition];
        }
        $this->addParams($params);
        return $this;
    }

    /**
     * @param int $n
     * @param array $columns
     * @return $this
     */
    public function limitBy(int $n, array $columns): self
    {
        $this->limitBy = [$n, $columns];
        return $this;
    }

    /**
     * @return $this
     */
    public function withTotals(): self
    {
        $this->withTotals = true;
        return $this;
    }

    /**
     * @return bool
     */
    public function hasWithTotals(): bool
    {
        return $this->withTotals;
    }

    /**
     * check is first method executed
     * @throws Exception
     */
    private function ensureQueryExecuted()
    {
        if (null === $this->command) {
            throw new Exception('Query was not executed yet');
        }
    }

    /**
     * call method Command::{$name}
     * @param $name
     * @return mixed
     * @throws Exception
     */
    private function callSpecialCommand(string $name)
    {
        $this->ensureQueryExecuted();
        return $this->command->{$name}();
    }

    /**
     * @param $name
     * @param $params
     * @return mixed|Query
     * @throws Exception
     */
    public function __call($name, $params)
    {
        $methods = ['getmeta', 'getdata', 'getextremes', 'gettotals', 'getcountall', 'getrows', 'download'];
        if (in_array(strtolower($name), $methods)) {
            return $this->callSpecialCommand($name);
        } else {
            return parent::__call($name, $params);
        }
    }

    /**
     * reset command
     */
    public function __clone()
    {
        $this->command = null;
    }

    /**
     * Starts a batch query.
     *
     * A batch query supports fetching data in batches, which can keep the memory usage under a limit.
     * This method will return a [[BatchQueryResult]] object which implements the [[\Iterator]] interface
     * and can be traversed to retrieve the data in batches.
     *
     * For example,
     *
     * ```php
     * $query = (new Query)->from('user');
     * foreach ($query->batch() as $rows) {
     *     // $rows is an array of 100 or fewer rows from user table
     * }
     * ```
     *
     * @param int $batchSize the number of records to be fetched in each batch.
     * @param ConnectionInterface $db the database connection. If not set, the "db" application component will be used.
     * @return BatchQueryResult the batch query result. It implements the [[\Iterator]] interface
     * and can be traversed to retrieve the data in batches.
     * @throws DependencyException
     * @throws NotFoundException
     */
    public function batch(int $batchSize = 100, ConnectionInterface $db = null): BatchQueryResult
    {
        return create([
            'class' => BatchQueryResult::class,
            'query' => $this,
            'batchSize' => $batchSize,
            'db' => $db,
            'each' => false,
        ], [], false);
    }

    /**
     * Starts a batch query and retrieves data row by row.
     *
     * This method is similar to [[batch()]] except that in each iteration of the result,
     * only one row of data is returned. For example,
     *
     * ```php
     * $query = (new Query)->from('user');
     * foreach ($query->each() as $row) {
     * }
     * ```
     *
     * @param int $batchSize the number of records to be fetched in each batch.
     * @param ConnectionInterface $db the database connection. If not set, the "db" application component will be used.
     * @return BatchQueryResult the batch query result. It implements the [[\Iterator]] interface
     * and can be traversed to retrieve the data in batches.
     * @throws DependencyException
     * @throws NotFoundException
     */
    public function each(int $batchSize = 100, ConnectionInterface $db = null): BatchQueryResult
    {
        return create([
            'class' => BatchQueryResult::class,
            'query' => $this,
            'batchSize' => $batchSize,
            'db' => $db,
            'each' => true,
        ], [], false);
    }
}
