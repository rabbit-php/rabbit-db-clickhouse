<?php
declare(strict_types=1);

namespace Rabbit\DB\ClickHouse;

use Psr\SimpleCache\InvalidArgumentException;
use Rabbit\Base\Exception\NotSupportedException;
use Throwable;

/**
 * BatchQueryResult represents a batch query from which you can retrieve data in batches.
 *
 * You usually do not instantiate BatchQueryResult directly. Instead, you obtain it by
 * calling [[Query::batch()]] or [[Query::each()]]. Because BatchQueryResult implements the [[\Iterator]] interface,
 * you can iterate it to obtain a batch of data in each iteration. For example,
 *
 * ```php
 * $query = (new Query)->from('user');
 * foreach ($query->batch() as $i => $users) {
 *     // $users represents the rows in the $i-th batch
 * }
 * foreach ($query->each() as $user) {
 * }
 * ```
 *
 * Class BatchQueryResult
 * @package rabbit\db\clickhouse
 */
class BatchQueryResult extends \Rabbit\DB\BatchQueryResult implements \Iterator
{
    /**
     * @var array the data retrieved in the current batch
     */
    private ?array $batch = null;
    /**
     * @var mixed the value for the current iteration
     */
    private $value;
    /**
     * @var string|int the key for the current iteration
     */
    private $key;

    private int $index = 0;

    /**
     * Destructor.
     */
    public function __destruct()
    {
        $this->reset();
    }

    /**
     * Resets the batch query.
     * This method will clean up the existing batch query so that a new batch query can be performed.
     */
    public function reset(): void
    {
        $this->batch = null;
        $this->value = null;
        $this->key = null;
        $this->index = 0;
    }

    /**
     * Resets the iterator to the initial state.
     * This method is required by the interface [[\Iterator]].
     */
    public function rewind()
    {
        $this->reset();
        $this->next();
    }

    /**
     * Moves the internal pointer to the next dataset.
     * This method is required by the interface [[\Iterator]].
     */
    public function next()
    {
        if ($this->batch === null || !$this->each || $this->each && next($this->batch) === false) {
            $this->batch = $this->fetchData();
            reset($this->batch);
        }

        if ($this->each) {
            $this->value = current($this->batch);
            if ($this->query->indexBy !== null) {
                $this->key = key($this->batch);
            } elseif (key($this->batch) !== null) {
                $this->key = $this->key === null ? 0 : $this->key + 1;
            } else {
                $this->key = null;
            }
        } else {
            $this->value = $this->batch;
            $this->key = $this->key === null ? 0 : $this->key + 1;
        }
    }

    /**
     * Fetches the next batch of data.
     * @return array the data fetched
     * @throws InvalidArgumentException
     * @throws NotSupportedException
     * @throws Throwable
     */
    protected function fetchData()
    {
        $command = $this->query->createCommand($this->db);

        $offset = ($this->index * $this->batchSize);
        $this->index++;
        $limit = $this->batchSize;
        $rawSql = $command->getRawSql();
        $command->setSql("{$rawSql} LIMIT {$offset},{$limit}");

        $rows = $command->queryAll();
        return $this->query->populate($rows);
    }

    /**
     * Returns the index of the current dataset.
     * This method is required by the interface [[\Iterator]].
     * @return int the index of the current row.
     */
    public function key()
    {
        return $this->key;
    }

    /**
     * Returns the current dataset.
     * This method is required by the interface [[\Iterator]].
     * @return mixed the current dataset.
     */
    public function current()
    {
        return $this->value;
    }

    /**
     * Returns whether there is a valid dataset at the current position.
     * This method is required by the interface [[\Iterator]].
     * @return bool whether there is a valid dataset at the current position.
     */
    public function valid()
    {
        return !empty($this->batch);
    }
}
