<?php
declare(strict_types=1);

namespace Rabbit\DB\ClickHouse;

use Psr\SimpleCache\InvalidArgumentException;
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
    protected int $index = 0;

    /**
     * Resets the batch query.
     * This method will clean up the existing batch query so that a new batch query can be performed.
     */
    public function reset(): void
    {
        parent::reset();
        $this->index = 0;
    }

    /**
     * Fetches the next batch of data.
     * @return array the data fetched
     * @throws InvalidArgumentException
     * @throws Throwable
     */
    protected function fetchData(): array
    {
        $command = $this->query->createCommand();

        $offset = ($this->index * $this->batchSize);
        $this->index++;
        $limit = $this->batchSize;
        $rawSql = $command->getRawSql();
        $command->setSql("{$rawSql} LIMIT {$offset},{$limit}");

        $rows = $command->queryAll();
        return $this->query->populate($rows);
    }
}
