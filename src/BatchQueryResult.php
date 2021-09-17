<?php

declare(strict_types=1);

namespace Rabbit\DB\ClickHouse;

use Psr\SimpleCache\InvalidArgumentException;
use Throwable;

class BatchQueryResult extends \Rabbit\DB\BatchQueryResult implements \Iterator
{
    protected int $index = 0;

    public function reset(): void
    {
        parent::reset();
        $this->index = 0;
    }

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
