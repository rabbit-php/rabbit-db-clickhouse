<?php

declare(strict_types=1);

namespace Rabbit\DB\ClickHouse;

use Rabbit\DB\BatchQueryResult;
use Rabbit\DB\Command;
use Rabbit\DB\Exception;
use Rabbit\DB\Expression;
use Rabbit\Pool\ConnectionInterface;

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
    private ?Command $command = null;
    private bool $withTotals = false;
    public ?float $sample = null;
    public null|string|array|Expression $preWhere = null;
    public ?array $limitBy = null;

    public function __construct(?ConnectionInterface $db = null, string $driver = 'clickhouse', array $config = [])
    {
        parent::__construct($db ?? getDI('db')->get($driver), $config);
    }

    public function createCommand(): Command
    {
        list($sql, $params) = $this->db->getQueryBuilder()->build($this);
        $this->command = $this->db->createCommand($sql, $params);
        $this->command->share($this->share);
        $this->setCommandCache($this->command);
        return $this->command;
    }

    public function sample(float $n): self
    {
        $this->sample = $n;
        return $this;
    }

    public function preWhere(string|array|Expression $condition, array $params = []): self
    {
        $this->preWhere = $condition;
        $this->addParams($params);
        return $this;
    }

    public function andPreWhere(string|array|Expression $condition, array $params = []): self
    {
        if ($this->preWhere === null) {
            $this->preWhere = $condition;
        } else {
            $this->preWhere = ['and', $this->preWhere, $condition];
        }
        $this->addParams($params);
        return $this;
    }

    public function orPreWhere(string|array|Expression $condition, array $params = []): self
    {
        if ($this->preWhere === null) {
            $this->preWhere = $condition;
        } else {
            $this->preWhere = ['or', $this->preWhere, $condition];
        }
        $this->addParams($params);
        return $this;
    }

    public function limitBy(int $n, array $columns): self
    {
        $this->limitBy = [$n, $columns];
        return $this;
    }

    public function withTotals(): self
    {
        $this->withTotals = true;
        return $this;
    }

    public function hasWithTotals(): bool
    {
        return $this->withTotals;
    }

    private function ensureQueryExecuted(): void
    {
        if (null === $this->command) {
            throw new Exception('Query was not executed yet');
        }
    }

    private function callSpecialCommand(string $name)
    {
        $this->ensureQueryExecuted();
        return $this->command->{$name}();
    }

    public function __call($name, $params)
    {
        $methods = ['getmeta', 'getdata', 'getextremes', 'gettotals', 'getcountall', 'getrows', 'download'];
        if (in_array(strtolower($name), $methods)) {
            return $this->callSpecialCommand($name);
        } else {
            return parent::__call($name, $params);
        }
    }

    public function __clone()
    {
        $this->command = null;
    }

    public function batch(int $batchSize = 100): BatchQueryResult
    {
        return create([
            'class' => BatchQueryResult::class,
            'query' => $this,
            'batchSize' => $batchSize,
            'db' => $this->db,
            'each' => false,
        ], [], false);
    }

    public function each(int $batchSize = 100): BatchQueryResult
    {
        return create([
            'class' => BatchQueryResult::class,
            'query' => $this,
            'batchSize' => $batchSize,
            'db' => $this->db,
            'each' => true,
        ], [], false);
    }
}
