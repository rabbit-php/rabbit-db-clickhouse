<?php
declare(strict_types=1);

namespace Rabbit\DB\ClickHouse;

use Rabbit\Base\Exception\NotSupportedException;
use Rabbit\DB\ConnectionInterface;

/**
 * Class BatchInsertJsonRows
 * @package Rabbit\DB\ClickHouse
 */
class BatchInsertJsonRows extends BatchInsert
{
    public function __construct(protected string $table, protected readonly ConnectionInterface $db)
    {
    }

    /**
     * @param array $columns
     * @return bool
     * @throws NotSupportedException
     */
    public function addColumns(array $columns): bool
    {
        throw new NotSupportedException("BatchInsertJsonRows no need to set columns");
    }

    /**
     * @param array $rows
     * @param bool $checkFields
     * @return bool
     */
    public function addRow(array $rows, bool $checkFields = true): bool
    {
        if (empty($rows)) {
            return false;
        }
        $this->hasRows++;
        $this->sql .= json_encode($rows, JSON_UNESCAPED_UNICODE) . PHP_EOL;
        return true;
    }

    public function clearData(): void
    {
        $this->sql = '';
        $this->hasRows = 0;
    }

    /**
     * @return int
     */
    public function execute(): int
    {
        if ($this->hasRows) {
            $this->sql = rtrim($this->sql, PHP_EOL);
            $this->db->createCommand()->insertJsonRows($this->table, $this->sql);
        }
        return $this->hasRows;
    }
}
