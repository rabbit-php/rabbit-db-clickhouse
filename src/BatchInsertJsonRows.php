<?php


namespace rabbit\db\clickhouse;

use rabbit\db\ConnectionInterface;

class BatchInsertJsonRows extends BatchInsert
{
    /**
     * BatchInsert constructor.
     * @param string $table
     * @param array $columns
     * @param ConnectionInterface $db
     */
    public function __construct(string $table, ConnectionInterface $db)
    {
        $this->table = $table;
        $this->db = $db;
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

    public function clearData()
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
