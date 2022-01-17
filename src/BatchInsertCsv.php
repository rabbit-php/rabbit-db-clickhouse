<?php

declare(strict_types=1);

namespace Rabbit\DB\ClickHouse;


use Rabbit\Base\Core\Exception;
use Rabbit\Base\Helper\FileHelper;
use Rabbit\DB\BatchInterface;
use Rabbit\DB\ConnectionInterface;

/**
 * Class BatchInsertCsv
 * @package Rabbit\DB\ClickHouse
 */
class BatchInsertCsv implements BatchInterface
{
    private array $columns = [];
    private string $cacheDir = '/dev/shm/ck/csv/';
    private $fp;
    private string $ext = 'csv';
    private string $fileName;
    private int $hasRows = 0;

    /**
     * BatchInsert constructor.
     * @param string $table
     * @param string $fileName
     * @param ConnectionInterface $db
     * @param string $cacheDir
     * @throws Exception
     */
    public function __construct(
        private string $table,
        string $fileName,
        private ConnectionInterface $db,
        string $cacheDir = '/dev/shm/ck/csv/'
    ) {
        $this->cacheDir = $cacheDir;
        $this->fileName = $this->cacheDir . pathinfo($fileName, PATHINFO_FILENAME) . '.' . $this->ext;
        $this->open();
    }

    /**
     * @throws Exception
     */
    private function open(): void
    {
        if (!FileHelper::createDirectory($this->cacheDir) || (($this->fp = @fopen($this->fileName, 'w+')) === false)) {
            throw new \InvalidArgumentException("Unable to open file: {$this->fileName}");
        }
    }

    private function close(): void
    {
        if ($this->fp !== null) {
            @fclose($this->fp);
            @unlink($this->fileName);
            $this->fp = null;
        }
    }

    public function __destruct()
    {
        $this->close();
    }

    /**
     * @param array $columns
     * @return bool
     */
    public function addColumns(array $columns): bool
    {
        if (empty($columns) || $this->columns) {
            return false;
        }
        $this->columns = $columns;
        return true;
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

        if (@fputcsv($this->fp, $rows, ',', "'") === false) {
            throw new \RuntimeException("fputcsv error data=" . implode(' | ', $rows));
        }
        $this->hasRows++;
        return true;
    }

    public function clearData(): void
    {
        @ftruncate($this->fp, 0);
    }

    /**
     * @return int
     */
    public function execute(): int
    {
        $this->db->createCommand()->insertFile($this->table, $this->columns, $this->fileName);
        return $this->hasRows;
    }
}
