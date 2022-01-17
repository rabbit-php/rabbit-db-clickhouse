<?php

declare(strict_types=1);

namespace Rabbit\DB\ClickHouse;

use Rabbit\Base\App;
use Rabbit\ActiveRecord\ActiveQuery;
use Rabbit\Pool\ConnectionInterface;
use Rabbit\ActiveRecord\ActiveRecord as ActiveRecordActiveRecord;
use Rabbit\DB\Query;

/**
 * Class ActiveRecord
 * @package Rabbit\DB\ClickHouse
 */
class ActiveRecord extends ActiveRecordActiveRecord
{
    public function __construct(string|ConnectionInterface $db = null)
    {
        if (is_string($db)) {
            $this->db = getDI('db')->get($db);
        } elseif ($db === null) {
            $this->db = getDI('db')->get('clickhouse');
        } else {
            $this->db = $db;
        }
    }

    public function getDb(): ConnectionInterface
    {
        return $this->db;
    }

    public function setDb(ConnectionInterface $db): self
    {
        $this->db = $db;
        return $this;
    }

    public function find(): ActiveQuery
    {
        return create(ActiveQuery::class, ['modelClass' => get_called_class()], false);
    }

    public function primaryKey(): array
    {
        return ['id'];
    }

    public function insertByQuery(Query $query, bool $withUpdate = false): bool
    {
        return (bool)$this->db->createCommand()->insert($this->tableName(), $query)->execute();
    }

    public function insert(bool $runValidation = true, array $attributes = null): bool
    {
        if ($runValidation && !$this->validate($attributes)) {
            App::info('Model not inserted due to validation error.', 'clickhouse');
            return false;
        }

        $values = $this->getDirtyAttributes($attributes);
        if (($this->db->getSchema()->insert($this->tableName(), $values)) === false) {
            return false;
        }

        $this->setOldAttributes($values);
        return true;
    }
}
