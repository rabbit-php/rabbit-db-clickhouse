<?php

declare(strict_types=1);

namespace Rabbit\DB\ClickHouse;

use Throwable;
use Rabbit\Base\App;
use ReflectionException;
use DI\NotFoundException;
use DI\DependencyException;
use Rabbit\ActiveRecord\ActiveQuery;
use Rabbit\Pool\ConnectionInterface;
use Rabbit\ActiveRecord\ActiveRecord as ActiveRecordActiveRecord;

/**
 * Class ActiveRecord
 * @package Rabbit\DB\ClickHouse
 */
class ActiveRecord extends ActiveRecordActiveRecord
{
    /**
     * @return ConnectionInterface
     * @throws Throwable
     */
    public static function getDb(): ConnectionInterface
    {
        return getDI('db')->get('clickhouse');
    }

    /**
     * @return ActiveQuery
     * @throws DependencyException
     * @throws NotFoundException
     */
    public static function find(): ActiveQuery
    {
        return create(ActiveQuery::class, ['modelClass' => get_called_class()], false);
    }

    /**
     * Returns the primary key **name(s)** for this AR class.
     *
     * Note that an array should be returned even when the record only has a single primary key.
     *
     * For the primary key **value** see [[getPrimaryKey()]] instead.
     *
     * @return string[] the primary key name(s) for this AR class.
     */
    public static function primaryKey(): array
    {
        return ['id'];
    }

    /**
     * @param bool $runValidation
     * @param array|null $attributes
     * @return bool
     * @throws Throwable
     * @throws ReflectionException
     */
    public function insert(bool $runValidation = true, array $attributes = null): bool
    {
        if ($runValidation && !$this->validate($attributes)) {
            App::info('Model not inserted due to validation error.', 'clickhouse');
            return false;
        }

        $values = $this->getDirtyAttributes($attributes);
        if ((static::getDb()->getSchema()->insert(static::tableName(), $values)) === false) {
            return false;
        }

        $this->setOldAttributes($values);
        return true;
    }
}
