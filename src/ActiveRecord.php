<?php

namespace rabbit\db\clickhouse;

use rabbit\App;
use rabbit\core\ObjectFactory;
use rabbit\db\ConnectionInterface;

/**
 * Class ActiveRecord
 * @package rabbit\db\clickhouse
 */
class ActiveRecord extends \rabbit\activerecord\ActiveRecord
{


    /**
     * Returns the connection used by this AR class.
     * @return mixed|Connection the database connection used by this AR class.
     */
    public static function getDb(): ConnectionInterface
    {
        return getDI('clickhouse')->getConnection();
    }

    /**
     * @return mixed|\rabbit\activerecord\ActiveQuery
     * @throws \DI\DependencyException
     * @throws \DI\NotFoundException
     */
    public static function find()
    {
        return ObjectFactory::createObject(ActiveQuery::class, ['modelClass' => get_called_class()]);
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
    public static function primaryKey()
    {
        // TODO: Implement primaryKey() method.
        return null;
    }

    /**
     * @param bool $runValidation
     * @param null $attributes
     * @return bool
     * @throws \Exception
     */
    public function insert($runValidation = true, $attributes = null)
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