<?php

namespace rabbit\db\clickhouse;

use rabbit\db\DBHelper;
use rabbit\db\Exception;
use rabbit\helper\ArrayHelper;

/**
 * Class UpdateExt
 * @package rabbit\db\mysql
 */
class UpdateExt
{
    /**
     * @param ActiveRecord $model
     * @param array $body
     * @param bool $hasRealation
     * @return array
     * @throws Exception
     * @throws \rabbit\exception\InvalidConfigException
     */
    public static function update(ActiveRecord $model, array $body, bool $hasRealation = false): array
    {
        if (ArrayHelper::isIndexed($body)) {
            if ($hasRealation) {
                $result = [];
                $exists = self::findExists($model, $body);
                foreach ($body as $params) {
                    $res = self::updateSeveral(clone $model, $params, self::checkExist($model, $params, $exists));
                    $result[] = $res;
                }
            } else {
                $result = $model::getDb()->updateSeveral($model, $body);
            }
        } elseif (isset($body['condition']) && $body['condition']) {
            $condition = DBHelper::Search((new Query()), $body['condition'])->where;
            $result = $model->updateAll($body['edit'], $condition);
            if ($result === false) {
                throw new Exception('Failed to update the object for unknown reason.');
            }
        } else {
            $result = self::updateSeveral($model, $body);
        }

        return is_array($result) ? $result : [$result];
    }

    /**
     * @param ActiveRecord $model
     * @param array $body
     * @param array $andCondition
     * @return array
     * @throws \rabbit\exception\InvalidConfigException
     */
    private static function findExists(ActiveRecord $model, array $body, array $condition = []): array
    {
        $keys = $model::primaryKey();
        if (ArrayHelper::isAssociative($body)) {
            $body = [$body];
        }
        foreach ($keys as $key) {
            foreach ($body as $item) {
                if (array_key_exists($key, $item)) {
                    $condition[$key][] = $item[$key];
                }
            }
        }
        if ($condition !== [] && count($keys) === count($condition)) {
            $exits = $model::find()->where($condition)->asArray()->all();
            return $exits;
        }
        return [];
    }

    /**
     * @param ActiveRecord $model
     * @param array $body
     * @param array|null $exist
     * @return array
     * @throws \Exception
     */
    public static function updateSeveral(ActiveRecord $model, array $body, ?array $exist): array
    {
        $model->setOldAttributes($exist);
        $model->load($body, '');
        if ($model->save() === false && !$model->hasErrors()) {
            throw new Exception('Failed to update the object for unknown reason.');
        } else {
            $result = self::saveRealation($model, $body);
        }
        return $result;
    }

    /**
     * @param ActiveRecord $model
     * @param array $body
     * @return array
     * @throws Exception
     * @throws \rabbit\exception\InvalidConfigException
     */
    public static function saveRealation(ActiveRecord $model, array $body): array
    {
        $result = [];
        //关联模型
        if (isset($model->realation)) {
            foreach ($model->realation as $key => $val) {
                if (isset($body[$key])) {
                    $child = $model->getRelatedRecords($key)->modelClass;
                    if ($body[$key]) {
                        if (isset($params['edit']) && $params['edit']) {
                            $child_model = new $child();
                            $result[$key] = self::update($child_model, $params);
                        } else {
                            if (ArrayHelper::isAssociative($body[$key])) {
                                $params = [$body[$key]];
                            } else {
                                $params = $body[$key];
                            }
                            $keys = $child::primaryKey();
                            $exists = self::findExists($child_model, $params);
                            foreach ($params as $param) {
                                if ($val) {
                                    /** @var ActiveRecord $child_model */
                                    $child_model = new $child();
                                    $child_id = key($val);
                                    foreach ($val as $c_attr => $p_attr) {
                                        $param[$c_attr] = $model->{$p_attr};
                                    }
                                    $result[$key][] = self::updateSeveral($child_model, $param,
                                        self::checkExist($child_model, $param, $exists,
                                            [$child_id => $model[$val[$child_id]]]));
                                }
                            }
                        }
                    }
                }
            }
        }
        $res = $model->toArray();
        foreach ($result as $key => $val) {
            $res[$key] = $val;
        }
        return $res;
    }

    /**
     * @param ActiveRecord $model
     * @param array $body
     * @param array $exists
     * @return array|null
     * @throws \rabbit\exception\InvalidConfigException
     */
    private static function checkExist(ActiveRecord $model, array $body, array $exists): ?array
    {
        if (!$exists) {
            return null;
        }
        $keys = $model::primaryKey();
        $existCount = 0;
        foreach ($exists as $exist) {
            foreach ($keys as $key) {
                if (isset($body[$key]) && $body[$key] == $exist[$key]) {
                    $existCount++;
                }
                if ($existCount === count($keys)) {
                    return $exist;
                }
            }
        }
        return null;
    }

}
