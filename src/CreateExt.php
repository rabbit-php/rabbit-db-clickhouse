<?php

namespace rabbit\db\clickhouse;

use rabbit\db\Exception;
use rabbit\helper\ArrayHelper;

/**
 * Class CreateExt
 * @package rabbit\db\mysql
 */
class CreateExt
{
    /**
     * @param ActiveRecord $model
     * @param array $body
     * @param bool $hasRealation
     * @return array
     * @throws Exception
     */
    public static function create(
        ActiveRecord $model,
        array $body,
        bool $hasRealation = false
    ): array {
        if (ArrayHelper::isIndexed($body)) {
            if ($hasRealation) {
                $result = [];
                foreach ($body as $params) {
                    $res = self::createSeveral(clone $model, $params);
                    $result[] = $res;
                }
            } else {
                $result = $model::getDb()->insertSeveral($model, $body);
            }
        } else {
            $result = self::createSeveral($model, $body);
        }

        return is_array($result) ? $result : [$result];
    }

    /**
     * @param ActiveRecord $model
     * @param array $body
     * @return array
     * @throws Exception
     */
    private static function createSeveral(ActiveRecord $model, array $body): array
    {
        $model->load($body, '');
        if ($model->save()) {
            $result = self::saveRealation($model, $body);
        } elseif (!$model->hasErrors()) {
            throw new Exception('Failed to create the object for unknown reason.');
        } else {
            throw new Exception(implode(BREAKS, $model->getErrors()));
        }
        return $result;
    }

    /**
     * @param ActiveRecord $model
     * @param array $body
     * @return array
     * @throws Exception
     */
    private static function saveRealation(ActiveRecord $model, array $body): array
    {
        $result = [];
        //关联模型
        if (isset($model->realation)) {
            foreach ($model->realation as $key => $val) {
                if (isset($body[$key])) {
                    $child = $model->getRelation($key)->modelClass;
                    if ($body[$key]) {
                        if (ArrayHelper::isAssociative($body[$key])) {
                            $body[$key] = [$body[$key]];
                        }
                        foreach ($body[$key] as $params) {
                            if ($val) {
                                foreach ($val as $c_attr => $p_attr) {
                                    $params[$c_attr] = $model->{$p_attr};
                                }
                            }
                            $child_model = new $child();
                            $res = self::createSeveral($child_model, $params);
                            $result[$key][] = $res;
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

}
