<?php

namespace rabbit\db\clickhouse;

use rabbit\activerecord\ActiveRecord;
use rabbit\db\DBHelper;
use rabbit\db\Query;

/**
 * Class IndexExt
 * @package rabbit\db\mysql
 */
class IndexExt
{
    /**
     * @param Query $query
     * @param array $filter
     * @param int $page
     * @return array
     */
    public static function index(Query $query, array $filter = [], int $page = 0): array
    {
        return DBHelper::SearchList($query, $filter, $page);
    }
}
