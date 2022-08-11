<?php

declare(strict_types=1);

namespace Rabbit\DB\ClickHouse;

use Generator;
use Rabbit\DB\Exception;
use Rabbit\DB\Expression;
use Rabbit\DB\ExpressionInterface;
use Rabbit\DB\Query;
use Rabbit\DB\QueryInterface;

/**
 * Class QueryBuilder
 * @package Rabbit\DB\ClickHouse
 */
class QueryBuilder extends \Rabbit\DB\QueryBuilder
{
    public array $typeMap = [
        Schema::TYPE_CHAR => 'FixedString(1)',
        Schema::TYPE_STRING => 'String',
        Schema::TYPE_TEXT => 'String',
        Schema::TYPE_SMALLINT => 'Int8',
        Schema::TYPE_INTEGER => 'Int32',
        Schema::TYPE_BIGINT => 'Int64',
        Schema::TYPE_FLOAT => 'Float32',
        Schema::TYPE_DOUBLE => 'Float64',
        Schema::TYPE_DECIMAL => 'Float32',
        Schema::TYPE_DATETIME => 'DateTime',
        Schema::TYPE_TIME => 'DateTime',
        Schema::TYPE_DATE => 'Date',
        Schema::TYPE_BINARY => 'String',
        Schema::TYPE_BOOLEAN => 'Int8',
        Schema::TYPE_MONEY => 'Float32',
    ];

    private function prepareFromByModel(QueryInterface $query): void
    {
        if (empty($query->from) && $query instanceof ActiveQuery && !empty($query->modelClass)) {
            $modelClass = $query->modelClass;
            $query->from = [call_user_func($modelClass . '::tableName')];
        }
    }

    public function build(Query $query, array $params = []): array
    {
        $query = $query->prepare($this);

        $params = [...$params, ...$query->params];

        $this->prepareFromByModel($query);

        $clauses = [
            $this->buildSelect($query->select, $params, $query->distinct, $query->selectOption),
            $this->buildFrom($query->from, $params),
            property_exists($query, 'sample') ? $this->buildSample($query->sample) : '',
            $this->buildJoin($query->join, $params),
            property_exists($query, 'preWhere') ? $this->buildPreWhere($query->preWhere, $params) : '',
            $this->buildWhere($query->where, $params),
            $this->buildGroupBy($query->groupBy, $params),
            $this->buildHaving($query->having, $params),
        ];

        $sql = implode($this->separator, array_filter($clauses));

        $orderBy = $this->buildOrderBy($query->orderBy, $params);
        if ($orderBy !== '') {
            $sql .= $this->separator . $orderBy;
        }
        $limitBy = property_exists($query, 'limitBy') ? $this->buildLimitBy($query->limitBy) : '';
        if ($limitBy !== '') {
            $sql .= $this->separator . $limitBy;
        }
        $limit = $this->buildLimit($query->limit, $query->offset);
        if ($limit !== '') {
            $sql .= $this->separator . $limit;
        }

        if (!empty($query->orderBy)) {
            foreach ($query->orderBy as $expression) {
                if ($expression instanceof Expression) {
                    $params = [...$params, ...$expression->params];
                }
            }
        }
        if (!empty($query->groupBy)) {
            foreach ($query->groupBy as $expression) {
                if ($expression instanceof Expression) {
                    $params = [...$params, ...$expression->params];
                }
            }
        }

        $union = $this->buildUnion($query->union, $params);
        if ($union !== '') {
            $sql = "$sql{$this->separator}$union";
        }

        return [$sql, $params];
    }

    public function buildPreWhere(string|array|null $condition, array &$params): string
    {
        $where = $this->buildCondition($condition, $params);
        return $where === '' ? '' : 'PREWHERE ' . $where;
    }

    public function buildSample(?string $condition): string
    {
        return $condition !== null ? ' SAMPLE ' . $condition : '';
    }

    public function createTable(string $table, array $columns, string $options = null): string
    {
        if ($options === null) {
            throw new Exception('Need set specific settings for engine table');
        }
        return parent::createTable($table, $columns, $options);
    }

    public function getColumnType(\Rabbit\DB\ColumnSchemaBuilder|string $type): string
    {
        if ($type instanceof ColumnSchemaBuilder) {
            $type = $type->__toString();
        }

        if (isset($this->typeMap[$type])) {
            return $this->typeMap[$type];
        } elseif (preg_match('/^(\w+)\s+/', $type, $matches)) {
            if (isset($this->typeMap[$matches[1]])) {
                return preg_replace('/^\w+/', $this->typeMap[$matches[1]], $type);
            }
        } elseif (preg_match('/^U(\w+)/', $type, $matches)) {
            if (isset($this->typeMap[$matches[1]])) {
                return 'U' . $this->typeMap[$matches[1]];
            }
        }

        return $type;
    }

    public function buildLimit(?int $limit, ?int $offset): string
    {
        $sql = '';
        if ($this->hasOffset($offset)) {
            $sql .= 'LIMIT ' . $offset . ' , ' . $limit;
        } else {
            if ($this->hasLimit($limit)) {
                $sql = 'LIMIT ' . $limit;
            }
        }

        return ltrim($sql);
    }

    public function buildLimitBy(?array $limitBy): string
    {
        if (empty($limitBy)) {
            return '';
        }
        $n = $limitBy[0];
        return 'LIMIT ' . $n . ' BY ' . implode(',', $limitBy[1]);
    }

    public function buildUnion(?array $unions, array &$params): string
    {
        if (empty($unions)) {
            return '';
        }

        $result = '';

        foreach ($unions as $i => $union) {
            $query = $union['query'];
            if ($query instanceof Query) {
                list($unions[$i]['query'], $params) = $this->build($query, $params);
            }

            $result .= 'UNION ' . ($union['all'] ? 'ALL ' : '') . $unions[$i]['query'];
        }

        return trim($result);
    }

    public function selectExists(string $rawSql): string
    {
        return 'SELECT count(*) FROM (' . $rawSql . ')';
    }

    public function addColumn(string $table, string $column, string $type): string
    {
        return 'ALTER TABLE ' . $this->db->quoteTableName($table)
            . ' ADD COLUMN ' . $this->db->quoteColumnName($column) . ' '
            . $this->getColumnType($type);
    }

    protected function prepareInsertValues(string $table, array|Query $columns, array $params = []): array
    {
        $schema = $this->db->getSchema();
        $tableSchema = $schema->getTableSchema($table);
        $columnSchemas = $tableSchema !== null ? $tableSchema->columns : [];
        $names = [];
        $placeholders = [];
        $values = ' DEFAULT VALUES';
        if ($columns instanceof Query) {
            list($names, $values, $params) = $this->prepareInsertSelectSubQuery($columns, $schema, $params);
        } else {
            foreach ($columns as $name => $value) {
                $names[] = $schema->quoteColumnName($name);

                if (isset($columnSchemas[$name]) && $columnSchemas[$name]->type === 'bigint') {
                    $value = new Expression($value);
                } else {
                    $value = isset($columnSchemas[$name]) ? $columnSchemas[$name]->dbTypecast($value) : $value;
                }

                if ($value instanceof ExpressionInterface) {
                    $placeholders[] = $this->buildExpression($value, $params);
                } elseif ($value instanceof Query) {
                    list($sql, $params) = $this->build($value, $params);
                    $placeholders[] = "($sql)";
                } else {
                    $placeholders[] = $this->bindParam($value, $params);
                }
            }
        }

        return [$names, $placeholders, $values, $params];
    }

    public function batchInsert(string $table, array $columns, array|Generator $rows, array &$params = []): string
    {
        if (empty($rows)) {
            return '';
        }

        $schema = $this->db->getSchema();
        if (($tableSchema = $schema->getTableSchema($table)) !== null) {
            $columnSchemas = $tableSchema->columns;
        } else {
            $columnSchemas = [];
        }

        $values = [];
        foreach ($rows as $row) {
            $vs = [];
            foreach ($row as $i => $value) {
                if (isset($columns[$i], $columnSchemas[$columns[$i]])) {
                    $value = $columnSchemas[$columns[$i]]->dbTypecast($value);

                    if (in_array($columnSchemas[$columns[$i]]->type, ['bigint'])) {
                        $value = new Expression($value);
                    }
                }

                if (is_string($value)) {
                    $value = $schema->quoteValue($value);
                } elseif ($value === false) {
                    $value = 0;
                } elseif ($value === null) {
                    $value = 'NULL';
                } elseif ($value instanceof ExpressionInterface) {
                    $value = $this->buildExpression($value, $params);
                }
                $vs[] = $value;
            }
            $values[] = '(' . implode(', ', $vs) . ')';
        }
        if (empty($values)) {
            return '';
        }

        foreach ($columns as $i => $name) {
            $columns[$i] = $schema->quoteColumnName($name);
        }

        return 'INSERT INTO ' . $schema->quoteTableName($table)
            . ' (' . implode(', ', $columns) . ') VALUES ' . implode(', ', $values);
    }

    public function update(string $table, array $columns, array|string $condition, array &$params = []): string
    {
        [$lines, $params] = $this->prepareUpdateSets($table, $columns, $params);
        $sql = 'ALTER TABLE ' . $this->db->quoteTableName($table) . ' UPDATE ' . implode(', ', $lines);
        $where = $this->buildWhere($condition, $params);
        return ($where === '' ? $sql : $sql . ' ' . $where);
    }

    public function delete(string $table, array|string $condition, array &$params): string
    {
        $sql = 'ALTER TABLE ' . $this->db->quoteTableName($table) . ' DELETE ';
        $where = $this->buildWhere($condition, $params);

        return ($where === '' ? $sql : $sql . ' ' . $where);
    }
}
