<?php
declare(strict_types=1);

namespace Rabbit\DB\ClickHouse;
use Rabbit\Base\Helper\StringHelper;
use Rabbit\DB\ExpressionInterface;
use Rabbit\DB\PdoValue;

/**
 * Class ColumnSchema
 * @package Rabbit\DB\ClickHouse
 */
class ColumnSchema extends \Rabbit\DB\ColumnSchema
{
    /**
     * @inheritdoc
     * @param mixed $value
     * @return bool|float|int|PdoValue|mixed|null|string
     */
    protected function typecast($value)
    {
        if ($value === ''
            && !in_array(
                $this->type,
                [
                    Schema::TYPE_TEXT,
                    Schema::TYPE_STRING,
                    Schema::TYPE_BINARY,
                    Schema::TYPE_CHAR
                ],
                true
            )
        ) {
            return null;
        }

        if ($value === null
            || gettype($value) === $this->phpType
            || $value instanceof ExpressionInterface
            || $value instanceof Query
        ) {
            return $value;
        }

        if (is_array($value) && strpos($this->dbType, 'Array(') !== false) {
            return $value;
        } elseif (is_array($value)
            && count($value) === 2
            && isset($value[1])
            && in_array($value[1], $this->getPdoParamTypes(), true)
        ) {
            return new PdoValue($value[0], $value[1]);
        }

        switch ($this->phpType) {
            case 'resource':
            case 'string':
                if (is_resource($value)) {
                    return $value;
                }
                if (is_float($value)) {
                    // ensure type cast always has . as decimal separator in all locales
                    return StringHelper::floatToString($value);
                }
                if (is_array($value)) {
                    return json_encode($value, JSON_UNESCAPED_UNICODE);
                }
                return (string)$value;
            case 'integer':
                return (int)$value;
            case 'boolean':
                return (bool)$value && $value !== "\0";
            case 'double':
                return (float)$value;
        }

        return $value;
    }
}
