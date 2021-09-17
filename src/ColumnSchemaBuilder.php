<?php

declare(strict_types=1);

namespace Rabbit\DB\ClickHouse;

class ColumnSchemaBuilder extends \Rabbit\DB\ColumnSchemaBuilder
{
    /**
     * @inheritdoc
     */
    public function __toString()
    {
        switch ($this->getTypeCategory()) {
            case self::CATEGORY_NUMERIC:
                $format = '{unsigned}{type}{default}';
                break;
            default:
                $format = '{type}{default}';
        }

        return $this->buildCompleteString($format);
    }

    /**
     * @inheritdoc
     */
    protected function buildUnsignedString(): string
    {
        return $this->isUnsigned ? 'U' : '';
    }
}
