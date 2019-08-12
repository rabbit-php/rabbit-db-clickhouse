<?php


namespace rabbit\db\clickhouse;

use rabbit\helper\StringHelper;

/**'
 * Class BatchInsert
 * @package rabbit\db
 */
class BatchInsert extends \rabbit\db\BatchInsert
{
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
        $this->hasRows++;
        if ($checkFields) {
            foreach ($rows as $i => $value) {
                $exist = isset($this->columns[$i], $this->columnSchemas[trim($this->columns[$i], '`')]);
                if ($exist) {
                    $columnSchema = $this->columnSchemas[trim($this->columns[$i], '`')];
                    $value = $columnSchema->dbTypecast($value);
                }
                if (is_string($value)) {
                    if ($columnSchema !== null) {
                        switch (true) {
                            case $columnSchema->dbType === "DateTime":
                            case $columnSchema->dbType === "Date":
                                $value = strtotime($value);
                                break;
                            case strpos($columnSchema->dbType, "Int") !== false:
                                if (strtolower($value) === 'false') {
                                    $value = 0;
                                } elseif (strtolower($value) === 'true') {
                                    $value = 1;
                                } else {
                                    $value = intval($value);
                                }
                                break;
                            case strpos($columnSchema->dbType, "Float") !== false:
                            case strpos($columnSchema->dbType, "Decimal") !== false:
                                if (strtolower($value) === 'false') {
                                    $value = 0;
                                } elseif (strtolower($value) === 'true') {
                                    $value = 1;
                                } else {
                                    $value = floatval($value);
                                }
                                break;
                            default:
                                $value = $this->schema->quoteValue($value);
                        }
                    }
                } elseif (is_float($value)) {
                    // ensure type cast always has . as decimal separator in all locales
                    $value = StringHelper::floatToString($value);
                } elseif ($value === false) {
                    $value = 0;
                } elseif ($value === null) {
                    if ($columnSchema !== null) {
                        $dbType = $columnSchema->dbType;
                        switch (true) {
                            case strpos($dbType, "Int") !== false:
                            case strpos($dbType, "Float") !== false:
                            case strpos($dbType, "Decimal") !== false:
                            case $dbType === "DateTime":
                            case $dbType === "Date":
                                $value = 0;
                                break;
                            case strpos($dbType, "String") !== false:
                            default:
                                $value = $this->schema->quoteValue("");
                        }
                    } else {
                        $value = "NULL";
                    }
                } elseif (is_array($value)) {
                    switch (true) {
                        case strpos($columnSchema->dbType, "Int") !== false:
                            foreach ($value as $index => $v) {
                                $value[$index] = intval($v);
                            }
                            $value = str_replace('"', "", json_encode($value, JSON_UNESCAPED_UNICODE));
                            break;
                        case strpos($columnSchema->dbType, "Float") !== false:
                        case strpos($columnSchema->dbType, "Decimal") !== false:
                            foreach ($value as $index => $v) {
                                $value[$index] = floatval($v);
                            }
                            $value = str_replace('"', "", json_encode($value, JSON_UNESCAPED_UNICODE));
                            break;
                        default:
                            $value = str_replace('"', "'", json_encode($value, JSON_UNESCAPED_UNICODE));
                    }
                }
                $rows[$i] = $value;
            }
        }
        $this->sql .= '(' . implode(', ', $rows) . '),';
        return true;
    }
}