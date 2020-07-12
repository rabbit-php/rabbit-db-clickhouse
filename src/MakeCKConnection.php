<?php
declare(strict_types=1);

namespace Rabbit\DB\ClickHouse;

use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\DB\RetryHandler;
use Rabbit\DB\Pool\PdoPool;
use Rabbit\Pool\BaseManager;
use Rabbit\Pool\PoolProperties;
use Throwable;
use function Swlib\Http\parse_query;

/**
 * Class MakeCKConnection
 * @package Rabbit\DB\ClickHouse
 */
class MakeCKConnection
{
    /**
     * @param string $class
     * @param string $name
     * @param string $dsn
     * @param array|null $config
     * @return string
     * @throws Throwable
     */
    public static function addConnection(string $class, string $name, string $dsn, array $config = null): string
    {
        $urlArr = parse_url($dsn);
        $driver = $urlArr['scheme'];
        /** @var BaseManager $manager */
        $manager = getDI($driver);
        if (!$manager->has($name)) {
            $conn = [
                'class' => $class,
                'name' => $name,
                'dsn' => $dsn
            ];
            if (is_array($config)) {
                foreach ($config as $key => $value) {
                    $conn[$key] = $value;
                }
            }
            if (in_array($driver, ['clickhouse', 'clickhouses'])) {
                $manager->add([$name => create($conn, [], false)]);
            } elseif ($driver === 'click') {
                $poolConfig = [
                    'class' => PoolProperties::class,
                ];
                [
                    $poolConfig['minActive'],
                    $poolConfig['maxActive'],
                    $poolConfig['maxWait'],
                    $poolConfig['maxRetry']
                ] = ArrayHelper::getValueByArray(parse_query($urlArr['query']), [
                    'min',
                    'max',
                    'wait',
                    'retry'
                ], [
                    5,
                    5,
                    0,
                    3
                ]);
                $conn['pool'] = create([
                    'class' => PdoPool::class,
                    'poolConfig' => create($poolConfig, [], false)
                ], [], false);
                if (!empty($retryHandler)) {
                    $conn['retryHandler'] = create($retryHandler);
                } else {
                    $conn['retryHandler'] = getDI(RetryHandler::class);
                }
                $manager->add([$name => $conn]);
            } else {
                throw new InvalidConfigException("Not support driver $driver");
            }

        }
        return $driver;
    }
}
