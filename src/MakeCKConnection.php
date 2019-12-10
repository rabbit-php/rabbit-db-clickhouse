<?php


namespace rabbit\db\clickhouse;

use rabbit\core\ObjectFactory;
use rabbit\exception\InvalidConfigException;
use rabbit\helper\ArrayHelper;
use function Swlib\Http\parse_query;

class MakeCKConnection
{
    /**
     * @throws \DI\DependencyException
     * @throws \DI\NotFoundException
     */
    public static function addConnection(string $class, string $name, string $dsn, array $config = null): string
    {
        $urlArr = parse_url($dsn);
        $driver = $urlArr['scheme'];
        /** @var Manager $manager */
        $manager = getDI($driver);
        if (!$manager->hasConnection($name)) {
            $conn = [
                'class' => $class,
                'dsn' => $dsn,
            ];
            if (is_array($config)) {
                foreach ($config as $key => $value) {
                    $conn[$key] = $value;
                }
            }
            if ($driver === 'clickhouse') {
                $manager->addConnection([$name => ObjectFactory::createObject($conn, [], false)]);
            } elseif ($driver === 'click') {
                $poolConfig = [
                    'class' => \rabbit\db\pool\PdoPoolConfig::class,
                ];
                [
                    $poolConfig['minActive'],
                    $poolConfig['maxActive'],
                    $poolConfig['maxWait'],
                    $poolConfig['maxReconnect']
                ] = ArrayHelper::getValueByArray(parse_query($urlArr['query']), [
                    'min',
                    'max',
                    'wait',
                    'retry'
                ], null, [
                    5,
                    5,
                    0,
                    3
                ]);
                $conn['pool'] = ObjectFactory::createObject([
                    'class' => \rabbit\db\pool\PdoPool::class,
                    'poolConfig' => ObjectFactory::createObject($poolConfig, [], false)
                ], [], false);
                $manager->addConnection([$name => $conn]);
            } else {
                throw new InvalidConfigException("Not support driver $driver");
            }

        }
        return $driver;
    }
}
