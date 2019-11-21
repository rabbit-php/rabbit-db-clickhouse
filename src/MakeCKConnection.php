<?php


namespace rabbit\db\clickhouse;

use rabbit\core\ObjectFactory;

class MakeCKConnection
{
    /**
     * @throws \DI\DependencyException
     * @throws \DI\NotFoundException
     */
    public static function addConnection(string $class, string $name, string $dsn, array $config = null): string
    {
        $driver = parse_url($dsn, PHP_URL_SCHEME);
        /** @var Manager $manager */
        if (in_array($driver, ['http', 'https'])) {
            $manager = getDI('clickhouse');
        } else {
            $manager = getDI('click');
        }
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
            $manager->addConnection([$name => ObjectFactory::createObject($conn, [], false)]);
        }
        return $driver;
    }
}
