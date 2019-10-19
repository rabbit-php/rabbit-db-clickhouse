<?php


namespace rabbit\db\clickhouse;

use rabbit\core\ObjectFactory;

class MakeCKConnection
{
    /**
     * @throws \DI\DependencyException
     * @throws \DI\NotFoundException
     */
    public static function addConnection(string $class, string $name, string $dsn, array $config = null): void
    {
        /** @var Manager $manager */
        $manager = getDI('clickhouse');
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
    }
}
