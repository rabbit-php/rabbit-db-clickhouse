<?php
declare(strict_types=1);

namespace rabbit\db\clickhouse;

use Co\Http\Client;
use rabbit\exception\NotSupportedException;
use rabbit\helper\ArrayHelper;
use rabbit\pool\AbstractConnection;
use rabbit\pool\PoolManager;
use rabbit\db\clickhouse\http\Response;

/**
 * Class SwooleTransport
 * @method set(array $settings)
 * @method getDefer()
 * @method setMethod(string $method)
 * @method setHeaders(array $headers)
 * @method setBasicAuth(string $username, string $password)
 * @method setCookies(array $cookies)
 * @method setData($data)
 * @method addFile(string $path, string $name, $type = null, string $filename = null, int $offset = null, int $length = null)
 * @method addData(string $path, string $name, $type = null, string $filename = null)
 * @method execute(string $path)
 * @method get(string $path)
 * @method post(string $path, $data)
 * @method download(string $path, $file, int $offset = null)
 * @method getBody()
 * @method getHeaders()
 * @method getCookies()
 * @method getStatusCode()
 * @method getHeaderOut()
 * @method upgrade(string $path)
 * @method push($data, int $opcode = null, bool $finish = null)
 * @method recv(float $timeout = null)
 * @method close()
 * @package rabbit\db\clickhouse
 */
class HttpClient extends AbstractConnection
{
    /** @var string */
    public $database = 'default';
    /** @var array */
    public $query = [];
    /** @var Client */
    protected $client;

    const SUPPORT = [
        'get',
        'post',
        'head',
        'put',
        'trace',
        'options',
        'delete',
        'lock',
        'mkcol',
        'copy',
        'download'
    ];

    public function createConnection(): void
    {
        $pool = PoolManager::getPool($this->poolKey);
        $dsn = $pool->getConnectionAddress();
        $parsed = parse_url($dsn);
        if (!isset($parsed['path'])) {
            $parsed['path'] = '/';
        }
        isset($parsed['query']) ? parse_str($parsed['query'], $this->query) : $this->query = [];
        if (isset($this->query['dbname'])) {
            $this->database = ArrayHelper::remove($this->query, 'dbname', 'default');
            $this->query['database'] = $this->database;
        }
        $scheme = (isset($parsed['scheme']) && $parsed['scheme'] === 'https' ? $parsed['scheme'] : 'http');
        $client = new Client(
            $parsed['host'],
            isset($parsed['port']) ? $parsed['port'] : ($scheme === 'http' ? 80 : 443),
            $scheme === 'http' ? false : true
        );
        $client->set([
            'timeout' => $pool->getTimeout(),
        ]);
        $client->setHeaders([
            'Content-Type' => 'application/x-www-form-urlencoded'
        ]);
        $user = !empty($parsed['user']) ? $parsed['user'] : '';
        $pwd = !empty($parsed['pass']) ? $parsed['pass'] : '';
        (!empty($user) || !empty($pwd)) && $client->setBasicAuth($user, $pwd);
        $client->setDefer();
        $this->client = $client;
    }

    public function reconnect(): void
    {
        $this->createConnection();
    }

    public function check(): bool
    {
        if (!$this->client->connected) {
            $this->client->get('/');
            $this->client->recv();
        }
        return $this->client->connected;
    }

    /**
     * @param float $timeout
     * @return mixed|void
     * @throws NotSupportedException
     */
    public function receive(float $timeout = -1)
    {
        throw new NotSupportedException('Http client not support ' . __METHOD__);
    }

    /**
     * @param bool $defer
     * @throws NotSupportedException
     */
    public function setDefer($defer = true): bool
    {
        return $this->client->setDefer($defer);
    }

    /**
     * @param $name
     * @param $arguments
     * @return mixed
     * @throws NotSupportedException
     */
    public function __call($name, $arguments)
    {
        if (method_exists($this->client, $name)) {
            if (in_array(strtolower($name), self::SUPPORT)) {
                $this->client->setDefer();
                $this->client->$name(...$arguments);
                $this->client->recv();
                if ($this->client->errCode !== 0) {
                    $this->release();
                    throw new \RuntimeException("Http $name error! msg=" . socket_strerror($this->client->errCode));
                }
                $response = new Response(
                    $this->client->getHeaders(),
                    $this->client->getCookies(),
                    $this->client->getStatusCode(),
                    $this->client->body
                );
                $this->release();
                return $response;
            }
            return $this->client->$name(...$arguments);
        }
        throw new NotSupportedException('Http client not support ' . __METHOD__);
    }

    /**
     * @param array $query
     * @return string
     */
    public function getQueryString(array $query = []): string
    {
        return '?' . http_build_query(array_merge($this->query, $query));
    }
}
