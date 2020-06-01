<?php


namespace rabbit\db\clickhouse\http;

use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\StreamInterface;

/**
 * Class Response
 * @package rabbit\socket\http
 */
class Response implements ResponseInterface
{
    /**
     * @var array
     */
    public static $phrases = [
        100 => 'Continue',
        101 => 'Switching Protocols',
        102 => 'Processing',
        200 => 'OK',
        201 => 'Created',
        202 => 'Accepted',
        203 => 'Non-Authoritative Information',
        204 => 'No Content',
        205 => 'Reset Content',
        206 => 'Partial Content',
        207 => 'Multi-status',
        208 => 'Already Reported',
        300 => 'Multiple Choices',
        301 => 'Moved Permanently',
        302 => 'Found',
        303 => 'See Other',
        304 => 'Not Modified',
        305 => 'Use Proxy',
        306 => 'Switch Proxy',
        307 => 'Temporary Redirect',
        400 => 'Bad Request',
        401 => 'Unauthorized',
        402 => 'Payment Required',
        403 => 'Forbidden',
        404 => 'Not Found',
        405 => 'Method Not Allowed',
        406 => 'Not Acceptable',
        407 => 'Proxy Authentication Required',
        408 => 'Request Time-out',
        409 => 'Conflict',
        410 => 'Gone',
        411 => 'Length Required',
        412 => 'Precondition Failed',
        413 => 'Request Entity Too Large',
        414 => 'Request-URI Too Large',
        415 => 'Unsupported Media Type',
        416 => 'Requested range not satisfiable',
        417 => 'Expectation Failed',
        418 => 'I\'m a teapot',
        422 => 'Unprocessable Entity',
        423 => 'Locked',
        424 => 'Failed Dependency',
        425 => 'Unordered Collection',
        426 => 'Upgrade Required',
        428 => 'Precondition Required',
        429 => 'Too Many Requests',
        431 => 'Request Header Fields Too Large',
        451 => 'Unavailable For Legal Reasons',
        500 => 'Internal Server Error',
        501 => 'Not Implemented',
        502 => 'Bad Gateway',
        503 => 'Service Unavailable',
        504 => 'Gateway Time-out',
        505 => 'HTTP Version not supported',
        506 => 'Variant Also Negotiates',
        507 => 'Insufficient Storage',
        508 => 'Loop Detected',
        511 => 'Network Authentication Required',
    ];

    /**
     * @var string
     */
    protected $reasonPhrase = '';

    /**
     * @var int
     */
    protected $statusCode = 200;

    /**
     * @var array
     */
    protected $cookies = [];

    /**
     * @var array
     */
    protected $headers = [];

    /**
     * @var string
     */
    protected $protocol = '1.1';

    /**
     * @var StreamInterface
     */
    protected $stream;

    /**
     * Response constructor.
     * @param array $header
     * @param array $cookies
     * @param int $statusCode
     * @param string $data
     */
    public function __construct(?array $header, ?array $cookies, ?int $statusCode, ?string $data)
    {
        $this->cookies = $cookies ?? [];
        $this->withStatus($statusCode);
        foreach ($header as $key => $value) {
            $this->withHeader($key, $value);
        }
        $this->withBody(new PHPMemory($data));
    }

    /**
     * @param int $code
     * @param string $reasonPhrase
     * @return Response|static
     */
    public function withStatus($code, $reasonPhrase = '')
    {
        $this->statusCode = (int)$code;
        if (!$reasonPhrase && isset(self::$phrases[$code])) {
            $reasonPhrase = self::$phrases[$code];
        }
        $this->reasonPhrase = $reasonPhrase;
        return $this;
    }

    /**
     * @return string
     */
    public function getReasonPhrase()
    {
        return $this->reasonPhrase;
    }

    /**
     * @return int
     */
    public function getStatusCode()
    {
        return $this->statusCode;
    }

    /**
     * @return string
     */
    public function getProtocolVersion(): string
    {
        return $this->protocol;
    }

    /**
     * @param string $version
     * @return $this|ResponseInterface
     */
    public function withProtocolVersion($version)
    {
        if ($this->protocol === $version) {
            return $this;
        }

        $this->protocol = $version;
        return $this;
    }

    /**
     * @return array
     */
    public function getHeaders(): array
    {
        return $this->headers;
    }

    /**
     * @param $name
     * @return bool
     */
    public function hasHeader($name): bool
    {
        return isset($this->headers[strtolower($name)]);
    }

    /**
     * @param $name
     * @return array
     */
    public function getHeader($name): array
    {
        $name = strtolower($name);
        return isset($this->headers[$name]) ? $this->headers[$name] : [];
    }

    /**
     * @param $name
     * @return string
     */
    public function getHeaderLine($name): string
    {
        return implode(', ', $this->getHeader($name));
    }

    /**
     * @param string $name
     * @param string|string[] $value
     * @return $this|ResponseInterface
     */
    public function withHeader($name, $value)
    {
        $normalized = strtolower($name);
        if (!is_array($value)) {
            $value = [$value];
        }
        $value = $this->trimHeaderValues($value);
        $this->headers[$normalized] = $value;

        return $this;
    }

    /**
     * @param array $values
     * @return array
     */
    private function trimHeaderValues(array $values)
    {
        return array_map(function ($value) {
            return trim($value, " \t");
        }, $values);
    }

    /**
     * @param string $name
     * @param string|string[] $value
     * @return $this|ResponseInterface
     */
    public function withAddedHeader($name, $value)
    {
        $normalized = strtolower($name);

        $this->headers[$name] = $value;
        return $this;
    }

    /**
     * @param $name
     * @return $this|MessageTrait
     */
    public function withoutHeader($name)
    {
        $normalized = strtolower($name);

        if (!isset($this->headers[$normalized])) {
            return $this;
        }

        unset($this->headers[$normalized]);

        return $this;
    }

    /**
     * @param array $headers
     * @return $this
     */
    private function setHeaders(array $headers)
    {
        $this->headerNames = $this->headers = [];
        foreach ($headers as $header => $value) {
            if (!is_array($value)) {
                $value = [$value];
            }

            $value = $this->trimHeaderValues($value);
            $normalized = strtolower($header);
            if (isset($this->headerNames[$normalized])) {
                $header = $this->headerNames[$normalized];
                $this->headers[$header] = array_merge($this->headers[$header], $value);
            } else {
                $this->headerNames[$normalized] = $header;
                $this->headers[$header] = $value;
            }
        }
        return $this;
    }

    /**
     * @return StreamInterface|PHPMemory
     */
    public function getBody()
    {
        if (!$this->stream) {
            $this->stream = new PHPMemory();
        }

        return $this->stream;
    }

    /**
     * @param StreamInterface $body
     * @return $this|ResponseInterface
     */
    public function withBody(StreamInterface $body)
    {
        if ($body === $this->stream) {
            return $this;
        }

        $this->stream = $body;
        return $this;
    }
}
