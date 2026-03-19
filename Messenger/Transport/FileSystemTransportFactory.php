<?php

declare(strict_types=1);

namespace MauticPlugin\FileSystemQueueBundle\Messenger\Transport;

use Mautic\CoreBundle\Helper\CoreParametersHelper;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Symfony\Component\HttpKernel\KernelInterface;

class FileSystemTransportFactory implements TransportFactoryInterface
{
    public const FILESYSTEM_DSN = 'filesystem://';
    public const QUEUE_DIR = '/var/queue';
    private string $projectDir;

    private SerializerInterface $serializer;

    public function __construct(
        KernelInterface $kernel,
        ?SerializerInterface $serializer = null,
        private CoreParametersHelper $coreParametersHelper
    ) {
        $this->serializer = $serializer ?? new PhpSerializer();

        $this->projectDir = $kernel->getProjectDir() . self::QUEUE_DIR;
    }

    public function getProjectDir(): string
    {
        return $this->projectDir;
    }
    
    public function getSerializer(): SerializerInterface 
    {
        return $this->serializer;
    }

    public function createTransport(string $dsn, array $options, SerializerInterface $serializer): TransportInterface
    {
        if (!str_starts_with($dsn, self::FILESYSTEM_DSN)) {
            throw new \InvalidArgumentException('Unsupported DSN');
        }

        $path = parse_url($dsn, PHP_URL_PATH) ?? '';
        $fullDirectory = $this->projectDir . rtrim($path, '/');

        if (!is_dir($fullDirectory) && !mkdir($fullDirectory, 0775, true)) {
            throw new \RuntimeException("Cannot create: $fullDirectory");
        }

        return new FileSystemTransport($fullDirectory, $this->serializer, $this->coreParametersHelper);
    }

    public function supports(string $dsn, array $options): bool
    {
        return str_starts_with($dsn, self::FILESYSTEM_DSN);
    }
}
