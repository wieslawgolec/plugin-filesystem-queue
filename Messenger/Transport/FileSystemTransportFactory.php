<?php

declare(strict_types=1);

namespace MauticPlugin\FileSystemQueueBundle\Messenger\Transport;

use Mautic\CoreBundle\Helper\CoreParametersHelper;
use Symfony\Component\HttpKernel\KernelInterface;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

class FileSystemTransportFactory implements TransportFactoryInterface
{
    public const FILESYSTEM_DSN = 'filesystem://';
    public const QUEUE_DIR      = '/var/queue';

    private string $projectDir;
    private SerializerInterface $serializer;

    public function __construct(
        KernelInterface $kernel,
        private CoreParametersHelper $coreParametersHelper,
        ?SerializerInterface $serializer = null,
    ) {
        $this->serializer = $serializer ?? new PhpSerializer();
        $this->projectDir = $kernel->getProjectDir() . self::QUEUE_DIR;
    }

    public function createTransport(string $dsn, array $options, SerializerInterface $serializer): TransportInterface
    {
        if (!str_starts_with($dsn, self::FILESYSTEM_DSN)) {
            throw new \InvalidArgumentException('Unsupported DSN: ' . $dsn);
        }

        $path          = parse_url($dsn, PHP_URL_PATH) ?? '';
        $fullDirectory = $this->projectDir . rtrim($path, '/');

        if (!is_dir($fullDirectory) && !@mkdir($fullDirectory, 0775, true) && !is_dir($fullDirectory)) {
            throw new \RuntimeException("Cannot create queue directory: $fullDirectory");
        }

        if (!is_writable($fullDirectory)) {
            throw new \RuntimeException("Queue directory not writable: $fullDirectory");
        }

        return new FileSystemTransport(
            $fullDirectory,
            $serializer,
            $this->coreParametersHelper
        );
    }

    public function supports(string $dsn, array $options): bool
    {
        return str_starts_with($dsn, self::FILESYSTEM_DSN);
    }

    public function getProjectDir(): string
    {
        return $this->projectDir;
    }
}
