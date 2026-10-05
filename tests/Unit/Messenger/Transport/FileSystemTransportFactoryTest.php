<?php

declare(strict_types=1);

namespace MauticPlugin\FileSystemQueueBundle\Tests\Unit\Messenger\Transport;

use Mautic\CoreBundle\Helper\CoreParametersHelper;
use MauticPlugin\FileSystemQueueBundle\Messenger\Transport\FileSystemTransport;
use MauticPlugin\FileSystemQueueBundle\Messenger\Transport\FileSystemTransportFactory;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpKernel\KernelInterface;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;

final class FileSystemTransportFactoryTest extends TestCase
{
    private string $projectDir;
    private FileSystemTransportFactory $factory;

    protected function setUp(): void
    {
        $this->projectDir = sys_get_temp_dir() . '/fsq_factory_' . uniqid('', true);
        mkdir($this->projectDir . '/var/queue', 0777, true);

        $kernel = $this->createMock(KernelInterface::class);
        $kernel->method('getProjectDir')->willReturn($this->projectDir);

        $params = $this->createMock(CoreParametersHelper::class);

        $this->factory = new FileSystemTransportFactory($kernel, $params, new PhpSerializer());
    }

    protected function tearDown(): void
    {
        $this->removeDirectory($this->projectDir);
    }

    public function testSupportsFilesystemDsn(): void
    {
        self::assertTrue($this->factory->supports('filesystem://default/email', []));
        self::assertFalse($this->factory->supports('doctrine://default', []));
    }

    public function testCreateTransportCreatesDirectoryAndReturnsTransport(): void
    {
        $transport = $this->factory->createTransport(
            'filesystem://default/email',
            [],
            new PhpSerializer()
        );

        self::assertInstanceOf(FileSystemTransport::class, $transport);
        self::assertDirectoryExists($this->projectDir . '/var/queue/email');
        self::assertSame($this->projectDir . '/var/queue/email', $transport->getDirectory());
    }

    private function removeDirectory(string $dir): void
    {
        if (!is_dir($dir)) {
            return;
        }
        $files = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($dir, \FilesystemIterator::SKIP_DOTS),
            \RecursiveIteratorIterator::CHILD_FIRST
        );
        foreach ($files as $file) {
            $file->isDir() ? rmdir($file->getPathname()) : unlink($file->getPathname());
        }
        rmdir($dir);
    }
}
