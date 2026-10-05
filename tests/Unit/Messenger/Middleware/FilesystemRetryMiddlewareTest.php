<?php

declare(strict_types=1);

namespace MauticPlugin\FileSystemQueueBundle\Tests\Unit\Messenger\Middleware;

use Mautic\CoreBundle\Helper\CoreParametersHelper;
use MauticPlugin\FileSystemQueueBundle\Messenger\Middleware\FilesystemRetryMiddleware;
use MauticPlugin\FileSystemQueueBundle\Messenger\Stamp\AttemptStamp;
use MauticPlugin\FileSystemQueueBundle\Messenger\Transport\FileSystemTransport;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;
use Symfony\Component\Messenger\Middleware\StackMiddleware;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;

final class FilesystemRetryMiddlewareTest extends TestCase
{
    private CoreParametersHelper&MockObject $params;
    private FileSystemTransport&MockObject $transport;
    private FilesystemRetryMiddleware $middleware;

    protected function setUp(): void
    {
        $this->params = $this->createMock(CoreParametersHelper::class);
        $this->params->method('get')->willReturnCallback(
            fn (string $key, mixed $default = null) => match ($key) {
                'mautic.filesystem_queue_max_attempts' => 5,
                default                                => $default,
            }
        );

        $this->transport  = $this->createMock(FileSystemTransport::class);
        $this->middleware = new FilesystemRetryMiddleware($this->params, $this->transport);
    }

    public function testIgnoresNonReceivedEnvelopes(): void
    {
        $envelope = new Envelope(new \stdClass());
        $stack    = $this->createPassthroughStack();

        $result = $this->middleware->handle($envelope, $stack);
        self::assertSame($envelope, $result);
    }

    public function testTemporaryFailureTriggersRequeue(): void
    {
        $envelope = (new Envelope(new \stdClass()))
            ->with(new ReceivedStamp('filesystem'))
            ->with(new TransportMessageIdStamp('test-id'))
            ->with(new AttemptStamp(1));

        $this->transport
            ->expects(self::once())
            ->method('requeueWithRetry')
            ->with($envelope, 2);

        $stack = $this->createThrowingStack(new \RuntimeException('temporary failure'));

        $result = $this->middleware->handle($envelope, $stack);
        self::assertSame($envelope, $result); // returned → worker will ack
    }

    public function testFinalFailureIsRethrown(): void
    {
        $envelope = (new Envelope(new \stdClass()))
            ->with(new ReceivedStamp('filesystem'))
            ->with(new TransportMessageIdStamp('test-id'))
            ->with(new AttemptStamp(4)); // 4 + 1 = 5 == max → final

        $this->transport
            ->expects(self::never())
            ->method('requeueWithRetry');

        $stack = $this->createThrowingStack(new \RuntimeException('final failure'));

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('final failure');

        $this->middleware->handle($envelope, $stack);
    }

    public function testDoesNothingWhenTransportIsNull(): void
    {
        $middleware = new FilesystemRetryMiddleware($this->params, null);

        $envelope = (new Envelope(new \stdClass()))
            ->with(new ReceivedStamp('filesystem'))
            ->with(new AttemptStamp(0));

        $stack = $this->createThrowingStack(new \RuntimeException('should bubble'));

        $this->expectException(\RuntimeException::class);
        $middleware->handle($envelope, $stack);
    }

    private function createPassthroughStack(): StackInterface
    {
        $next = new class implements MiddlewareInterface {
            public function handle(Envelope $envelope, StackInterface $stack): Envelope
            {
                return $envelope;
            }
        };

        return new StackMiddleware($next);
    }

    private function createThrowingStack(\Throwable $e): StackInterface
    {
        $next = new class($e) implements MiddlewareInterface {
            public function __construct(private \Throwable $e) {}

            public function handle(Envelope $envelope, StackInterface $stack): Envelope
            {
                throw $this->e;
            }
        };

        return new StackMiddleware($next);
    }
}
