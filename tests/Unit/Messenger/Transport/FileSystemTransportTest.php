<?php

declare(strict_types=1);

namespace MauticPlugin\FileSystemQueueBundle\Tests\Unit\Messenger\Transport;

use Mautic\CoreBundle\Helper\CoreParametersHelper;
use MauticPlugin\FileSystemQueueBundle\Messenger\Stamp\AttemptStamp;
use MauticPlugin\FileSystemQueueBundle\Messenger\Transport\FileSystemTransport;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;

final class FileSystemTransportTest extends TestCase
{
    private string $directory;
    private CoreParametersHelper&MockObject $params;
    private FileSystemTransport $transport;

    protected function setUp(): void
    {
        $this->directory = sys_get_temp_dir() . '/fsq_test_' . uniqid('', true);
        mkdir($this->directory, 0777, true);

        $this->params = $this->createMock(CoreParametersHelper::class);
        $this->params->method('get')->willReturnCallback(function (string $key, mixed $default = null) {
            return match ($key) {
                'mautic.filesystem_queue_batch_size'          => 10,
                'mautic.filesystem_queue_batch_auto_shuffle'  => false,
                'mautic.filesystem_queue_max_attempts'        => 5,
                'mautic.filesystem_queue_recovery_timeout'    => 60,
                'mautic.filesystem_queue_recovery_interval'   => 30,
                'mautic.filesystem_queue_recovery_stuck'      => true,
                'mautic.filesystem_queue_retry_backoff_base'  => 0, // disable backoff for most tests
                'mautic.filesystem_queue_send_max_retries'    => 5,
                'mautic.messenger_retry_strategy_max_retries' => 2,
                'mautic.messenger_retry_strategy_delay'       => 10,
                'mautic.messenger_retry_strategy_multiplier'  => 1.0,
                'mautic.messenger_retry_strategy_max_delay'   => 0,
                default                                      => $default,
            };
        });

        $this->transport = new FileSystemTransport(
            $this->directory,
            new PhpSerializer(),
            $this->params
        );
    }

    protected function tearDown(): void
    {
        $this->removeDirectory($this->directory);
    }

    public function testSendCreatesMessageFile(): void
    {
        $envelope = new Envelope(new \stdClass());
        $result   = $this->transport->send($envelope);

        $idStamp = $result->last(TransportMessageIdStamp::class);
        self::assertInstanceOf(TransportMessageIdStamp::class, $idStamp);
        $id = (string) $idStamp->getId();

        $file = $this->directory . '/' . $id . '.message';
        self::assertFileExists($file);

        $content = file_get_contents($file);
        self::assertNotFalse($content);
        $data = json_decode($content, true);
        self::assertIsArray($data);
    }

    public function testGetClaimsMessageAtomically(): void
    {
        $envelope = $this->transport->send(new Envelope(new \stdClass()));
        $id       = (string) $envelope->last(TransportMessageIdStamp::class)->getId();

        $claimed = $this->transport->get();
        self::assertCount(1, $claimed);

        /** @var Envelope $claimedEnvelope */
        $claimedEnvelope = $claimed[0];
        self::assertSame($id, (string) $claimedEnvelope->last(TransportMessageIdStamp::class)->getId());
        self::assertInstanceOf(AttemptStamp::class, $claimedEnvelope->last(AttemptStamp::class));
        self::assertSame(0, $claimedEnvelope->last(AttemptStamp::class)->attempt);

        // Original ready file is gone, processing file exists
        self::assertFileDoesNotExist($this->directory . '/' . $id . '.message');
        self::assertFileExists($this->directory . '/' . $id . '.processing');
    }

    public function testSecondClaimOnSameIdFails(): void
    {
        $envelope = $this->transport->send(new Envelope(new \stdClass()));
        $id       = (string) $envelope->last(TransportMessageIdStamp::class)->getId();

        $first = $this->transport->tryClaim($id);
        self::assertNotNull($first);

        $second = $this->transport->tryClaim($id);
        self::assertNull($second);
    }

    public function testAckDeletesProcessingFile(): void
    {
        $envelope = $this->transport->send(new Envelope(new \stdClass()));
        $id       = (string) $envelope->last(TransportMessageIdStamp::class)->getId();

        $claimed = $this->transport->tryClaim($id);
        self::assertNotNull($claimed);
        self::assertFileExists($this->directory . '/' . $id . '.processing');

        $this->transport->ack($claimed);
        self::assertFileDoesNotExist($this->directory . '/' . $id . '.processing');
        self::assertFileDoesNotExist($this->directory . '/' . $id . '.message');
    }

    public function testRejectDeletesProcessingFile(): void
    {
        $envelope = $this->transport->send(new Envelope(new \stdClass()));
        $id       = (string) $envelope->last(TransportMessageIdStamp::class)->getId();

        $claimed = $this->transport->tryClaim($id);
        self::assertNotNull($claimed);

        $this->transport->reject($claimed);
        self::assertFileDoesNotExist($this->directory . '/' . $id . '.processing');
        self::assertFileDoesNotExist($this->directory . '/' . $id . '.message');
    }

    public function testRequeueWithRetryCreatesRetryFile(): void
    {
        $envelope = $this->transport->send(new Envelope(new \stdClass()));
        $id       = (string) $envelope->last(TransportMessageIdStamp::class)->getId();

        $claimed = $this->transport->tryClaim($id);
        self::assertNotNull($claimed);
        self::assertFileExists($this->directory . '/' . $id . '.processing');

        $this->transport->requeueWithRetry($claimed, 1);

        self::assertFileDoesNotExist($this->directory . '/' . $id . '.processing');
        self::assertFileExists($this->directory . '/' . $id . '.message.retry.1');
    }

    public function testClaimAfterRetryCarriesCorrectAttempt(): void
    {
        $envelope = $this->transport->send(new Envelope(new \stdClass()));
        $id       = (string) $envelope->last(TransportMessageIdStamp::class)->getId();

        $claimed = $this->transport->tryClaim($id);
        $this->transport->requeueWithRetry($claimed, 2);

        $reclaimed = $this->transport->tryClaim($id);
        self::assertNotNull($reclaimed);
        self::assertSame(2, $reclaimed->last(AttemptStamp::class)->attempt);
        self::assertFileExists($this->directory . '/' . $id . '.processing.retry.2');
    }

    public function testRecoverStuckProcessingFiles(): void
    {
        $envelope = $this->transport->send(new Envelope(new \stdClass()));
        $id       = (string) $envelope->last(TransportMessageIdStamp::class)->getId();

        $claimed = $this->transport->tryClaim($id);
        self::assertNotNull($claimed);

        $processing = $this->directory . '/' . $id . '.processing';
        // Make it look stuck (mtime far in the past)
        touch($processing, time() - 3600);

        $this->transport->recoverStuckProcessingFiles();

        self::assertFileDoesNotExist($processing);
        self::assertFileExists($this->directory . '/' . $id . '.message');
    }

    public function testGetMessageCount(): void
    {
        self::assertSame(0, $this->transport->getMessageCount());

        $this->transport->send(new Envelope(new \stdClass()));
        $this->transport->send(new Envelope(new \stdClass()));

        self::assertSame(2, $this->transport->getMessageCount());
    }

    public function testTempFilesAreIgnored(): void
    {
        // Create a fake temp file that should never be claimed
        file_put_contents($this->directory . '/fake.message.tmp.1234.abcd', '{}');

        $envelopes = $this->transport->get();
        self::assertCount(0, $envelopes);
    }

    public function testKeepaliveUpdatesMtime(): void
    {
        $envelope = $this->transport->send(new Envelope(new \stdClass()));
        $id       = (string) $envelope->last(TransportMessageIdStamp::class)->getId();

        $claimed = $this->transport->tryClaim($id);
        self::assertNotNull($claimed);

        $processing = $this->directory . '/' . $id . '.processing';
        $oldMtime   = filemtime($processing);

        // Ensure at least 1 second difference
        sleep(1);
        $this->transport->keepalive($claimed);

        clearstatcache();
        $newMtime = filemtime($processing);
        self::assertGreaterThan($oldMtime, $newMtime);
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
