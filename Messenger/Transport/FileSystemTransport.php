<?php

declare(strict_types=1);

namespace MauticPlugin\FileSystemQueueBundle\Messenger\Transport;

use Mautic\CoreBundle\Helper\CoreParametersHelper;
use MauticPlugin\FileSystemQueueBundle\Messenger\Stamp\AttemptStamp;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Receiver\KeepaliveReceiverInterface;
use Symfony\Component\Messenger\Transport\Receiver\ListableReceiverInterface;
use Symfony\Component\Messenger\Transport\Receiver\MessageCountAwareInterface;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

/**
 * Filesystem transport with Mautic-4 style state machine:
 *
 *   {id}.message                  – pending (attempt 0)
 *   {id}.message.retry.N          – pending after N failures
 *   {id}.processing               – claimed (attempt 0)
 *   {id}.processing.retry.N       – claimed (attempt N)
 *
 * Temporary failure  → rename back to .message.retry.N  (middleware forces ack)
 * Final failure      → reject() deletes file → Messenger sends to failed transport
 */
class FileSystemTransport implements
    TransportInterface,
    ListableReceiverInterface,
    MessageCountAwareInterface,
    KeepaliveReceiverInterface
{
    public const MESSAGE_EXTENSION    = '.message';
    public const PROCESSING_EXTENSION = '.processing';
    public const RETRY_SUFFIX         = '.retry.';
    public const SEND_MAX_RETRIES     = 15;

    private SerializerInterface $serializer;
    private string $directory;
    private ?int $lastRecoveryTime = null;

    public function __construct(
        string $directory,
        ?SerializerInterface $serializer,
        private CoreParametersHelper $coreParametersHelper,
    ) {
        $this->directory  = rtrim($directory, '/');
        $this->serializer = $serializer ?? new PhpSerializer();
    }

    public function getDirectory(): string
    {
        return $this->directory;
    }

    public function getSerializer(): SerializerInterface
    {
        return $this->serializer;
    }

    // -------------------------------------------------------------------------
    // Producer
    // -------------------------------------------------------------------------

    public function send(Envelope $envelope): Envelope
    {
        $attempt = $envelope->last(AttemptStamp::class)?->attempt ?? 0;
        $envelope = $this->withAttempt($envelope, $attempt);

        $serialized = $this->serializer->encode($envelope);
        $jsonData   = json_encode($serialized, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE);

        $id       = $this->generateUniqueId();
        $fileName = $this->buildReadyFilename($id, $attempt);
        $tmpName  = $fileName . '.tmp.' . getmypid() . '.' . bin2hex(random_bytes(4));

        $maxRetries = max(5, (int) $this->coreParametersHelper->get(
            'mautic.filesystem_queue_send_max_retries',
            self::SEND_MAX_RETRIES
        ));

        $lastError = null;

        for ($i = 0; $i < $maxRetries; ++$i) {
            try {
                $fp = @fopen($tmpName, 'wb');
                if ($fp === false) {
                    throw new TransportException("Cannot open temp file: $tmpName");
                }

                $written = fwrite($fp, $jsonData);
                fclose($fp);

                if ($written === false || $written !== strlen($jsonData)) {
                    @unlink($tmpName);
                    throw new TransportException("Incomplete write: $tmpName");
                }

                if (!@rename($tmpName, $fileName)) {
                    @unlink($tmpName);
                    // collision – new id
                    $id       = $this->generateUniqueId();
                    $fileName = $this->buildReadyFilename($id, $attempt);
                    $tmpName  = $fileName . '.tmp.' . getmypid() . '.' . bin2hex(random_bytes(4));
                    usleep(random_int(50, 300) * 1000);
                    continue;
                }

                return $envelope->with(new TransportMessageIdStamp($id));
            } catch (\Throwable $e) {
                $lastError = $e;
                @unlink($tmpName);
                usleep((100 + $i * 50) * 1000);
            }
        }

        throw new TransportException(
            sprintf('Filesystem send failed after %d attempts: %s', $maxRetries, $lastError?->getMessage() ?? 'unknown'),
            0,
            $lastError
        );
    }

    // -------------------------------------------------------------------------
    // Consumer – claim
    // -------------------------------------------------------------------------

    public function get(): iterable
    {
        $this->maybeRecoverStuck();

        $batchSize   = max(1, (int) $this->coreParametersHelper->get('mautic.filesystem_queue_batch_size', 1));
        $autoShuffle = (bool) $this->coreParametersHelper->get('mautic.filesystem_queue_batch_auto_shuffle', true);

        $ids = iterator_to_array($this->listReadyIds());
        if ($ids === []) {
            return [];
        }

        if ($autoShuffle) {
            shuffle($ids);
        }

        $envelopes = [];
        foreach ($ids as $id) {
            if (count($envelopes) >= $batchSize) {
                break;
            }
            $envelope = $this->tryClaim($id);
            if ($envelope !== null) {
                $envelopes[] = $envelope;
            }
        }

        return $envelopes;
    }

    /**
     * Atomic claim of one ready message.
     */
    public function tryClaim(string $id): ?Envelope
    {
        $readyFiles = glob($this->directory . '/' . $id . '.message*') ?: [];
        // Ignore temp files
        $readyFiles = array_values(array_filter(
            $readyFiles,
            static fn (string $f) => !str_contains(basename($f), '.tmp.')
        ));

        if ($readyFiles === []) {
            return null;
        }

        // Prefer highest attempt number if multiple somehow exist
        usort($readyFiles, fn ($a, $b) => $this->extractAttemptFromFilename($b) <=> $this->extractAttemptFromFilename($a));
        $readyFile = $readyFiles[0];

        // Honour back-off (future mtime)
        $mtime = filemtime($readyFile) ?: 0;
        if ($mtime > time()) {
            return null;
        }

        $attempt        = $this->extractAttemptFromFilename($readyFile);
        $processingFile = $this->buildProcessingFilename($id, $attempt);

        if (!@rename($readyFile, $processingFile)) {
            return null; // lost race
        }

        try {
            $content = file_get_contents($processingFile);
            if ($content === false || $content === '') {
                @unlink($processingFile);
                return null;
            }

            $data = json_decode($content, true);
            if (!is_array($data)) {
                @unlink($processingFile);
                return null;
            }

            $envelope = $this->serializer->decode($data);
            $envelope = $this->withAttempt($envelope, $attempt);
            $envelope = $envelope->with(new TransportMessageIdStamp($id));

            return $envelope;
        } catch (\Throwable) {
            // Leave .processing for recovery
            return null;
        }
    }

    // -------------------------------------------------------------------------
    // ack / reject
    // -------------------------------------------------------------------------

    public function ack(Envelope $envelope): void
    {
        // After a temporary-failure rename the processing file is already gone → no-op.
        $this->deleteProcessingFiles($envelope);
    }

    public function reject(Envelope $envelope): void
    {
        // Final failure only – delete so Messenger can move the envelope to failed transport.
        $this->deleteProcessingFiles($envelope);
    }

    // -------------------------------------------------------------------------
    // Temporary re-queue (called from middleware – NEVER from reject)
    // -------------------------------------------------------------------------

    /**
     * Rename current processing file back to a ready file with incremented attempt.
     * No new envelope is created → no duplication.
     */
    public function requeueWithRetry(Envelope $envelope, int $nextAttempt): void
    {
        $id = $this->getIdFromEnvelope($envelope);
        if ($id === null) {
            return;
        }

        $processingFiles = glob($this->directory . '/' . $id . '.processing*') ?: [];
        if ($processingFiles === []) {
            return;
        }
        $processingFile = $processingFiles[0];

        $max         = $this->getMaxAttempts();
        $nextAttempt = min(max(1, $nextAttempt), $max);

        $readyFile = $this->buildReadyFilename($id, $nextAttempt);

        // Never create a second ready file for the same id
        $existingReady = glob($this->directory . '/' . $id . '.message*') ?: [];
        $existingReady = array_values(array_filter(
            $existingReady,
            static fn (string $f) => !str_contains(basename($f), '.tmp.')
        ));
        if ($existingReady !== []) {
            @unlink($processingFile);
            return;
        }

        if (!@rename($processingFile, $readyFile)) {
            @unlink($processingFile);
            return;
        }

        $this->applyBackoff($readyFile, $nextAttempt);
    }

    // -------------------------------------------------------------------------
    // Recovery
    // -------------------------------------------------------------------------

    public function recoverStuckProcessingFiles(): void
    {
        $timeout = $this->getRecoveryTimeout();
        $now     = time();
        $dir     = $this->directory;

        $files = array_merge(
            glob($dir . '/*.processing') ?: [],
            glob($dir . '/*.processing.retry.*') ?: []
        );

        foreach ($files as $file) {
            $mtime = filemtime($file) ?: 0;
            if (($now - $mtime) < $timeout) {
                continue;
            }

            $baseName = basename($file);
            $id       = preg_replace('/\.processing.*$/', '', $baseName);
            $attempt  = $this->extractAttemptFromFilename($file);

            // Already a ready version? just drop the stuck processing file
            $existingReady = glob($dir . '/' . $id . '.message*') ?: [];
            $existingReady = array_values(array_filter(
                $existingReady,
                static fn (string $f) => !str_contains(basename($f), '.tmp.')
            ));
            if ($existingReady !== []) {
                @unlink($file);
                continue;
            }

            $ready = $this->buildReadyFilename($id, $attempt);
            @rename($file, $ready);
            // Do not apply extra back-off – original failure already did
        }
    }

    private function maybeRecoverStuck(): void
    {
        $recover = (bool) $this->coreParametersHelper->get('mautic.filesystem_queue_recovery_stuck', true);
        if (!$recover) {
            return;
        }

        $interval = max(5, (int) $this->coreParametersHelper->get('mautic.filesystem_queue_recovery_interval', 30));
        $now      = time();

        if ($this->lastRecoveryTime === null || ($now - $this->lastRecoveryTime) >= $interval) {
            $this->recoverStuckProcessingFiles();
            $this->lastRecoveryTime = $now;
        }
    }

    // -------------------------------------------------------------------------
    // Keepalive / list / count
    // -------------------------------------------------------------------------

    public function keepalive(Envelope $envelope, ?int $seconds = null): void
    {
        $id = $this->getIdFromEnvelope($envelope);
        if ($id === null) {
            return;
        }
        $files = glob($this->directory . '/' . $id . '.processing*') ?: [];
        foreach ($files as $file) {
            if (!file_exists($file)) {
                continue;
            }
            // Force mtime update without touch() (some FS restrict it)
            $fp = @fopen($file, 'r+');
            if ($fp !== false) {
                @ftruncate($fp, filesize($file) ?: 0);
                fclose($fp);
            }
        }
    }

    public function all(?int $limit = null): iterable
    {
        $count = 0;
        foreach ($this->listReadyIds() as $id) {
            if ($limit !== null && $count >= $limit) {
                break;
            }
            $envelope = $this->tryClaim($id);
            if ($envelope !== null) {
                yield $envelope;
                ++$count;
            }
        }
    }

    public function find(mixed $id): ?Envelope
    {
        return $this->tryClaim((string) $id);
    }

    public function getMessageCount(): int
    {
        return iterator_count($this->listReadyIds());
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private function listReadyIds(): \Generator
    {
        if (!is_dir($this->directory)) {
            return;
        }

        $finder = Finder::create()
            ->files()
            ->in($this->directory)
            ->name('*.message*')
            ->notName('*.tmp.*');

        $seen = [];
        foreach ($finder as $fileInfo) {
            $name = $fileInfo->getFilename();
            if (!str_contains($name, self::MESSAGE_EXTENSION)) {
                continue;
            }
            $id = preg_replace('/\.message.*$/', '', $name);
            if ($id === null || $id === '' || isset($seen[$id])) {
                continue;
            }
            $seen[$id] = true;
            yield $id;
        }
    }

    private function deleteProcessingFiles(Envelope $envelope): void
    {
        $id = $this->getIdFromEnvelope($envelope);
        if ($id === null) {
            return;
        }

        $matches = glob($this->directory . '/' . $id . '.processing*') ?: [];
        foreach ($matches as $file) {
            $this->withFileRetry(
                static function () use ($file): void {
                    if (file_exists($file) && !@unlink($file)) {
                        throw new TransportException("Unlink failed: $file");
                    }
                },
                'delete-processing'
            );
        }
    }

    private function buildReadyFilename(string $id, int $attempt): string
    {
        if ($attempt > 0) {
            return $this->directory . '/' . $id . self::MESSAGE_EXTENSION . self::RETRY_SUFFIX . $attempt;
        }
        return $this->directory . '/' . $id . self::MESSAGE_EXTENSION;
    }

    private function buildProcessingFilename(string $id, int $attempt): string
    {
        if ($attempt > 0) {
            return $this->directory . '/' . $id . self::PROCESSING_EXTENSION . self::RETRY_SUFFIX . $attempt;
        }
        return $this->directory . '/' . $id . self::PROCESSING_EXTENSION;
    }

    private function extractAttemptFromFilename(string $filename): int
    {
        if (preg_match('/\.retry\.(\d+)/', basename($filename), $m)) {
            return min((int) $m[1], $this->getMaxAttempts());
        }
        return 0;
    }

    private function getAttempt(Envelope $envelope): int
    {
        $stamp = $envelope->last(AttemptStamp::class);
        return $stamp instanceof AttemptStamp ? $stamp->attempt : 0;
    }

    private function withAttempt(Envelope $envelope, int $attempt): Envelope
    {
        $envelope = $envelope->withoutAll(AttemptStamp::class);
        return $envelope->with(new AttemptStamp($attempt));
    }

    private function getIdFromEnvelope(Envelope $envelope): ?string
    {
        $stamp = $envelope->last(TransportMessageIdStamp::class);
        return $stamp instanceof TransportMessageIdStamp ? (string) $stamp->getId() : null;
    }

    private function generateUniqueId(): string
    {
        return date('YmdHis')
            . substr(str_replace('.', '', sprintf('%.6f', microtime(true))), -6)
            . '.'
            . bin2hex(random_bytes(4))
            . '.'
            . getmypid();
    }

    private function applyBackoff(string $file, int $attempt): void
    {
        $base = $this->getBackoffBase();
        if ($base <= 0) {
            return;
        }
        $delay  = min($base * (2 ** max(0, $attempt - 1)), 3600);
        $future = time() + $delay;
        @touch($file, $future);
    }

    private function getMaxAttempts(): int
    {
        return max(1, (int) $this->coreParametersHelper->get('mautic.filesystem_queue_max_attempts', 5));
    }

    private function getRecoveryTimeout(): int
    {
        return max(60, (int) $this->coreParametersHelper->get('mautic.filesystem_queue_recovery_timeout', 300));
    }

    private function getBackoffBase(): int
    {
        return max(0, (int) $this->coreParametersHelper->get('mautic.filesystem_queue_retry_backoff_base', 30));
    }

    public function withFileRetry(callable $operation, string $context = 'operation'): void
    {
        $maxRetries  = max(0, (int) $this->coreParametersHelper->get('mautic.messenger_retry_strategy_max_retries', 3));
        $baseDelayMs = (int) $this->coreParametersHelper->get('mautic.messenger_retry_strategy_delay', 1000);
        $multiplier  = (float) $this->coreParametersHelper->get('mautic.messenger_retry_strategy_multiplier', 2.0);
        $maxDelayMs  = (int) $this->coreParametersHelper->get('mautic.messenger_retry_strategy_max_delay', 0);

        $delayMs = $baseDelayMs;
        $retries = 0;

        while (true) {
            try {
                $operation();
                return;
            } catch (\Throwable $e) {
                if (!$this->isRetryableFileError($e) || ++$retries > $maxRetries) {
                    throw new TransportException("File error in $context: " . $e->getMessage(), 0, $e);
                }
                $current = $maxDelayMs > 0 ? min($delayMs, $maxDelayMs) : $delayMs;
                usleep(($current + random_int(-50, 50)) * 1000);
                $delayMs = (int) ($delayMs * $multiplier);
            }
        }
    }

    private function isRetryableFileError(\Throwable $e): bool
    {
        $msg = $e->getMessage();
        return str_contains($msg, 'Permission denied')
            || str_contains($msg, 'Resource temporarily unavailable')
            || str_contains($msg, 'Device or resource busy')
            || str_contains($msg, 'No space left')
            || str_contains($msg, 'Input/output error');
    }
}
