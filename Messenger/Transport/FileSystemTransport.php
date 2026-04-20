<?php

namespace MauticPlugin\FileSystemQueueBundle\Messenger\Transport;

use Mautic\CoreBundle\Helper\CoreParametersHelper;
use Mautic\CoreBundle\Helper\PathsHelper;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Transport\Receiver\MessageCountAwareInterface;
use Symfony\Component\Messenger\Transport\Receiver\ListableReceiverInterface;
use Symfony\Component\Messenger\Transport\Receiver\KeepaliveReceiverInterface;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;

class FileSystemTransport implements ListableReceiverInterface,TransportInterface, MessageCountAwareInterface, KeepaliveReceiverInterface
{
    public const MESSAGE_EXTENSION    = '.message';
    public const PROCESSING_EXTENSION = '.processing';

    public const SEND_MAX_RETRIES     = 10;

    private SerializerInterface $serializer;
    private string $directory;
    private ?int $lastRecoveryTime     = null;   // timestamp of last recovery run

    public function __construct(
        string $directory,
        SerializerInterface $serializer = null,
        private CoreParametersHelper $coreParametersHelper,
        protected PathsHelper $pathsHelper,
    ) {
        $this->directory   = rtrim($directory, '/');

        $this->serializer  = $serializer ?? new PhpSerializer();
    }

    public function getDirectory(): string
    {
        return $this->directory;
    }
    
    public function getSerializer(): SerializerInterface 
    {
        return $this->serializer;
    }    

    public function send(Envelope $envelope): Envelope
    {
        $serialized = $this->serializer->encode($envelope);
        $jsonData = json_encode($serialized);
        if ($jsonData === false) {
            throw new TransportException('Non-retryable file error in send: Failed to JSON-encode the envelope for filesystem queue.');
        }

        // Generate file name (datetime with microsec + random string
        // It should be unique with single attempt as it include microsecs with 6 digits
        // 2026-04-20 15:59:59.123456 => 20260420155959123456
        $id = date("YmdHis") . substr((string)microtime(), 2, 6).'.'.$this->getRandomString();
        $fileName = $this->generateFilenameById($id);

        $maxRetries = self::SEND_MAX_RETRIES;
        $i = 0;

        do {
            /* We try an exclusive creation of the file. This is an atomic operation, it avoid locking mechanism */
            $fp = @fopen($fileName, 'xb');
            if (false !== $fp) {
                if (false === fwrite($fp, $jsonData)) {
                    throw new TransportException("Non-retryable file error in send: Write failed: $fileName", 0);
                }
                fclose($fp);

                return $envelope->with(new TransportMessageIdStamp($id));
            }

            if(++$i < $maxRetries) {
                /* The file already exists, we try a longer fileName */
                $id .= $this->getRandomString(1);
                $fileName = $this->generateFilenameById($id);
            } else {
                break;
            }
        } while(true);

        throw new TransportException("Max retries ($maxRetries) exceeded in send: Write falied: $fileName", 0);
    }

    public function ack(Envelope $envelope): void
    {
        $filename = $this->getFileForEnvelope($envelope, self::PROCESSING_EXTENSION);

        if (!$filename || !file_exists($filename)) {
            return;
        }

        $this->withFileRetry(function () use ($filename) {
            if (!@unlink($filename)) {
                throw new TransportException("Unlink failed: $filename");
            }
        }, 'ack');
    }

    public function reject(Envelope $envelope): void
    {
        $filename = $this->getFileForEnvelope($envelope, self::PROCESSING_EXTENSION);

        if (!$filename || !file_exists($filename)) {
            return;
        }

        $this->withFileRetry(function () use ($filename) {
            if (!@unlink($filename)) {
                throw new TransportException("Unlink failed: $filename");
            }
        }, 'reject');
    }

    public function get(): iterable
    {
        $intervalSeconds = max(5, (int) $this->coreParametersHelper->get(
            'mautic.filesystem_queue_recovery_interval',
            30   // default: every 30 seconds
        ));

        $recoverStuck = (bool) $this->coreParametersHelper->get(
            'mautic.filesystem_queue_recovery_stuck',
            true
        );

        if($recoverStuck === true) {
            $now = time();

            if ($this->lastRecoveryTime === null || ($now - $this->lastRecoveryTime) >= $intervalSeconds) {
                $this->recoverStuckProcessingFiles();
                $this->lastRecoveryTime = $now;
            }
        }

        $batchSize = max(1, (int) $this->coreParametersHelper->get(
            'mautic.filesystem_queue_batch_size',
            1
        ));

        $autoShuffle = (bool) $this->coreParametersHelper->get('mautic.filesystem_queue_batch_auto_shuffle', true);

        $offset = $autoShuffle === false ? 0 : rand(0, max(0, $this->total() - 1));
        $files = new \LimitIterator(new \IteratorIterator($this->listMessageIdsGenerator()), $offset);

        $envelopes = [];

        foreach ($files as $filename) {
            if (count($envelopes) >= $batchSize) {
                break;
            }

            $envelope = $this->tryProcessFile($filename);

            if ($envelope !== null) {
                $envelopes[] = $envelope;
            }
        }

        return $envelopes;
    }

    /**
     * Refreshes mtime on .processing file WITHOUT using touch() and without ever creating files.
     * $seconds parameter is accepted but ignored (filesystem uses filectime + configured timeout).
     */
    public function keepalive(Envelope $envelope, ?int $seconds = null): void
    {
        $filename = $this->getFileForEnvelope($envelope, self::PROCESSING_EXTENSION);

        if (!$filename || !file_exists($filename)) {
            return;
        }

        $this->withFileRetry(static function () use ($filename): void {
            // Double-check inside retry block (protects against race)
            if (!file_exists($filename)) {
                return;
            }

            $fp = @fopen($filename, 'r+');
            if (false === $fp) {
                throw new TransportException(sprintf('Unable to open keepalive file for update "%s".', $filename));
            }

            // Minimal operation that forces mtime update without changing content
            // ftruncate + fwrite(0 bytes) or even just fclose() after open is enough on most FS
            if (false === @ftruncate($fp, 0)) {
                @fclose($fp);
                throw new TransportException(sprintf('Unable to truncate keepalive file "%s".', $filename));
            }

            if (false === @fclose($fp)) {
                throw new TransportException(sprintf('Unable to close keepalive file "%s".', $filename));
            }
        }, 'keepalive');
    }

    /**
     * Try to process one file with retry protection
     */
    public function tryProcessFile(string $filename): ?Envelope
    {
        $envelope = null;

        try {
            $this->withFileRetry(function () use ($filename, &$envelope) {
                $fp = @fopen($filename, 'r');
                if ($fp === false) {
                    throw new TransportException("Open failed: $filename");
                }

                if (!flock($fp, LOCK_EX | LOCK_NB)) {
                    throw new TransportException("Lock failed: $filename");
                }

                $processingFile = $this->getProcessingFilename($filename);

                if (!@rename($filename, $processingFile)) {
                    flock($fp, LOCK_UN);
                    fclose($fp);
                    throw new TransportException("Rename failed: $filename → $processingFile");
                }

                $content = file_get_contents($processingFile);
                if ($content === false) {
                    flock($fp, LOCK_UN);
                    fclose($fp);
                    throw new TransportException("Read failed: $processingFile");
                }

                $data = json_decode($content, true);
                if ($data === null || $data === false) {
                    flock($fp, LOCK_UN);
                    fclose($fp);
                    throw new TransportException("Invalid JSON: $processingFile");
                }

                $decoded = $this->serializer->decode($data);
                $id = basename($filename, self::MESSAGE_EXTENSION);
                $envelope = $decoded->with(new TransportMessageIdStamp($id));

                flock($fp, LOCK_UN);
                fclose($fp);
            }, 'get/process file');
        } catch (TransportException) {
            // All retryable failures end up here
            return null;
        } catch (\Throwable $e) {
            // Critical / unexpected error — re-throw to stop worker
            throw $e;
        }

        return $envelope;
    }

    /**
     * Retry helper for transient file operations, using Messenger retry strategy parameters
     */
   public function withFileRetry(callable $operation, string $context = 'operation'): void
    {
        // Read current strategy from config (exactly your parameters)
        $maxRetries   = max(0, (int) $this->coreParametersHelper->get('mautic.messenger_retry_strategy_max_retries', 3));
        $baseDelayMs  = (int) $this->coreParametersHelper->get('mautic.messenger_retry_strategy_delay', 1000);
        $multiplier   = (float) $this->coreParametersHelper->get('mautic.messenger_retry_strategy_multiplier', 2.0);
        $maxDelayMs   = (int) $this->coreParametersHelper->get('mautic.messenger_retry_strategy_max_delay', 0);

        $delayMs = $baseDelayMs;
        $retries = 0;

        retry:
        try {
            $operation();
            return;
        } catch (\Throwable $e) {
            // Only retry on errors that look transient
            if (!$this->isRetryableFileError($e)) {
                throw new TransportException("Non-retryable file error in $context: " . $e->getMessage(), 0, $e);
            }

            if (++$retries > $maxRetries) {
                throw new TransportException("Max retries ($maxRetries) exceeded in $context: " . $e->getMessage(), 0, $e);
            }

            // Exponential backoff + small random jitter
            $currentDelay = $maxDelayMs > 0 ? min($delayMs, $maxDelayMs) : $delayMs;
            $jitter = random_int(-50, 50); // ±50 ms

            usleep(($currentDelay + $jitter) * 1000);

            // Next delay = current * multiplier
            $delayMs = (int) ($delayMs * $multiplier);

            goto retry;
        }
    }

    /**
     * Decide if error is worth retrying (filesystem transient errors)
     */
    public function isRetryableFileError(\Throwable $e): bool
    {
        $msg = $e->getMessage();

        // Common transient file errors
        return str_contains($msg, 'Permission denied') ||
            str_contains($msg, 'Resource temporarily unavailable') ||
            str_contains($msg, 'Device or resource busy') ||
            str_contains($msg, 'No space left') ||
            str_contains($msg, 'Input/output error') ||
            $e instanceof \ErrorException && str_contains($msg, 'fopen') && str_contains($msg, 'failed');
    }

    public function listMessageIdsGenerator(string $extension = self::MESSAGE_EXTENSION): \Generator
    {
        $finder = Finder::create()
            ->in($this->directory)
            ->name('*' . $extension);
            //->sortByName();   // oldest first (like transport when !autoShuffle)

        foreach ($finder as $fileInfo) {
            yield $fileInfo->getBasename($extension);
        }
    }

    /**
     * Returns all ready messages (up to limit) as Envelopes with stamps.
     */
    public function all(?int $limit = null): iterable
    {
        $autoShuffle = (bool) $this->coreParametersHelper->get('mautic.filesystem_queue_batch_auto_shuffle', true);

        $offset = ($limit === null || $autoShuffle === false) ? 0 : rand(0, max(0, $this->total() - $limit - 1));
        $files = new \LimitIterator(new \IteratorIterator($this->listMessageIdsGenerator()), $offset, $limit === null ? -1 : $limit);

        $count = 0;
        foreach ($files as $filename) {
            if ($limit !== null && $count >= $limit) {
                break;
            }

            $content = @file_get_contents($filename);
            if ($content === false) {
                continue;
            }

            $data = @json_decode($content, true);
            if ($data === null || $data === false) {
                continue;
            }

            try {
                $envelope = $this->serializer->decode($data);

                $id = basename($filename, self::MESSAGE_EXTENSION);
                yield $envelope->with(new TransportMessageIdStamp($id));

                $count++;
            } catch (\Throwable $e) {
                // Skip invalid messages - do not throw
                continue;
            }
        }
    }

    /**
     * Finds and returns a single message by its ID (TransportMessageIdStamp).
     * Returns null if not found or invalid.
     */
    public function find(mixed $id): ?Envelope
    {
        if (!is_string($id)) {
            return null;
        }

        $filename = $this->generateFilenameById($id);

        if (!file_exists($filename)) {
            // Also check if it's currently processing
            $processingFile = $this->generateFilenameById($id, self::PROCESSING_EXTENSION);
            if (!file_exists($processingFile)) {
                return null;
            }
            $filename = $processingFile;
        }

        $content = @file_get_contents($filename);
        if ($content === false) {
            return null;
        }

        $data = @json_decode($content, true);
        if ($data === null || $data === false) {
            return null;
        }

        try {
            $envelope = $this->serializer->decode($data);
            return $envelope->with(new TransportMessageIdStamp($id));
        } catch (\Throwable $e) {
            return null;
        }
    }

    /**
     * Recover stuck .processing files based on retry strategy settings
     */
    public function recoverStuckProcessingFiles(): void
    {
        $timeout = $this->coreParametersHelper->get(
            'mautic.filesystem_queue_recovery_timeout',
            3600  // default 1 hour
        );

        $finder = Finder::create()
            ->in($this->directory)
            ->name([
                '*' . self::PROCESSING_EXTENSION
            ])
            ->date('before ' . $timeout . ' seconds ago');

        foreach ($finder as $fileInfo) {
            $file = $fileInfo->getRealPath();

            $lockedtime = filectime($file);
            if ((time() - $lockedtime) <= $timeout) {
                // Not old enough
                continue;
            }

            $fp = @fopen($file, 'r+');
            if (!$fp) {
                continue;
            }

            if (!flock($fp, LOCK_EX | LOCK_NB)) {
                fclose($fp);
                continue;
            }

            // Re-check time after lock
            $lockedtime = filectime($file);
            if ((time() - $lockedtime) <= $timeout) {
                flock($fp, LOCK_UN);
                fclose($fp);
                continue;
            }

            // Rename to .message
            $originalFile = str_replace(
                self::PROCESSING_EXTENSION,
                self::MESSAGE_EXTENSION,
                $file
            );

            // Retry allowed → put back to queue
            @rename($file, $originalFile);
        }
    }

    public function total(string $extension = self::MESSAGE_EXTENSION): int {
        $count = 0;
        // DirectoryIterator is much faster/lighter than Finder for just counting
        foreach (new \DirectoryIterator($this->directory) as $file) {
            if ($file->isDot() || $file->isDir()) continue;
            if (str_ends_with($file->getFilename(), $extension)) {
                $count++;
            }
        }
        return $count;
    }

    public function getMessageCount(): int
    {
        return $this->total() + $this->total(self::PROCESSING_EXTENSION);
    }

    public function getFileForEnvelope(Envelope $envelope, string $extension = self::MESSAGE_EXTENSION): ?string
    {
        $stamp = $envelope->last(TransportMessageIdStamp::class);
        if (!$stamp instanceof TransportMessageIdStamp) {
            throw new \LogicException('No TransportMessageIdStamp found on the Envelope.');
        }

        $filename = $this->generateFilenameById($stamp->getId(), $extension);
        return file_exists($filename) ? $filename : null;
    }

    public function generateFilenameById(string $id, string $extension = self::MESSAGE_EXTENSION): string
    {
        return $this->directory . '/' . $id . $extension;
    }

    public function getProcessingFilename(string $originalFilename): string
    {
        return str_replace(self::MESSAGE_EXTENSION, self::PROCESSING_EXTENSION, $originalFilename);
    }

    /**
     * FS-safe random string generator (identical to Mautic 4 Swift queue)
     */
    protected function getRandomString(int $count = 10): string
    {
        // This string MUST stay FS safe, avoid special chars
        $base = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-';
        $ret = '';
        $strlen = \strlen($base);
        for ($i = 0; $i < $count; ++$i) {
            $ret .= $base[random_int(0, $strlen - 1)];
        }

        return $ret;
    }
}
