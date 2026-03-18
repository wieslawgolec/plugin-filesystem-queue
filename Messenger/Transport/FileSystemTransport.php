<?php

namespace MauticPlugin\FileSystemQueueBundle\Messenger\Transport;

use Mautic\CoreBundle\Helper\CoreParametersHelper;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Transport\Receiver\MessageCountAwareInterface;
use Symfony\Component\Messenger\Transport\Receiver\ListableReceiverInterface;
use \Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;

class FileSystemTransport implements ListableReceiverInterface,TransportInterface, MessageCountAwareInterface
{
    public const MESSAGE_EXTENSION    = '.message';
    public const PROCESSING_EXTENSION = '.processing';

    private SerializerInterface $serializer;
    private string $directory;

    public function __construct(
        string $directory,
        SerializerInterface $serializer = null,
        private CoreParametersHelper $coreParametersHelper
    ) {
        $this->directory = rtrim($directory, '/');

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

    public function send(Envelope $envelope): Envelope
    {
        $id = $this->generateUniqueId();
        $fileName = $this->generateFilenameById($id);

        $this->withFileRetry(function () use ($envelope, $fileName) {
            $serialized = $this->serializer->encode($envelope);
            $result = @file_put_contents($fileName, json_encode($serialized), LOCK_EX);
            if ($result === false) {
                throw new TransportException("Write failed: $fileName");
            }
        }, 'send');

        return $envelope->with(new TransportMessageIdStamp($id));
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
        $this->recoverStuckProcessingFiles();

        $batchSize = max(1, (int) $this->coreParametersHelper->get(
            'mautic.filesystem_queue_batch_size',
            1
        ));

        $files = glob($this->directory . '/*' . self::MESSAGE_EXTENSION);

        if ($files === false || $files === []) {
            return [];
        }

        $autoShuffle = (bool) $this->coreParametersHelper->get('mautic.filesystem_queue_batch_auto_shuffle', true);
        if($autoShuffle) {
            shuffle($files);
        } else {
            // Sort by modification time - oldest first
            usort($files, static fn($a, $b) => basename($a) <=> basename($b));
        }

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
                    throw new TransportException("Rename failed: $filename → $processingFile");
                }

                flock($fp, LOCK_UN);
                fclose($fp);

                $content = file_get_contents($processingFile);
                if ($content === false) {
                    throw new TransportException("Read failed: $processingFile");
                }

                $data = json_decode($content, true);
                if ($data === null || $data === false) {
                    throw new TransportException("Invalid JSON: $processingFile");
                }

                $decoded = $this->serializer->decode($data);
                $id = basename($filename, self::MESSAGE_EXTENSION);
                $envelope = $decoded->with(new TransportMessageIdStamp($id));
            }, 'get/process file');
        } catch (TransportException $e) {
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


    /**
     * Returns all ready messages (up to limit) as Envelopes with stamps.
     */
    public function all(?int $limit = null): iterable
    {
        $files = glob($this->directory . '/*' . self::MESSAGE_EXTENSION);

        if ($files === false || $files === []) {
            return [];
        }

        $autoShuffle = (bool) $this->coreParametersHelper->get('mautic.filesystem_queue_batch_auto_shuffle', true);
        if($autoShuffle) {
            shuffle($files);
        } else {
            // Sort by modification time - oldest first
            usort($files, static fn($a, $b) => basename($a) <=> basename($b));
        }
        
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

        $filename = $this->generateFilenameById($id, self::MESSAGE_EXTENSION);

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
        $delaySeconds = $this->coreParametersHelper->get(
            'mautic.messenger_retry_strategy_delay',
            300  // default 5 minutes if not set
        );

        $maxRetries = $this->coreParametersHelper->get(
            'mautic.messenger_retry_strategy_max_retries',
            0   // default 0 = no retry → delete
        );

        $finder = \Symfony\Component\Finder\Finder::create()
            ->in($this->directory)
            ->name('*' . self::PROCESSING_EXTENSION)
            ->date('before ' . $delaySeconds . ' seconds ago');

        foreach ($finder as $fileInfo) {
            $processingFile = $fileInfo->getRealPath();
            $originalFile   = str_replace(self::PROCESSING_EXTENSION, self::MESSAGE_EXTENSION, $processingFile);

            if ($maxRetries > 0) {
                // Retry allowed → put back to queue
                @rename($processingFile, $originalFile);
            } else {
                // No retry → delete
                @unlink($processingFile);
            }
        }
    }

    public function getMessageCount(): int
    {
        $readyFiles = glob($this->directory . '/*' . self::MESSAGE_EXTENSION);
        $processingFiles = glob($this->directory . '/*' . self::PROCESSING_EXTENSION);

        return ($readyFiles === false ? 0 : count($readyFiles))
            + ($processingFiles === false ? 0 : count($processingFiles));
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

    public function generateUniqueId(): string
    {
        do {
            $id = uniqid(date('Ymd_His_') . gettimeofday()['usec'], true);
            $fileName = $this->generateFilenameById($id);
        } while (file_exists($fileName));

        return $id;
    }

    public function generateFilenameById(string $id, string $extension = self::MESSAGE_EXTENSION): string
    {
        return $this->directory . '/' . $id . $extension;
    }

    public function getProcessingFilename(string $originalFilename): string
    {
        return str_replace(self::MESSAGE_EXTENSION, self::PROCESSING_EXTENSION, $originalFilename);
    }
}
