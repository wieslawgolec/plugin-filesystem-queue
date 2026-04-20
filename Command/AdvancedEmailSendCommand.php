<?php

declare(strict_types=1);

namespace MauticPlugin\FileSystemQueueBundle\Command;

use Mautic\CoreBundle\Command\ModeratedCommand;
use Mautic\CoreBundle\Helper\CoreParametersHelper;
use Mautic\CoreBundle\Helper\PathsHelper;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\MessageBusInterface;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Contracts\EventDispatcher\EventDispatcherInterface;
use MauticPlugin\FileSystemQueueBundle\Messenger\Transport\FileSystemTransport;
use MauticPlugin\FileSystemQueueBundle\Messenger\Transport\FileSystemTransportFactory;
use Symfony\Component\HttpKernel\KernelInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Event\WorkerMessageReceivedEvent;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;
use Symfony\Component\Finder\Finder;

#[AsCommand(
    name: 'mautic:emails:advanced-send',
    description: 'Advanced multi-thread email sender - optimized for filesystem queue, fallback for other transports.'
)]
class AdvancedEmailSendCommand extends ModeratedCommand
{
    public const MAX_MESSAGES_PER_THREAD = 50000;

    public const MIN_MESSAGES_PER_THREAD = 300;
    private int $processedSinceLastAck = 0;
    private int $lastAckTime = 0;
    private array $acks = [];

    private const DEFAULT_ACK_BATCH_SIZE = 100;
    private const MAX_ACK_BATCH_SIZE     = 1000;
    private const ACK_MAX_AGE_SECONDS    = 60;

    private const QUEUE_EMAIL = 'email';

    private string $queueDirectory;
    private SerializerInterface $serializer;

    public function __construct(
        protected PathsHelper $pathsHelper,
        protected CoreParametersHelper $coreParametersHelper,
        KernelInterface $kernel,
        private readonly MessageBusInterface $bus,
        private readonly ?EventDispatcherInterface $eventDispatcher = null,
        private readonly ?TransportInterface $emailTransport = null,
        ?SerializerInterface $serializer = null,
    ) {
        parent::__construct($pathsHelper, $coreParametersHelper);
        $this->serializer = $serializer ?? new PhpSerializer();
        $this->queueDirectory = $kernel->getProjectDir() . FileSystemTransportFactory::QUEUE_DIR;
        $dsn = $this->coreParametersHelper->get('mautic.messenger_dsn_email');

        if (str_starts_with($dsn, FileSystemTransportFactory::FILESYSTEM_DSN)) {
            $path = parse_url($dsn, PHP_URL_PATH) ?? '';
            $this->queueDirectory = rtrim($this->queueDirectory . $path, '/');
        }
    }

    protected function configure(): void
    {
        parent::configure();

        $this
            ->addOption('--message-limit', null, InputOption::VALUE_OPTIONAL, 'Global max messages to process (across all threads)')
            ->addOption('--time-limit', null, InputOption::VALUE_OPTIONAL, 'Max seconds to run')
            ->addOption('--memory-limit', null, InputOption::VALUE_REQUIRED, 'Stop when memory exceeds this (e.g. 256M)')
            ->addOption('--thread', null, InputOption::VALUE_REQUIRED, 'This thread number (1-based)', '1')
            ->addOption('--max-threads', null, InputOption::VALUE_REQUIRED, 'Total threads (pagination parts)', '1')
            ->addOption('--lock-name', null, InputOption::VALUE_OPTIONAL, 'Custom lock (override auto-generated lock name')
            ->addOption('--max-messages-per-thread', null, InputOption::VALUE_REQUIRED, 'Max messages this thread should process', (string) self::MAX_MESSAGES_PER_THREAD)
            ->addOption('--benchmark', null, InputOption::VALUE_NONE, 'Show performance stats')
            ->addOption('--recover-stuck', null, InputOption::VALUE_NONE, 'Recover stuck .processing files (file transport only)')
            ->addOption('--recover-timeout', null, InputOption::VALUE_OPTIONAL, 'Sets the amount of time in seconds before attempting to resend failed messages.  Defaults to value set in config.', 1800)

            ->setHelp(<<<'EOT'
Advanced email sender with multi-threading support.

- Filesystem transport: low-memory filename listing + atomic claim + one-by-one load
- Other transports: standard repeated get() loop (similar to original consume command)
- Lock name auto-generated: mautic-queue-email-thread-X-of-Y
EOT
            );
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $quiet        = (bool) $input->getOption('quiet');
        $benchmark    = $input->hasOption('benchmark') && $input->getOption('benchmark');
        $thread       = max(1, (int) $input->getOption('thread'));
        $maxThreads   = max(1, (int) $input->getOption('max-threads'));
        $maxPerThread = max(1, (int) $input->getOption('max-messages-per-thread'));
        $recoverStuck  = (bool) $input->getOption('recover-stuck');
        $recoverTimeout  = $input->getOption('recover-timeout') !== null
            ? (int) $input->getOption('recover-timeout')
            : $this->coreParametersHelper->get('mautic.filesystem_queue_recovery_timeout', 1800);

        if ($thread > $maxThreads) {
            $output->writeln('<error>--thread cannot exceed --max-threads</error>');
            return Command::FAILURE;
        }

        $transport = $this->emailTransport;
        if (!$transport) {
            $output->writeln('<error>Email transport not available</error>');
            return Command::FAILURE;
        }

        if ($this->isQueueInSyncMode(self::QUEUE_EMAIL)) {
            if (!$quiet) {
                $output->writeln('<info>Email queue in sync mode - skipping.</info>');
            }
            return Command::SUCCESS;
        }

        // Thread based lock (unless --lock-name was forced)
        $lockName = $input->getOption('lock-name') ?: sprintf('%d-%d', $thread, $maxThreads);

        if (!$this->checkRunStatus($input, $output, $lockName)) {
            return Command::SUCCESS;
        }

        // ← Add recovery here — only the thread that acquired the lock does it
        $this->recoverStuckProcessingFiles($output, $recoverStuck, $recoverTimeout);

        $globalMsgLimit = (int) ($input->getOption('message-limit') ?: $this->coreParametersHelper->get('mautic.filesystem_queue_email_msg_limit') ?? 0);
        $timeLimit      = (int) ($input->getOption('time-limit')   ?: $this->coreParametersHelper->get('mautic.filesystem_queue_email_time_limit') ?? 0);
        $memoryLimit    = $this->parseMemoryLimit($input->getOption('memory-limit'));

        $ackBatchSize = max(1, min(
            self::MAX_ACK_BATCH_SIZE,
            (int) $this->coreParametersHelper->get('mautic.filesystem_queue_batch_size', self::DEFAULT_ACK_BATCH_SIZE)
        ));

        $startTime   = time();
        $processed   = 0;
        $benchStart  = microtime(true);
        $benchMemory = memory_get_usage(true);

        $this->lastAckTime           = $startTime;
        $this->acks                  = [];
        $this->processedSinceLastAck = 0;

        $isFsTransport = $transport instanceof FileSystemTransport;

        if (!$quiet) {
            $output->writeln(sprintf(
                '<info>Mode: %s (thread %d/%d, max per thread: %d)</info>',
                $isFsTransport ? 'Optimized filesystem (filename scan + atomic claim)' : 'Standard transport get() loop',
                $thread,
                $maxThreads,
                $maxPerThread
            ));
        }

        if ($isFsTransport) {
            $processed += $this->processFilesystemOptimized($transport, $input, $output, $globalMsgLimit, $timeLimit, $memoryLimit, $ackBatchSize, $startTime, $maxPerThread);
        } else {
            $processed += $this->processStandardGetLoop($transport, $output, $globalMsgLimit, $timeLimit, $memoryLimit, $ackBatchSize, $startTime, $maxPerThread);
        }

        $this->ackAll($transport);

        if ($benchmark && $output->isVerbose()) {
            $duration = microtime(true) - $benchStart;
            $memDelta = memory_get_usage(true) - $benchMemory;
            $output->writeln(sprintf(
                '<info>Thread %d finished: %d messages | %.2fs | %.1f msg/s | +%s mem</info>',
                $thread, $processed, $duration, $processed / max(0.01, $duration), $this->formatMemory($memDelta)
            ));
        }

        $this->completeRun();

        if (!$quiet) {
            $output->writeln(sprintf(
                '<info>Advanced send finished (thread %d/%d): %d messages processed</info>',
                $thread, $maxThreads, $processed
            ));
        }

        return Command::SUCCESS;
    }

    private function processFilesystemOptimized(
        FileSystemTransport $transport,
        InputInterface $input,
        OutputInterface $output,
        int $globalMsgLimit,
        int $timeLimit,
        ?int $memoryLimit,
        int $ackBatchSize,
        int $startTime,
        int $maxPerThread
    ): int {
        $processed = 0;

        // Get Total Number (for offset calculation)
        $total = $transport->total();

        if ($total < 1 && !$output->isQuiet()) {
            $output->writeln('<info>No messages assigned to this thread.</info>');
        }

        if($total > 0) {
            if ($total <= self::MIN_MESSAGES_PER_THREAD) {
                $offset = 0;
                $chunkSize = self::MIN_MESSAGES_PER_THREAD;
            } else {
                $maxThreads = (int)$input->getOption('max-threads');
                $thread = (int)$input->getOption('thread');
                $chunkSize = (int)ceil($total / $maxThreads);
                $offset = ($thread - 1) * $chunkSize;

                if ($chunkSize < self::MIN_MESSAGES_PER_THREAD) {
                    $chunkSize = self::MIN_MESSAGES_PER_THREAD;

                    // make sure chunk size match (if its not lower because of offset is too high)
                    if ($offset + $chunkSize > $total) {
                        $offset = $total - $chunkSize;

                        if ($offset < 0) {
                            $offset = 0;
                        }
                    }
                }

                if ($maxPerThread > 0 && $chunkSize > $maxPerThread) {
                    $chunkSize = $maxPerThread;
                }
            }

            // Stream Ids
            $generator = $transport->listMessageIdsGenerator();
            $ids = new \LimitIterator(new \IteratorIterator($generator), $offset, $chunkSize);

            foreach ($ids as $id) {
                if ($memoryLimit && memory_get_usage(true) > $memoryLimit) break;
                if ($globalMsgLimit > 0 && $processed >= $globalMsgLimit) break;
                if ($timeLimit > 0 && (time() - $startTime) >= $timeLimit) break;

                $filename = $transport->generateFilenameById($id); // .message file

                $lockedFp = $this->tryClaimFile($transport, $filename);
                if ($lockedFp === false) {
                    if ($output->isVerbose()) {
                        $output->writeln("<comment>Already claimed or missing: $id</comment>");
                    }
                    continue;
                }

                $processingFile = $transport->getProcessingFilename($filename);

                $envelope = $this->loadEnvelopeFromFile($processingFile, $id);
                if (!$envelope) {
                    // load failed → immediate cleanup + delete file
                    if (is_resource($lockedFp)) {
                        @flock($lockedFp, LOCK_UN);
                        @fclose($lockedFp);
                    }

                    // we leave processing file there. it will be recovered to try again later
                    continue;
                }

                try {
                    $processed++;
                    $this->handleMessage($envelope, $output, $lockedFp);
                } catch (\Throwable $e) {
                    if ($output->isVerbose()) {
                        $output->writeln("<error>$id failed: {$e->getMessage()}</error>");
                    }
                    // Still add to acks so reject + unlock happens in ackAll
                    $this->acks[] = [$envelope, $e, $lockedFp];
                }

                if (($processed > $ackBatchSize && $this->processedSinceLastAck > 0) ||
                    $this->processedSinceLastAck >= $ackBatchSize ||
                    (time() - $this->lastAckTime >= self::ACK_MAX_AGE_SECONDS)) {

                    $this->ackAll($transport);
                }
            }
        }

        return $processed;
    }

    private function processStandardGetLoop(
        TransportInterface $transport,
        OutputInterface $output,
        int $globalMsgLimit,
        int $timeLimit,
        ?int $memoryLimit,
        int $ackBatchSize,
        int $startTime,
        int $maxPerThread
    ): int {
        $processed = 0;

        while (true) {
            if ($memoryLimit && memory_get_usage(true) > $memoryLimit) break;
            if ($globalMsgLimit > 0 && $processed >= $globalMsgLimit) break;
            if ($timeLimit > 0 && (time() - $startTime) >= $timeLimit) break;
            if ($maxPerThread > 0 && $processed >= $maxPerThread) break;

            $envelopes = $transport->get();
            if (empty($envelopes)) {
                if ($output->isVerbose()) {
                    $output->writeln('<info>Queue empty.</info>');
                }
                break;
            }

            foreach ($envelopes as $envelope) {
                try {
                    $processed++;
                    $this->handleMessage($envelope, $output);

                    if ($output->isDebug()) {
                        $output->writeln("<info>[".self::QUEUE_EMAIL."] Processed #$processed</info>");
                    }
                } catch (\Throwable $e) {
                    if ($output->isVerbose()) {
                        $output->writeln('<error>Message handling failed: ' . $e->getMessage() . '</error>');
                    }
                    $transport->reject($envelope);
                }

                if ($memoryLimit && memory_get_usage(true) > $memoryLimit) break 2;
                if ($globalMsgLimit > 0 && $processed >= $globalMsgLimit) break 2;
                if ($timeLimit > 0 && (time() - $startTime) >= $timeLimit) break 2;
                if ($maxPerThread > 0 && $processed >= $maxPerThread) break 2;

                // Periodically ack & release memory if processed > ackBatchSize
                if($processed > $ackBatchSize && $this->processedSinceLastAck > 0) {
                    $this->ackAll($transport);
                }
            }

            // Periodically ack & release memory
            if ($this->processedSinceLastAck >= $ackBatchSize ||
                (microtime(true) - $this->lastAckTime >= self::ACK_MAX_AGE_SECONDS)) {
                $this->ackAll($transport);
            }
        }

        return $processed;
    }


    /**
     * Recover stuck .processing files — similar logic as FileSystemTransport
     */
    private function recoverStuckProcessingFiles(OutputInterface $output, bool $recoverStuck = false, int $recoverTimeout = 1800): void
    {
        if($recoverStuck === false || $recoverTimeout <= 0) {
            return; // feature disabled
        }

        $finder = Finder::create()
            ->in($this->queueDirectory)
            ->name([
                '*' . FileSystemTransport::PROCESSING_EXTENSION
            ])
            ->date('before ' . $recoverTimeout . ' seconds ago');

        $recovered = 0;
        foreach ($finder as $fileInfo) {
            $file = $fileInfo->getRealPath();

            try {
                $fp = null;

                if ($file === false || !file_exists($file)) {
                    continue;
                }

                $ctime = filectime($file);

                // Double-check age after getting ctime
                if ((time() - $ctime) <= $recoverTimeout) {
                    continue;
                }

                $fp = @fopen($file, 'r+');
                if ($fp === false) {
                    continue;
                }

                if (!flock($fp, LOCK_EX | LOCK_NB)) {
                    throw new TransportException('Failed to lock file');
                }

                // Re-check age after acquiring lock (race condition protection)
                clearstatcache(true, $file);
                $ctime = filectime($file);
                if ((time() - $ctime) <= $recoverTimeout) {
                    throw new TransportException('File not match recovery time criteria');
                }

                // Decide action
                $originalFile = str_replace(
                    FileSystemTransport::PROCESSING_EXTENSION,
                    FileSystemTransport::MESSAGE_EXTENSION,
                    $file
                );

                // Put back to queue for retry
                @rename($file, $originalFile);
                $recovered++;
            } catch(\Throwable) {
                if (is_resource($fp)) {
                    @flock($fp, LOCK_UN);
                    @fclose($fp);
                }
            }
        }

        if (!$output->isQuiet() && $recovered > 0) {
            $output->writeln('<comment>'.$recovered.' stuck .processing file'.($recovered == 1 ? '' : 's').' recovered.</comment>');
        }
    }

    private function tryClaimFile(FileSystemTransport $transport, string $filename): mixed // resource|false
    {
        if (!file_exists($filename)) {
            return false;
        }

        $fp = null;

        try {
            $transport->withFileRetry(function () use ($filename, &$fp) {
                $fp = @fopen($filename, 'r');
                if ($fp === false) {
                    throw new TransportException("Cannot open $filename");
                }

                if (!flock($fp, LOCK_EX | LOCK_NB)) {
                    throw new TransportException("Cannot lock $filename");
                }

                $processing = str_replace(
                    FileSystemTransport::MESSAGE_EXTENSION,
                    FileSystemTransport::PROCESSING_EXTENSION,
                    $filename
                );

                if (!@rename($filename, $processing)) {
                    throw new TransportException("Rename failed: $filename → $processing");
                }
                // ← IMPORTANT: DO NOT unlock or fclose here any more!
                // We keep the lock until after ack/reject.
            }, 'advanced-claim');

            return $fp; // locked file handle (still valid after rename)
        } catch (TransportException) {
            // Cleanup only on failure (fp may still point to original file)
            if (is_resource($fp)) {
                @flock($fp, LOCK_UN);
                @fclose($fp);
            }
            return false;
        }
    }

    private function loadEnvelopeFromFile(string $filename, string $id): ?Envelope
    {
        try {
            if (!file_exists($filename)) {
                return null;
            }

            $content = @file_get_contents($filename);
            if ($content === false) {
                return null;
            }

            $data = json_decode($content, true);
            if (!is_array($data)) {
                return null; // or log invalid JSON
            }

            $envelope = $this->serializer->decode($data);
            return $envelope->with(new TransportMessageIdStamp($id));
        } catch (\Throwable) {
            // log error if verbose/debug
            return null;
        }
    }

    protected function isQueueInSyncMode(string $queueName): bool
    {
        $dsnParam = match ($queueName) {
            self::QUEUE_EMAIL => 'mautic.messenger_dsn_email',
            default           => null,
        };

        if ($dsnParam === null) {
            return false;
        }

        $dsn = $this->coreParametersHelper->get($dsnParam);
        return str_starts_with((string) $dsn, 'sync');
    }

    private function handleMessage(Envelope $envelope, OutputInterface $output, mixed $fp = null): void
    {
        $event = new WorkerMessageReceivedEvent($envelope, self::QUEUE_EMAIL);
        $this->eventDispatcher?->dispatch($event);

        if (!$event->shouldHandle()) {
            $output->writeln('<comment>Message skipped by event listener.</comment>');
            // still store fp so it gets unlocked below
            $this->acks[] = [$envelope, null, $fp];
            return;
        }

        $envelope = $event->getEnvelope();

        $this->bus->dispatch($envelope->with(
            new ReceivedStamp(self::QUEUE_EMAIL)
        ));

        $this->acks[] = [$envelope, null, $fp];   // ← fp is stored with the ack
        $this->processedSinceLastAck++;
    }

    private function ackAll(TransportInterface $transport): void
    {
        foreach ($this->acks as [$envelope, $exception, $fp]) {
            try {
                if ($exception) {
                    $transport->reject($envelope);
                } else {
                    $transport->ack($envelope);
                }
            } catch (\Throwable) {
                // silent fail
            }

            // ← ALWAYS unlock and close the handle after ack/reject
            if (is_resource($fp)) {
                @flock($fp, LOCK_UN);
                @fclose($fp);
            }
        }

        $this->acks = [];
        $this->processedSinceLastAck = 0;
        $this->lastAckTime = time();
    }

    private function parseMemoryLimit(?string $limit): ?int
    {
        if (!$limit) {
            return null;
        }

        $limit = strtolower(trim($limit));
        $unit  = substr($limit, -1);
        $value = (int) $limit;

        return match ($unit) {
            'g' => $value * 1073741824,
            'm' => $value * 1048576,
            'k' => $value * 1024,
            default => $value,
        };
    }

    private function formatMemory(int $bytes): string
    {
        return number_format($bytes / 1048576, 1) . ' MB';
    }
}
