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
use Symfony\Component\Messenger\Transport\TransportInterface;
use Symfony\Contracts\EventDispatcher\EventDispatcherInterface;
use MauticPlugin\FileSystemQueueBundle\Messenger\Transport\FileSystemTransport;
use MauticPlugin\FileSystemQueueBundle\Messenger\Transport\FileSystemTransportFactory;
use Symfony\Component\HttpKernel\KernelInterface;
use Symfony\Component\Serializer\SerializerInterface;
use Symfony\Component\Messenger\Serializer\PhpSerializer;

#[AsCommand(
    name: 'mautic:emails:advanced-send',
    description: 'Advanced multi-thread email sender - optimized for filesystem queue, fallback for other transports.'
)]
class AdvancedEmailSendCommand extends ModeratedCommand
{
    public const MAX_MESSAGES_PER_THREAD = 10000;

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
        $this->queueDirectory = $kernel->getProjectDir() . FileSystemTransportFactory::QUEUE_DIR;
        $this->serializer = $serializer ?? new PhpSerializer();
    }

    protected function configure(): void
    {
        $this
            ->addOption('--message-limit', null, InputOption::VALUE_OPTIONAL, 'Global max messages to process (across all threads)')
            ->addOption('--time-limit', null, InputOption::VALUE_OPTIONAL, 'Max seconds to run')
            ->addOption('--memory-limit', null, InputOption::VALUE_REQUIRED, 'Stop when memory exceeds this (e.g. 256M)')
            ->addOption('--thread', null, InputOption::VALUE_REQUIRED, 'This thread number (1-based)', '1')
            ->addOption('--max-threads', null, InputOption::VALUE_REQUIRED, 'Total threads (pagination parts)', '1')
            ->addOption('--max-messages-per-thread', null, InputOption::VALUE_REQUIRED, 'Max messages this thread should process', (string) self::MAX_MESSAGES_PER_THREAD)
            ->addOption('--benchmark', null, InputOption::VALUE_NONE, 'Show performance stats')
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

        $lockName = sprintf('mautic-queue-email-thread-%d-of-%d', $thread, $maxThreads);

        if (!$this->checkRunStatus($input, $output, $lockName)) {
            return Command::SUCCESS;
        }

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

        $maxLoad = min(self::MAX_MESSAGES_PER_THREAD, max(1, $maxPerThread));
        if ($isFsTransport) {
            $processed += $this->processFilesystemOptimized($transport, $input, $output, $globalMsgLimit, $timeLimit, $memoryLimit, $ackBatchSize, $startTime, $maxPerThread, $maxLoad);
        } else {
            $processed += $this->processStandardGetLoop($transport, $output, $globalMsgLimit, $timeLimit, $memoryLimit, $ackBatchSize, $startTime, $maxPerThread, $maxLoad);
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
        int $maxPerThread,
        int $maxLoad
    ): int {
        $processed = 0;

        $ids = $this->listMessageIds($maxLoad);
        sort($ids);

        $total     = count($ids);
        $maxThreads = (int) $input->getOption('max-threads');
        $thread     = (int) $input->getOption('thread');
        $chunkSize = (int) ceil($total / $maxThreads);
        $offset    = ($thread - 1) * $chunkSize;
        $myIds     = array_slice($ids, $offset, $chunkSize);

        if ($maxPerThread > 0) {
            $myIds = array_slice($myIds, 0, $maxPerThread);
        }

        if (empty($myIds) && !$output->isQuiet()) {
            $output->writeln('<info>No messages assigned to this thread.</info>');
        }

        foreach ($myIds as $id) {
            if ($memoryLimit && memory_get_usage(true) > $memoryLimit) break;
            if ($globalMsgLimit > 0 && $processed >= $globalMsgLimit) break;
            if ($timeLimit > 0 && (time() - $startTime) >= $timeLimit) break;

            $filename = $transport->generateFilenameById($id, FileSystemTransport::MESSAGE_EXTENSION);

            if (!$this->tryClaimFile($transport, $filename)) {
                if ($output->isVerbose()) {
                    $output->writeln("<comment>Already claimed or missing: $id</comment>");
                }
                continue;
            }

            $processingFile = $transport->getProcessingFilename($filename);

            $envelope = $this->loadEnvelopeFromFile($processingFile, $id);
            if (!$envelope) continue;

            try {
                $this->handleMessage($envelope, self::QUEUE_EMAIL, $output);
                $processed++;
            } catch (\Throwable $e) {
                if ($output->isVerbose()) {
                    $output->writeln("<error>$id failed: {$e->getMessage()}</error>");
                }
            }

            if ($this->processedSinceLastAck >= $ackBatchSize || (time() - $this->lastAckTime >= self::ACK_MAX_AGE_SECONDS)) {
                $this->ackAll($transport);
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
        int $maxPerThread,
        int $maxLoad
    ): int {
        $processed = 0;
        $count     = 0;

        while (true) {
            if ($memoryLimit && memory_get_usage(true) > $memoryLimit) break;
            if ($globalMsgLimit > 0 && $processed >= $globalMsgLimit) break;
            if ($timeLimit > 0 && (time() - $startTime) >= $timeLimit) break;
            if ($maxPerThread > 0 && $count >= $maxPerThread) break;
            if ($maxLoad > 0 && $count >= $maxLoad) break;

            $envelope = $transport->get();
            if ($envelope === null) break;

            $count++;

            try {
                $this->handleMessage($envelope, self::QUEUE_EMAIL, $output);
                $processed++;
            } catch (\Throwable $e) {
                if ($output->isVerbose()) {
                    $output->writeln('<error>Message handling failed: ' . $e->getMessage() . '</error>');
                }
                $transport->reject($envelope);
            }

            if ($this->processedSinceLastAck >= $ackBatchSize || (time() - $this->lastAckTime >= self::ACK_MAX_AGE_SECONDS)) {
                $this->ackAll($transport);
            }
        }

        return $processed;
    }

    private function listMessageIds(int $maxLoad = 0): array
    {
        if (!is_dir($this->queueDirectory)) {
            return [];
        }

        $files = scandir($this->queueDirectory);
        $ids   = [];

        $ext = FileSystemTransport::MESSAGE_EXTENSION;

        foreach ($files as $file) {
            if (str_ends_with($file, $ext)) {
                $ids[] = substr($file, 0, -strlen($ext));
            }

            if($maxLoad > 0 && count($ids) >= $maxLoad) {
                break;
            }
        }

        return $ids;
    }

    private function tryClaimFile(FileSystemTransport $transport, string $filename): bool
    {
        if (!file_exists($filename)) {
            return false;
        }

        try {
            $transport->withFileRetry(function () use ($filename) {
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

                flock($fp, LOCK_UN);
                fclose($fp);
            }, 'advanced-claim');

            return true;
        } catch (TransportException) {
            return false;
        }
    }

    private function loadEnvelopeFromFile(string $filename, string $id): ?Envelope
    {
        if (!file_exists($filename)) {
            return null;
        }

        $content = @file_get_contents($filename);
        if ($content === false) {
            return null;
        }

        try {
            $message = $this->serializer->deserialize($content, 'object', 'php');
            if ($message === false || $message === null) {
                return null;
            }

            return new Envelope($message, [new TransportMessageIdStamp($id)]);
        } catch (\Throwable) {
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

    private function handleMessage(Envelope $envelope, string $transportName, OutputInterface $output): void
    {
        $event = new \Symfony\Component\Messenger\Event\WorkerMessageReceivedEvent($envelope, $transportName);
        $this->eventDispatcher?->dispatch($event);

        if (!$event->shouldHandle()) {
            $output->writeln('<comment>Message skipped by event listener.</comment>');
            return;
        }

        $envelope = $event->getEnvelope();

        $this->bus->dispatch($envelope->with(
            new \Symfony\Component\Messenger\Stamp\ReceivedStamp($transportName)
        ));

        $this->acks[] = [$envelope, null];
        $this->processedSinceLastAck++;
    }

    private function ackAll(TransportInterface $transport): void
    {
        foreach ($this->acks as [$envelope, $exception]) {
            try {
                if ($exception) {
                    $transport->reject($envelope);
                } else {
                    $transport->ack($envelope);
                }
            } catch (\Throwable) {
                // silent fail
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
