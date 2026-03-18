<?php

declare(strict_types=1);

namespace MauticPlugin\FileSystemQueueBundle\Command;

use Mautic\CoreBundle\Command\ModeratedCommand;
use Mautic\CoreBundle\Helper\CoreParametersHelper;
use Mautic\CoreBundle\Helper\PathsHelper;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Event\WorkerMessageReceivedEvent;
use Symfony\Component\Messenger\MessageBusInterface;
use Symfony\Component\Messenger\Stamp\AckStamp;
use Symfony\Component\Messenger\Stamp\ConsumedByWorkerStamp;
use Symfony\Component\Messenger\Stamp\NoAutoAckStamp;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Symfony\Contracts\EventDispatcher\EventDispatcherInterface;

#[AsCommand(
    name: 'mautic:queue:consume',
    description: 'Consume messages from Mautic Messenger queues (email, hit, failed)',
    aliases: ['mautic:emails:send', 'mautic:emails:consume', 'mautic:hits:consume', 'mautic:failed:consume']
)]
class ConsumeQueueCommand extends ModeratedCommand
{
    private int $processedSinceLastAck = 0;
    private int $lastAckTime = 0;

    public const DEFAULT_ACK_BATCH_SIZE   = 100;
    public const MAX_ACK_BATCH_SIZE       = 1000;
    public const ACK_MAX_AGE_SECONDS      = 60;

    private array $acks = [];

    public const QUEUE_ALL     = 'all';
    public const QUEUE_EMAIL   = 'email';
    public const QUEUE_HIT     = 'hit';
    public const QUEUE_FAILED  = 'failed';

    public const ALIAS_TO_QUEUE = [
        'mautic:emails:send'     => self::QUEUE_EMAIL,
        'mautic:emails:consume'  => self::QUEUE_EMAIL,
        'mautic:hits:consume'    => self::QUEUE_HIT,
        'mautic:failed:consume'  => self::QUEUE_FAILED,
    ];

    public function __construct(
        protected PathsHelper $pathsHelper,
        protected CoreParametersHelper $coreParametersHelper,
        private readonly MessageBusInterface $bus,
        private readonly ?EventDispatcherInterface $eventDispatcher = null,
        private readonly ?TransportInterface $emailTransport = null,
        private readonly ?TransportInterface $hitTransport = null,
        private readonly ?TransportInterface $failedTransport = null,
    ) {
        parent::__construct($pathsHelper, $coreParametersHelper);
    }

    protected function configure(): void
    {
        parent::configure();

        $this
            ->addArgument(
                'queue',
                InputArgument::OPTIONAL,
                'Queue name: email | hit | failed | all (default when using generic command)'
            )
            ->addOption('--memory-limit', null, InputOption::VALUE_REQUIRED, 'Stop when memory usage exceeds this value (e.g. 256M, 1G)')
            ->addOption('--message-limit', null, InputOption::VALUE_OPTIONAL, 'Max messages per queue')
            ->addOption('--time-limit', null, InputOption::VALUE_OPTIONAL, 'Max seconds per queue')
            ->addOption('--lock-name', null, InputOption::VALUE_OPTIONAL, 'Custom lock (ignored when processing all queues)')
            ->addOption('--benchmark', null, InputOption::VALUE_NONE, 'Show performance stats')
            ->setHelp(<<<'EOT'
Processes one or all Mautic message queues.

Examples:
  <info>%command.full_name% email --benchmark</info>
  <info>%command.full_name% all --time-limit=120</info>           # process email → hit → failed
  <info>mautic:emails:consume --message-limit=50</info>           # auto selects email
  <info>mautic:failed:consume</info>                              # auto selects failed
EOT
            );
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $commandName = $input->getFirstArgument() ?? $this->getName();
        $queueArg    = $input->getArgument('queue');
        $quiet       = (bool) $input->getOption('quiet');

        // 1. Determine which queue(s) to process
        if ($queueArg !== null) {
            $queues = $queueArg === self::QUEUE_ALL ? [self::QUEUE_EMAIL, self::QUEUE_HIT, self::QUEUE_FAILED] : [$queueArg];
        } elseif (isset(self::ALIAS_TO_QUEUE[$commandName])) {
            $queues = [self::ALIAS_TO_QUEUE[$commandName]];
        } else {
            // generic name + no argument → process all
            $queues = [self::QUEUE_EMAIL, self::QUEUE_HIT, self::QUEUE_FAILED];
        }

        $benchmark = $input->hasOption('benchmark') && $input->getOption('benchmark');
        $globalMsgLimit = $input->getOption('message-limit') ?: null;
        $globalTimeLimit = $input->getOption('time-limit') ?: null;
        $memoryLimit = $this->parseMemoryLimit($input->getOption('memory-limit'));

        $totalProcessed = 0;
        $overallStart   = microtime(true);
        $overallMemory  = memory_get_usage(true);
        $skippedQueues = [];

        $ackBatchSize = (int) $this->coreParametersHelper->get(
            'mautic.filesystem_queue_batch_size',
            self::DEFAULT_ACK_BATCH_SIZE
        );
        $ackBatchSize = max(1, min(self::MAX_ACK_BATCH_SIZE, $ackBatchSize));

        foreach ($queues as $queueName) {
            if (!$quiet) {
                $output->writeln('Processing <info>'.$queueName.'</info> queue messages... ');
            }

            $transport = $this->getTransportForQueue($queueName, $output);
            if ($transport === null) {
                continue; // skip invalid – but continue with others
            }

            if ($this->isQueueInSyncMode($queueName)) {
                $skippedQueues[] = $queueName;
                if (count($queues) === 1) {
                    // Single queue mode (explicit or via alias) → exit early like original
                    $output->writeln("<info>Mautic is configured to send {$queueName} synchronously (not queued). Nothing to consume.</info>");
                    return Command::SUCCESS;
                }
                // Multi-queue mode → just skip and inform
                $output->writeln("<comment>Skipping {$queueName} queue — configured as synchronous (not queued).</comment>");
                continue;
            }

            // Per-queue lock (unless --lock-name was forced)
            $lockName = $input->getOption('lock-name') ?: "mautic-queue-{$queueName}";

            if (!$this->checkRunStatus($input, $output, $lockName)) {
                $output->writeln("<info>Queue '{$queueName}' already being processed. Skipping.</info>");
                continue;
            }

            $msgLimit  = $globalMsgLimit
                ?? $this->coreParametersHelper->get("mautic.filesystem_queue_{$queueName}_msg_limit")
                ?? 0;

            $timeLimit = $globalTimeLimit
                ?? $this->coreParametersHelper->get("mautic.filesystem_queue_{$queueName}_time_limit")
                ?? 0;

            $startTime   = time();
            $processed   = 0;
            $benchStart  = microtime(true);
            $benchMemory = memory_get_usage(true);
            $this->lastAckTime = $startTime;

            while (true) {
                if ($memoryLimit > 0 && memory_get_usage(true) > $memoryLimit) {
                    if ($output->isVerbose()) {
                        $output->writeln("<comment>Memory limit reached ({$this->formatMemory(memory_get_usage(true))}). Exiting gracefully.</comment>");
                    }
                    break;
                }

                if ($msgLimit > 0 && $processed >= $msgLimit) {
                    if ($output->isVerbose()) {
                        $output->writeln("<comment>Message limit reached ({$msgLimit}). Exiting gracefully.</comment>");
                    }
                    break;
                }
                if ($timeLimit > 0 && (time() - $startTime) >= $timeLimit) {
                    if ($output->isVerbose()) {
                        $output->writeln("<comment>Time limit reached ({$timeLimit}). Exiting gracefully.</comment>");
                    }
                    break;
                }

                $envelopes = $transport->get();
                if (empty($envelopes)) {
                    if ($output->isVerbose()) {
                        $output->writeln('<info>Queue empty.</info>');
                    }
                    break;
                }

                foreach ($envelopes as $envelope) {
                    try {
                        $this->handleMessage($envelope, $queueName, $output);
                        $processed++;
                        $totalProcessed++;
                        if ($output->isDebug()) {
                            $output->writeln("<info>[$queueName] Processed #$processed</info>");
                        }
                    } catch (\Throwable $e) {
                        if ($output->isVerbose()) {
                            $output->writeln("<error>[$queueName] Error: {$e->getMessage()}</error>");
                        }
                        $transport->reject($envelope);
                    }

                    if ($memoryLimit > 0 && memory_get_usage(true) > $memoryLimit) {
                        if ($output->isVerbose()) {
                            $output->writeln("<comment>Memory limit reached ({$this->formatMemory(memory_get_usage(true))}). Exiting gracefully.</comment>");
                        }
                        break 2;
                    }
                    if ($msgLimit > 0 && $processed >= $msgLimit) {
                        if ($output->isVerbose()) {
                            $output->writeln("<comment>Message limit reached ({$msgLimit}). Exiting gracefully.</comment>");
                        }
                        break 2;
                    }
                    if ($timeLimit > 0 && (time() - $startTime) >= $timeLimit) {
                        if ($output->isVerbose()) {
                            $output->writeln("<comment>Time limit reached ({$timeLimit}). Exiting gracefully.</comment>");
                        }
                        break 2;
                    }
                }

                // Periodically ack & release memory
                if ($this->processedSinceLastAck >= $ackBatchSize ||
                    (microtime(true) - $this->lastAckTime >= self::ACK_MAX_AGE_SECONDS)) {
                    $this->ackAll($transport);
                }
            }

            $this->ackAll($transport);

            if ($output->isVerbose()) {
                if ($benchmark) {
                    $duration = microtime(true) - $benchStart;
                    $memDelta = memory_get_usage(true) - $benchMemory;
                    $output->writeln(sprintf(
                        "<comment>[$queueName] Done: %d msgs in %.2fs (%.1f/s), Δmem: %s</comment>",
                        $processed, $duration, $processed / max(0.01, $duration), $this->formatMemory($memDelta)
                    ));
                }
            }
        }

        $this->completeRun();

        if (!empty($skippedQueues) && count($queues) > 1) {
            if ($output->isVerbose()) {
                $output->writeln(sprintf(
                    "<comment>Skipped synchronous queue(s): %s</comment>",
                    implode(', ', $skippedQueues)
                ));
            }
        }

        if (!$quiet) {
            $output->writeln(sprintf('<comment>%d</comment> message%s processed', $totalProcessed, ($totalProcessed != 1) ? 's' : ''));
        }

        if ($benchmark && count($queues) > 1) {
            $totalDuration = microtime(true) - $overallStart;
            $totalMemDelta = memory_get_usage(true) - $overallMemory;
            $output->writeln(sprintf(
                "<fg=cyan>Total across all queues: %d messages in %.2fs, Δmemory: %s</>",
                $totalProcessed, $totalDuration, $this->formatMemory($totalMemDelta)
            ));
        }

        return Command::SUCCESS;
    }

    private function isQueueInSyncMode(string $queueName): bool
    {
        $dsnParam = match ($queueName) {
            'email'  => 'mautic.messenger_dsn_email',
            'hit'    => 'mautic.messenger_dsn_hit',
            'failed' => 'mautic.messenger_dsn_failed',
            default  => null,
        };

        if ($dsnParam === null) {
            return false; // unknown → assume async / don't skip
        }

        $dsn = $this->coreParametersHelper->get($dsnParam);

        // Typical sync DSN prefix is 'sync://'
        return str_starts_with((string)$dsn, 'sync');
    }

    private function getTransportForQueue(string $queueName, OutputInterface $output): ?TransportInterface
    {
        $transport = match (strtolower($queueName)) {
            self::QUEUE_EMAIL  => $this->emailTransport,
            self::QUEUE_HIT    => $this->hitTransport,
            self::QUEUE_FAILED => $this->failedTransport,
            default            => null,
        };

        if ($transport === null) {
            $output->writeln("<error>Unknown or unavailable queue: '$queueName'. Allowed: email, hit, failed</error>");
        }

        return $transport;
    }

    private function handleMessage(Envelope $envelope, string $transportName, OutputInterface $output): void
    {
        $event = new WorkerMessageReceivedEvent($envelope, $transportName);
        $this->eventDispatcher?->dispatch($event);

        $envelope = $event->getEnvelope();

        if (!$event->shouldHandle()) {
            $output->writeln('<comment>Message skipped by event listener.</comment>');
            return;
        }

        $acked = false;

        $ackCallback = function (Envelope $envelope, ?\Throwable $e = null) use (&$acked) {
            $acked = true;
            $this->acks[] = [$envelope, $e];
        };

        try {
            $this->bus->dispatch(
                $envelope->with(
                    new ReceivedStamp($transportName),
                    new ConsumedByWorkerStamp(),
                    new AckStamp($ackCallback)
                )
            );
        } catch (\Throwable $e) {
            $output->writeln("<error>Dispatch/handling failed: {$e->getMessage()}</error>");
        }

        if (!$acked && !$envelope->last(NoAutoAckStamp::class)) {
            $this->acks[] = [$envelope, null];
        }

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
                // silent fail — don't block rest of acks
            }
        }

        $this->acks = [];
        $this->processedSinceLastAck = 0;
        $this->lastAckTime = time();
    }

    private function parseMemoryLimit(?string $limit): ?int
    {
        if (!$limit) return null;
        $limit = strtolower(trim($limit));
        $unit = substr($limit, -1);
        $value = (int) $limit;
        return match ($unit) {
            'g' => $value * 1024 * 1024 * 1024,
            'm' => $value * 1024 * 1024,
            'k' => $value * 1024,
            default => $value,
        };
    }

    private function formatMemory(int $bytes): string
    {
        return sprintf('%.2f MB', $bytes / 1024 / 1024);
    }
}
