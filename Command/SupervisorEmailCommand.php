<?php

declare(strict_types=1);

namespace MauticPlugin\FileSystemQueueBundle\Command;

use Mautic\CoreBundle\Command\ModeratedCommand;
use Mautic\CoreBundle\Helper\CoreParametersHelper;
use Mautic\CoreBundle\Helper\PathsHelper;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Messenger\Transport\Receiver\MessageCountAwareInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Symfony\Component\Process\Process;
use Symfony\Component\HttpKernel\KernelInterface;
use Symfony\Component\Finder\Finder;

#[AsCommand(
    name: 'mautic:emails:supervisor',
    description: 'Supervisor for dynamic multi-thread email sending using the filesystem queue. Manages threads of mautic:emails:advanced-send.'
)]
class SupervisorEmailCommand extends ModeratedCommand
{
    private const MAINTENANCE_IN_PROGRESS_LOCK  = 'maintenance.in.progress.lock';
    private const MAINTENANCE_SCHEDULED_LOCK    = 'maintenance.scheduled.lock';

    private const QUEUE_EMAIL_THREAD_LOCK       = 'mautic-queue-email-thread';
    private const WAIT_FOR_ALL_THREAD_TO_FINISH = 5;

    private const QUEUE_EMAIL = 'email';

    /** @var Process[] Thread ID => Process instance (only for threads started by this supervisor instance) */
    private array $activeProcesses = [];

    private string $runDirectory;
    private string $consolePath;
    private bool $loggingEnabled;
    private ?string $customLogName;
    private string $logFilePath;

    public function __construct(
        protected PathsHelper $pathsHelper,
        protected CoreParametersHelper $coreParametersHelper,
        private readonly KernelInterface $kernel,
        private readonly ?TransportInterface $emailTransport = null,
    ) {
        parent::__construct($pathsHelper, $coreParametersHelper);

        $this->consolePath = $this->kernel->getProjectDir() . '/bin/console';
        $this->runDirectory = $this->pathsHelper->getSystemPath('cache') . '/../run';

        // Ensure run directory exists (same as ModeratedCommand)
        if (!is_dir($this->runDirectory) && !mkdir($this->runDirectory, 0777, true)) {
            throw new \RuntimeException('Could not create run directory: ' . $this->runDirectory);
        }
    }

    protected function configure(): void
    {
        parent::configure();

        $this->setHelp(<<<'EOT'
Supervisor command for dynamic thread management of mautic:emails:advanced-send.
All tuning is done via config.php (mautic.filesystem_queue_supervisor_* parameters).
The supervisor runs until the queue is empty and all threads have finished.
EOT
        );
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $this->output = $output;
        $quiet = (bool) $input->getOption('quiet');

        // Load all settings from config.php with sensible defaults
        $settings = $this->loadSupervisorSettings();

        $this->loggingEnabled = $settings['logging_enabled'];
        $this->customLogName  = $settings['custom_log_name'] ?: null;
        $this->logFilePath    = $this->pathsHelper->getSystemPath('logs') . '/' .
            ($this->customLogName ?? 'mautic_filesystem_queue_supervisor.log');

        // Do not start if email queue is in sync mode
        if ($this->isQueueInSyncMode(self::QUEUE_EMAIL)) {
            if (!$quiet) {
                $output->writeln('<info>Email queue is in sync mode - supervisor exiting.</info>');
            }
            return Command::SUCCESS;
        }

        // Supervisor lock (only one supervisor at a time)
        if (!$this->checkRunStatus($input, $output)) {
            return Command::SUCCESS;
        }

        // Clean any stale thread locks from killed processes
        $this->cleanStaleThreadLocks();

        // Check maintenance locks before starting
        if ($settings['check_maintenance_locks'] && $this->isMaintenanceActive()) {
            if (!$quiet) {
                $output->writeln('<comment>Maintenance lock detected - supervisor will not start.</comment>');
            }
            return Command::SUCCESS;
        }

        $activeCount = $this->getActiveThreadCount();
        if ($activeCount > 0 && !$quiet) {
            $output->writeln(sprintf('<info>Resuming supervision - %d threads already running.</info>', $activeCount));
        }

        $startTime = time();
        $lastCheck = 0;

        while (true) {
            $now = time();

            // Periodic logic check
            if ($now - $lastCheck >= $settings['check_interval']) {
                $lastCheck = $now;

                // Re-check maintenance - if active, wait for threads to finish and exit
                if ($settings['check_maintenance_locks'] && $this->isMaintenanceActive()) {
                    if (!$quiet) {
                        $output->writeln('<comment>Maintenance lock appeared - waiting for all threads to finish...</comment>');
                    }
                    $this->waitForAllThreadsToFinish($settings, $quiet);
                    $this->log('Supervisor exited due to maintenance lock.');
                    return Command::SUCCESS;
                }

                $this->cleanStaleThreadLocks();

                $activeCount = $this->getActiveThreadCount();
                $messageCount = $this->getMessageCount();

                // Exit condition: no messages and no running threads
                if ($messageCount === 0 && $activeCount === 0) {
                    if (!$quiet) {
                        $output->writeln('<info>Queue empty and all threads finished - supervisor exiting.</info>');
                    }
                    $this->log('Supervisor finished - queue empty.');
                    return Command::SUCCESS;
                }

                // Try to start new threads if possible
                if ($activeCount < $settings['max_threads'] && $messageCount >= $settings['min_messages_per_thread']) {
                    $this->tryStartNewThread($settings, $activeCount, $messageCount, $quiet);
                }

                // Stream output from our managed processes
                $this->streamThreadOutputs($quiet);
            }

            // Small sleep to prevent CPU spin
            usleep(100_000); // 0.1s
        }
    }

    private function loadSupervisorSettings(): array
    {
        return [
            'max_threads'                  => (int) $this->coreParametersHelper->get('mautic.filesystem_queue_supervisor_max_threads', 8),
            'initial_threads'              => (int) $this->coreParametersHelper->get('mautic.filesystem_queue_supervisor_initial_threads', 2),
            'min_messages_per_thread'      => (int) $this->coreParametersHelper->get('mautic.filesystem_queue_supervisor_min_messages_per_thread', 10),
            'time_limit'                   => (int) $this->coreParametersHelper->get('mautic.filesystem_queue_supervisor_time_limit', 300),
            'memory_limit'                 => (string) $this->coreParametersHelper->get('mautic.filesystem_queue_supervisor_memory_limit', '256M'),
            'max_server_load'              => (float) $this->coreParametersHelper->get('mautic.filesystem_queue_supervisor_max_server_load', 4.0),
            'max_memory_usage_percent'     => (int) $this->coreParametersHelper->get('mautic.filesystem_queue_supervisor_max_memory_usage_percent', 80),
            'delay_between_threads'        => (int) $this->coreParametersHelper->get('mautic.filesystem_queue_supervisor_delay_between_threads', 5),
            'max_load_increase'            => (float) $this->coreParametersHelper->get('mautic.filesystem_queue_supervisor_max_load_increase', 0.5),
            'max_mem_increase_percent'     => (int) $this->coreParametersHelper->get('mautic.filesystem_queue_supervisor_max_mem_increase_percent', 5),
            'check_interval'               => (int) $this->coreParametersHelper->get('mautic.filesystem_queue_supervisor_check_interval', 10),
            'logging_enabled'              => (bool) $this->coreParametersHelper->get('mautic.filesystem_queue_supervisor_logging_enabled', true),
            'custom_log_name'              => (string) $this->coreParametersHelper->get('mautic.filesystem_queue_supervisor_custom_log_name', ''),
            'benchmark_enabled'            => (bool) $this->coreParametersHelper->get('mautic.filesystem_queue_supervisor_benchmark_enabled', false),
            'verbosity_level'              => (int) $this->coreParametersHelper->get('mautic.filesystem_queue_supervisor_verbosity_level', 0),
            'check_maintenance_locks'      => (bool) $this->coreParametersHelper->get('mautic.filesystem_queue_supervisor_check_maintenance_locks', true),
        ];
    }

    private function isQueueInSyncMode(string $queue): bool
    {
        $dsn = $this->coreParametersHelper->get('mautic.messenger_dsn_' . $queue);
        return $dsn && str_starts_with($dsn, 'sync://');
    }

    private function getMessageCount(): int
    {
        $transport = $this->emailTransport;
        if ($transport instanceof MessageCountAwareInterface) {
            return $transport->getMessageCount();
        }

        return 0;
    }

    private function isMaintenanceActive(): bool
    {
        return file_exists($this->runDirectory . '/' . self::MAINTENANCE_IN_PROGRESS_LOCK) ||
               file_exists($this->runDirectory . '/' . self::MAINTENANCE_SCHEDULED_LOCK);
    }

    private function cleanStaleThreadLocks(): void
    {
        $lockFiles = glob($this->runDirectory . '/'.self::QUEUE_EMAIL_THREAD_LOCK.'-*.lock');
        foreach ($lockFiles as $file) {
            if (!$this->isLockFileActive($file)) {
                @unlink($file);
            }
        }
    }

    private function isLockFileActive(string $lockFile): bool
    {
        if (!is_readable($lockFile)) {
            return false;
        }
        $content = trim(file_get_contents($lockFile));
        if (!is_numeric($content)) {
            return false; // not a PID file
        }
        $pid = (int) $content;
        return $pid > 0 && posix_getpgid($pid) !== false;
    }

    private function getActiveThreadCount(): int
    {
        $this->cleanStaleThreadLocks();
        $lockFiles = glob($this->runDirectory . '/'.self::QUEUE_EMAIL_THREAD_LOCK.'-*.lock');
        return count($lockFiles);
    }

    private function tryStartNewThread(array $settings, int $activeCount, int $totalMessages, bool $quiet): void
    {
        $plannedThreads = $activeCount + 1;
        $messagesPerThread = (int) floor($totalMessages / $plannedThreads) + ($settings['min_messages_per_thread'] * $activeCount);

        // Respect max per thread from advanced command
        $messagesPerThread = min($messagesPerThread, 10000);

        $currentLoad = sys_getloadavg()[0];
        $currentMem  = $this->getMemoryUsagePercent();

        if ($currentLoad >= $settings['max_server_load'] || $currentMem >= $settings['max_memory_usage_percent']) {
            return;
        }

        $threadId = $this->getNextThreadId();

        $args = [
            $this->consolePath,
            'mautic:emails:advanced-send',
            '--no-interaction',
            '--thread', (string) $threadId,
            '--max-threads', (string) $settings['max_threads'],
            '--lock-name', sprintf(self::QUEUE_EMAIL_THREAD_LOCK.'-%d', $threadId),
            '--max-messages-per-thread', (string) $messagesPerThread,
            '--time-limit', (string) $settings['time_limit'],
            '--memory-limit', $settings['memory_limit'],
        ];

        if ($settings['benchmark_enabled']) {
            $args[] = '--benchmark';
        }

        // Add verbosity
        if($settings['verbosity_level'] > 2) {
          $args[] = '-vvv';
        } else if($settings['verbosity_level'] == 2) {
          $args[] = '-vv';
        } else if($settings['verbosity_level'] == 1) {
          $args[] = '-v';
        }

        $process = new Process($args);
        $process->setTimeout(null); 
        $process->start();

        $this->activeProcesses[$threadId] = $process;

        if (!$quiet) {
            $this->output->writeln(sprintf('<info>Started thread %d (messages/thread: %d)</info>', $threadId, $messagesPerThread));
        }
        $this->log(sprintf('Started thread %d', $threadId));

        // Wait delay and check increase
        sleep($settings['delay_between_threads']);
        $newLoad = sys_getloadavg()[0];
        $newMem  = $this->getMemoryUsagePercent();

        if (($newLoad - $currentLoad) > $settings['max_load_increase'] ||
            ($newMem - $currentMem) > $settings['max_mem_increase_percent']) {
            $this->log(sprintf('WARNING: Thread %d caused excessive resource increase', $threadId));
        }
    }

    private function getNextThreadId(): int
    {
        $activeIds = [];
        $lockFiles = glob($this->runDirectory . '/'.self::QUEUE_EMAIL_THREAD_LOCK.'-*.lock');
        foreach ($lockFiles as $file) {
            if (preg_match('/'.self::QUEUE_EMAIL_THREAD_LOCK.'-(\d+)/', $file, $m)) {
                $activeIds[] = (int) $m[1];
            }
        }
        $max = $activeIds ? max($activeIds) : 0;
        return $max + 1;
    }

    private function getMemoryUsagePercent(): int
    {
        if (!file_exists('/proc/meminfo')) {
            return 0;
        }
        $meminfo = file_get_contents('/proc/meminfo');
        preg_match('/MemTotal:\s+(\d+)/', $meminfo, $total);
        preg_match('/MemAvailable:\s+(\d+)/', $meminfo, $avail);
        if (empty($total[1])) {
            return 0;
        }
        $total = (int) $total[1];
        $avail = isset($avail[1]) ? (int) $avail[1] : 0;
        return (int) round(100 * ($total - $avail) / $total);
    }

    private function streamThreadOutputs(bool $quiet): void
    {
        foreach ($this->activeProcesses as $threadId => $process) {
            if (!$process->isRunning()) {
                unset($this->activeProcesses[$threadId]);
                continue;
            }

            $out = $process->getIncrementalOutput();
            $err = $process->getIncrementalErrorOutput();

            $buffer = $out . $err;
            if ($buffer === '') {
                continue;
            }

            $clean = $this->cleanThreadOutput($buffer);
            $prefixed = sprintf('[Thread %d] %s', $threadId, $clean);

            if ($this->loggingEnabled) {
                $this->log($prefixed);
            }
            if (!$quiet) {
                $this->output->writeln($prefixed);
            }
        }
    }

    private function cleanThreadOutput(string $buffer): string
    {
        // Remove common timestamp prefixes added by --no-interaction / Symfony formatter
        $buffer = preg_replace('/^\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\]\s+/m', '', $buffer);
        $buffer = preg_replace('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\s+/m', '', $buffer);
        return trim($buffer);
    }

    private function log(string $message): void
    {
        $line = sprintf('[%s] %s', date('Y-m-d H:i:s'), $message) . "\n";
        file_put_contents($this->logFilePath, $line, FILE_APPEND | LOCK_EX);
    }

    private function waitForAllThreadsToFinish(array $settings, bool $quiet): void
    {
        while (true) {
            $this->cleanStaleThreadLocks();
            $active = $this->getActiveThreadCount();

            $this->streamThreadOutputs($quiet);

            if ($active === 0 && count($this->activeProcesses) === 0) {
                break;
            }
            sleep(self::WAIT_FOR_ALL_THREAD_TO_FINISH);
        }
    }
}
