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
use Symfony\Component\Process\Exception\ProcessTimedOutException;
use Symfony\Component\Process\Process;
use Symfony\Component\HttpKernel\KernelInterface;
use Symfony\Component\Process\PhpExecutableFinder;

#[AsCommand(
    name: 'mautic:emails:supervisor',
    description: 'Supervisor for dynamic multi-thread email sending using the filesystem queue. Manages threads of mautic:emails:advanced-send.'
)]
class SupervisorEmailCommand extends ModeratedCommand
{
    // Timeout constants
    private const INTERNAL_TIME_LIMIT_BUFFER_SECONDS = 300;   // extra time after internal --time-limit ; most common: 60–180
    private const MINIMUM_IDLE_TIMEOUT_SECONDS      = 180;   // minimum idle timeout (no output/error) ; most common: 120–300
    private const GRACEFUL_STOP_TIMEOUT_SECONDS     = 10;    // seconds to wait after SIGTERM before SIGKILL ; usually 5–15 s

    private const MAINTENANCE_IN_PROGRESS_LOCK  = 'maintenance.in.progress.lock';
    private const MAINTENANCE_SCHEDULED_LOCK    = 'maintenance.scheduled.lock';

    private const QUEUE_EMAIL_THREAD_LOCK       = 'sf.mautic-emails-advanced-send';
    private const WAIT_FOR_ALL_THREAD_TO_FINISH = 5;

    private const LOCK_CLEAN_TIME               = 60; 

    private const QUEUE_EMAIL = 'email';

    /** @var Process[] Thread ID => Process instance (only for threads started by this supervisor instance) */
    private array $activeProcesses = [];

    protected $runDirectory;
    private string $consolePath;
    private bool $loggingEnabled;
    private string $logFilePath;

    // Dynamic rate + enhanced settings
    private array $settings = [];
    private ?float $currentEmailsPerSecond = null;
    private string $rateCacheFile;
    private ?int $lastRateCheckTime = null;
    private ?int $lastPendingCount = null;
    private ?int $lastLockCleanTime = null;

    private string $phpBinary;

    public function __construct(
        protected PathsHelper $pathsHelper,
        protected CoreParametersHelper $coreParametersHelper,
        private readonly KernelInterface $kernel,
        private readonly ?TransportInterface $emailTransport = null,
    ) {
        parent::__construct($pathsHelper, $coreParametersHelper);

        $this->consolePath = $this->kernel->getProjectDir() . '/bin/console';
        $this->runDirectory = $this->pathsHelper->getSystemPath('cache') . '/../run';

        // Ensure run directory exists
        if (!is_dir($this->runDirectory) && !mkdir($this->runDirectory, 0777, true)) {
            throw new \RuntimeException('Could not create run directory: ' . $this->runDirectory);
        }

        // Find the actual PHP binary used to run this script
        $finder = new PhpExecutableFinder();
        $this->phpBinary = $finder->find() ?: PHP_BINARY;

        $this->rateCacheFile = $this->runDirectory . '/supervisor_emails_per_second.rate';
        $this->loadCurrentRate();
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
        $this->settings = $settings;

        $this->loggingEnabled = $settings['logging_enabled'];
        $customLogName        = $settings['custom_log_name'] ?: null;
        $this->logFilePath    = $this->pathsHelper->getSystemPath('logs') . '/' .
            ($customLogName ?? 'queue_supervisor-'.date('Y-m-d').'.log');

        // Do not start if email queue is in sync mode
        if ($this->isQueueInSyncMode()) {
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

        $lastCheck = 0;

        while (true) {
            $now = time();

            // Stream output frequently
            $this->streamThreadOutputs($quiet);

            // Periodic tasks
            if ($now - $lastCheck >= $settings['check_interval']) {
                $lastCheck = $now;

                // Re-check maintenance - if active, wait for threads to finish and exit
                if ($settings['check_maintenance_locks'] && $this->isMaintenanceActive()) {
                    if (!$quiet) {
                        $output->writeln('<comment>Maintenance lock appeared - waiting for all threads to finish...</comment>');
                    }
                    $this->waitForAllThreadsToFinish($quiet);
                    $this->log('Supervisor exited due to maintenance lock.');
                    return Command::SUCCESS;
                }

                $activeCount = $this->getActiveThreadCount();
                $messageCount = $this->getMessageCount();

                // Dynamic rate update
                if ($this->lastRateCheckTime !== null && $this->lastPendingCount !== null) {
                    $timeDelta    = $now - $this->lastRateCheckTime;
                    $pendingDelta = $this->lastPendingCount - $messageCount;

                    if ($timeDelta >= 5 && $pendingDelta > 0) {
                        $observedRate = $pendingDelta / $timeDelta;
                        $this->updateDynamicRate($observedRate);
                    }
                }
                $this->lastRateCheckTime = $now;
                $this->lastPendingCount  = $messageCount;

                // Exit condition: no messages and no running threads
                if ($messageCount === 0 && $activeCount === 0) {
                    if (!$quiet) {
                        $output->writeln('<info>Queue empty and all threads finished - supervisor exiting.</info>');
                    }
                    $this->log('Supervisor finished - queue empty.');
                    $this->cleanStaleThreadLocks(); // final clean
                    return Command::SUCCESS;
                }

                // Periodic lock cleaning (every ~60 seconds)
                if ($this->lastLockCleanTime === null || $now - $this->lastLockCleanTime >= self::LOCK_CLEAN_TIME) {
                    $this->cleanStaleThreadLocks();
                    $this->lastLockCleanTime = $now;
                }

                $desired = $this->getDesiredThreads($messageCount, $settings);
                if ($activeCount < $desired) {
                    $this->tryStartNewThread($settings, $activeCount, $messageCount, $quiet);
                }

                // Enforce supervisor-level timeouts
                $this->enforceProcessTimeouts($quiet);
            }

            usleep(200_000); // 0.2s - more responsive output polling
        }
    }

    private function loadSupervisorSettings(): array
    {
        return [
            'max_threads'                  => (int) $this->coreParametersHelper->get('mautic.filesystem_queue_supervisor_max_threads', 8),
            'initial_threads'              => (int) $this->coreParametersHelper->get('mautic.filesystem_queue_supervisor_initial_threads', 1),
            'email_limit'                  => (int) $this->coreParametersHelper->get('mautic.filesystem_queue_supervisor_email_limit', 300),
            'settle_time'                  => (int) $this->coreParametersHelper->get('mautic.filesystem_queue_supervisor_settle_time', 15),
            'emails_per_extra_thread'      => (int) $this->coreParametersHelper->get('mautic.filesystem_queue_supervisor_emails_per_extra_thread', 250),
            'emails_per_second_initial'    => (float) $this->coreParametersHelper->get('mautic.filesystem_queue_supervisor_emails_per_second_initial', 0.8),
            'rate_smoothing_factor'        => (float) $this->coreParametersHelper->get('mautic.filesystem_queue_supervisor_rate_smoothing_factor', 0.3),
            'dynamic_rate_enabled'         => (bool) $this->coreParametersHelper->get('mautic.filesystem_queue_supervisor_dynamic_rate_enabled', true),
            'max_messages_per_thread'      => (int) $this->coreParametersHelper->get('mautic.filesystem_queue_supervisor_max_messages_per_thread', 0), // 0 = no cap
            'time_limit'                   => (int) $this->coreParametersHelper->get('mautic.filesystem_queue_supervisor_time_limit', 3600),
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
            'check_maintenance_locks'      => (bool) $this->coreParametersHelper->get('mautic.filesystem_queue_supervisor_check_maintenance_locks', false),
        ];
    }

    private function isQueueInSyncMode(): bool
    {
        $dsn = $this->coreParametersHelper->get('mautic.messenger_dsn_' . self::QUEUE_EMAIL);
        return $dsn && str_starts_with($dsn, 'sync://');
    }

    private function getMessageCount(): int
    {
        if ($this->emailTransport instanceof MessageCountAwareInterface) {
            return $this->emailTransport->getMessageCount();
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
        $lockFiles = glob($this->runDirectory . '/'.self::QUEUE_EMAIL_THREAD_LOCK.'*.lock');
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
            return false;
        }
        $pid = (int) $content;
        return $pid > 0 && posix_getpgid($pid) !== false;
    }

    private function getActiveThreadCount(): int
    {
        $lockFiles = glob($this->runDirectory . '/' . self::QUEUE_EMAIL_THREAD_LOCK . '*');

        $active = 0;
        foreach ($lockFiles as $file) {
            if (preg_match('/^' . preg_quote(self::QUEUE_EMAIL_THREAD_LOCK, '/') . '(\d+)\./', basename($file), $m)) {
                if ($this->isLockFileActive($file)) {
                    $active++;
                }
            }
        }

        return $active;
    }

    private function getDesiredThreads(int $totalMessages, array $settings): int
    {
        $extra   = (int) floor($totalMessages / $settings['emails_per_extra_thread']);
        $desired = $settings['initial_threads'] + $extra;
        return min($desired, $settings['max_threads']);
    }

    private function tryStartNewThread(array $settings, int $activeCount, int $totalMessages, bool $quiet): void
    {
        $threadId = $this->getNextThreadId();

        $this->log("Assigned new thread ID: $threadId (current active: $activeCount)");

        $threadNum   = $threadId; // for calculation consistency
        $baseLimit   = $settings['email_limit'] * $settings['max_threads'];
        $decrement   = $this->getCurrentDecrement($settings);
        $calculated  = $baseLimit + ($settings['max_threads'] - $threadNum) * $decrement;

        $messagesLimit = ($totalMessages < $baseLimit)
            ? $totalMessages
            : min($calculated, $totalMessages);

        $messagesPerThread = AdvancedEmailSendCommand::MAX_MESSAGES_PER_THREAD;

        // Optional hard cap (0 = disabled)
        if ($settings['max_messages_per_thread'] > 0) {
            $messagesPerThread = min(AdvancedEmailSendCommand::MAX_MESSAGES_PER_THREAD, max($messagesLimit, $settings['max_messages_per_thread']));
        }

        $currentLoad = sys_getloadavg()[0];
        $currentMem  = $this->getMemoryUsagePercent();

        if ($currentLoad >= $settings['max_server_load'] || $currentMem >= $settings['max_memory_usage_percent']) {
            $this->log("Resources too high - skipping thread $threadId start");
            return;
        }

        $args = [
            $this->phpBinary,               // ← use the real PHP binary
            $this->consolePath,             // bin/console
            'mautic:emails:advanced-send',
            '--no-interaction',
            '--thread='                      . $threadId,
            '--max-threads='                 . $settings['max_threads'],
            '--lock-name='                   . sprintf('%d', $threadId),
            '--lock_mode='                   . ModeratedCommand::MODE_PID,
            '--message-limit='               . $messagesLimit,
            '--time-limit='                  . $settings['time_limit'],
            '--memory-limit='                . $settings['memory_limit'],
        ];

        if($messagesPerThread !== AdvancedEmailSendCommand::MAX_MESSAGES_PER_THREAD) {
            $args[] = '--max-messages-per-thread=' . $messagesPerThread;
        }

        if ($settings['benchmark_enabled']) {
            $args[] = '--benchmark';
        }

        // Add verbosity
        if ($settings['verbosity_level'] > 2) {
            $args[] = '-vvv';
        } elseif ($settings['verbosity_level'] == 2) {
            $args[] = '-vv';
        } elseif ($settings['verbosity_level'] == 1) {
            $args[] = '-v';
        }

        $process = new Process($args);
        // Supervisor-level safety timeouts
        $supervisorTimeout = $settings['time_limit'] + self::INTERNAL_TIME_LIMIT_BUFFER_SECONDS;
        $idleTimeout = max(self::MINIMUM_IDLE_TIMEOUT_SECONDS, (int)($supervisorTimeout * 0.5));

        $process->setTimeout($supervisorTimeout);
        $process->setIdleTimeout($idleTimeout);
        $process->start();

        $this->activeProcesses[$threadId] = $process;

        if (!$quiet) {
            $this->output->writeln(sprintf(
                '<info>Started thread %d (messages: %d | timeout: %ds | idle: %ds)</info>',
                $threadId, $messagesLimit, $supervisorTimeout, $idleTimeout
            ));
        }

        $this->log(sprintf(
            'Started thread %d (messages: %d | timeout: %ds | idle: %ds)',
            $threadId, $messagesLimit, $supervisorTimeout, $idleTimeout
        ));

        $this->log(sprintf('Started thread %d (messages/thread: %d)', $threadId, $messagesLimit));

        // Wait delay and check resource increase
        sleep($settings['delay_between_threads']);

        $newLoad = sys_getloadavg()[0];
        $newMem  = $this->getMemoryUsagePercent();

        if (($newLoad - $currentLoad) > $settings['max_load_increase'] ||
            ($newMem - $currentMem) > $settings['max_mem_increase_percent']) {
            $this->log(sprintf('WARNING: Thread %d caused excessive resource increase', $threadId));
        }
    }

    private function enforceProcessTimeouts(bool $quiet): void
    {
        foreach ($this->activeProcesses as $threadId => $process) {
            if (!$process->isRunning()) {
                continue;
            }

            try {
                $process->checkTimeout();
            } catch (ProcessTimedOutException $e) {
                $timeoutType = $e->isGeneralTimeout() ? 'general' : 'idle';
                $this->log(sprintf(
                    'Supervisor killing thread %d (%s timeout exceeded %d s)',
                    $threadId,
                    $timeoutType,
                    $process->getTimeout()
                ));

                if (!$quiet) {
                    $this->output->writeln(sprintf(
                        '<error>Supervisor killing thread %d (%s timeout exceeded)</error>',
                        $threadId,
                        $timeoutType
                    ));
                }

                $process->stop(self::GRACEFUL_STOP_TIMEOUT_SECONDS, SIGKILL);
                unset($this->activeProcesses[$threadId]);
            }
        }
    }

    private function getNextThreadId(): int
    {
        $lockFiles = glob($this->runDirectory . '/' . self::QUEUE_EMAIL_THREAD_LOCK . '*');

        $used = [];
        foreach ($lockFiles as $file) {
            $basename = basename($file);
            if (preg_match('/^' . preg_quote(self::QUEUE_EMAIL_THREAD_LOCK, '/') . '(\d+)\./', $basename, $m)) {
                $id = (int)$m[1];
                if ($this->isLockFileActive($file)) {
                    $used[] = $id;
                }
            }
        }

        $next = 1;
        while (in_array($next, $used, true)) {
            $next++;
        }

        return $next;
    }

    private function getMemoryUsagePercent(): int
    {
        if (!file_exists('/proc/meminfo')) {
            return 0;
        }

        $meminfo = file_get_contents('/proc/meminfo');
        if ($meminfo === false) {
            return 0;
        }

        $lines = explode("\n", $meminfo);
        $data = [];

        foreach ($lines as $line) {
            if (preg_match('/^(\w+):\s+(\d+)/', $line, $m)) {
                $data[$m[1]] = (int)$m[2];
            }
        }

        if (!isset($data['MemTotal'], $data['MemAvailable'], $data['SwapTotal'], $data['SwapFree'])) {
            return 0;
        }

        $totalPhysical = $data['MemTotal'];
        $availPhysical = $data['MemAvailable'];

        $totalSwap     = $data['SwapTotal'];
        $usedSwap      = $data['SwapTotal'] - $data['SwapFree'];

        $totalSystem   = $totalPhysical + $totalSwap;
        if ($totalSystem <= 0) {
            return 0;
        }

        $usedSystem = ($totalPhysical - $availPhysical) + $usedSwap;

        $percent = (int) round(100 * $usedSystem / $totalSystem);

        // Optional: cap at 100 in case of calculation rounding issues
        return min(100, max(0, $percent));
    }

    private function streamThreadOutputs(bool $quiet): void
    {
        foreach ($this->activeProcesses as $threadId => $process) {
            // ────────────────────────────────────────────────
            // First: check if process has terminated
            // ────────────────────────────────────────────────
            if (!$process->isRunning() && !$process->isStarted()) {
                // Process never really started or was already cleaned
                unset($this->activeProcesses[$threadId]);
                continue;
            }

            if (!$process->isRunning()) {
                $exitCode      = $process->getExitCode();
                $exitCodeText  = $process->getExitCodeText() ?? 'no description';
                $termSignal    = $process->getTermSignal();
                $signalText    = $termSignal ? " (signal $termSignal)" : '';

                $msg = sprintf(
                    'Thread %d terminated - exit code: %s, text: "%s"%s',
                    $threadId,
                    $exitCode !== null ? $exitCode : 'null',
                    $exitCodeText,
                    $signalText
                );

                if ($exitCode === 0) {
                    // Normal success
                    $this->log($msg . ' (success)');
                } elseif ($exitCode === null && $termSignal) {
                    // Killed by supervisor timeout or external signal
                    $this->log($msg . ' (terminated by signal)');
                    if (!$quiet) {
                        $this->output->writeln("<comment>$msg</comment>");
                    }
                } else {
                    // Error / abnormal exit
                    $errOutput = trim($process->getErrorOutput());
                    $this->log($msg . ' — FAILURE');
                    if ($errOutput) {
                        $this->log("stderr:\n" . $errOutput);
                    }

                    if (!$quiet) {
                        $this->output->writeln(sprintf('<error>%s</error>', $msg));
                        if ($errOutput) {
                            $this->output->writeln('<error>stderr excerpt:</error>');
                            $this->output->writeln(substr($errOutput, 0, 1000)); // avoid flooding console
                        }
                    }
                }

                unset($this->activeProcesses[$threadId]);
                continue;
            }

            // ────────────────────────────────────────────────
            // Process still running → normal output streaming
            // ────────────────────────────────────────────────
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
        $buffer = preg_replace('/^\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\]\s+/m', '', $buffer);
        $buffer = preg_replace('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\s+/m', '', $buffer);
        return trim($buffer);
    }

    private function log(string $message): void
    {
        $line = sprintf('[%s] %s', date('Y-m-d H:i:s'), $message) . "\n";
        file_put_contents($this->logFilePath, $line, FILE_APPEND | LOCK_EX);
    }

    private function waitForAllThreadsToFinish(bool $quiet): void
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

    private function loadCurrentRate(): void
    {
        if (file_exists($this->rateCacheFile)) {
            $rate = (float) trim(file_get_contents($this->rateCacheFile));
            $this->currentEmailsPerSecond = max(0.1, min(20.0, $rate));
        } else {
            $this->currentEmailsPerSecond = $this->settings['emails_per_second_initial'] ?? 0.8;
        }
    }

    private function saveCurrentRate(float $rate): void
    {
        $safeRate = max(0.1, min(20.0, $rate));
        file_put_contents($this->rateCacheFile, $safeRate, LOCK_EX);
        $this->currentEmailsPerSecond = $safeRate;
    }

    private function updateDynamicRate(float $observedRate): void
    {
        if (!$this->settings['dynamic_rate_enabled']) {
            return;
        }
        $oldRate = $this->currentEmailsPerSecond ?? 0.8;
        $weight  = $this->settings['rate_smoothing_factor'];

        $newRate = ($oldRate * (1 - $weight)) + ($observedRate * $weight);
        $this->saveCurrentRate($newRate);
    }

    private function getCurrentDecrement(array $settings): int
    {
        $rate = $this->currentEmailsPerSecond ?? ($settings['emails_per_second_initial'] ?? 0.8);
        return (int) round($rate * $settings['settle_time']);
    }
}
