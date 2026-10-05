<?php

declare(strict_types=1);

namespace MauticPlugin\FileSystemQueueBundle\Tests\Concurrency;

use PHPUnit\Framework\TestCase;

/**
 * Multi-process race-condition tests.
 *
 * Spawns real PHP CLI worker processes that share one queue directory.
 * Verifies atomic rename-based claiming under contention:
 *   - each message is claimed by exactly one worker
 *   - no duplicates, no lost messages
 *   - concurrent sends produce unique files
 *   - requeue + concurrent claim does not double-process
 */
final class ConcurrentTransportTest extends TestCase
{
    private string $queueDir;
    private string $resultDir;
    private string $projectRoot;
    private string $phpBinary;

    protected function setUp(): void
    {
        $this->projectRoot = dirname(__DIR__, 2);
        $this->phpBinary   = PHP_BINARY;

        $base = sys_get_temp_dir() . '/fsq_concurrent_' . uniqid('', true);
        $this->queueDir  = $base . '/queue';
        $this->resultDir = $base . '/results';
        mkdir($this->queueDir, 0777, true);
        mkdir($this->resultDir, 0777, true);
    }

    protected function tearDown(): void
    {
        $this->removeDirectory(dirname($this->queueDir));
    }

    /**
     * N parallel workers claim from a shared pool of M messages.
     * Assert: total unique claimed IDs === M (no duplicates, no losses).
     */
    public function testConcurrentClaimNoDuplicates(): void
    {
        $messageCount = 40;
        $workerCount  = 8;

        // Seed messages via a single process first (deterministic)
        $this->runWorkerScript('seed', [
            'queue_dir' => $this->queueDir,
            'count'     => $messageCount,
        ]);

        self::assertSame(
            $messageCount,
            $this->countFiles($this->queueDir, '*.message'),
            'Seed should create exactly the requested number of ready messages'
        );

        // Launch workers in parallel
        $processes = [];
        for ($i = 0; $i < $workerCount; ++$i) {
            $resultFile = $this->resultDir . '/worker_' . $i . '.json';
            $processes[] = $this->startWorkerScript('claim', [
                'queue_dir'   => $this->queueDir,
                'result_file' => $resultFile,
                'worker_id'   => $i,
                'max_claim'   => $messageCount, // each may try to claim all
            ]);
        }

        $this->waitForProcesses($processes);

        // Collect claimed IDs from all workers
        $allClaimed = [];
        for ($i = 0; $i < $workerCount; ++$i) {
            $resultFile = $this->resultDir . '/worker_' . $i . '.json';
            self::assertFileExists($resultFile, "Worker $i result missing");
            $data = json_decode((string) file_get_contents($resultFile), true);
            self::assertIsArray($data);
            self::assertArrayHasKey('claimed_ids', $data);
            foreach ($data['claimed_ids'] as $id) {
                $allClaimed[] = $id;
            }
        }

        // No duplicates
        $unique = array_unique($allClaimed);
        self::assertCount(
            count($allClaimed),
            $unique,
            'Duplicate claims detected: ' . json_encode(array_diff_key($allClaimed, $unique))
        );

        // All messages claimed exactly once
        self::assertCount(
            $messageCount,
            $unique,
            sprintf('Expected %d unique claims, got %d', $messageCount, count($unique))
        );

        // No ready messages left
        self::assertSame(0, $this->countFiles($this->queueDir, '*.message'));
        // All should be in processing (workers did not ack)
        self::assertSame($messageCount, $this->countFiles($this->queueDir, '*.processing*'));
    }

    /**
     * Concurrent send from multiple processes → all files unique, none lost.
     */
    public function testConcurrentSendUniqueIds(): void
    {
        $workerCount    = 6;
        $perWorkerSend  = 15;
        $expectedTotal  = $workerCount * $perWorkerSend;

        $processes = [];
        for ($i = 0; $i < $workerCount; ++$i) {
            $resultFile = $this->resultDir . '/send_' . $i . '.json';
            $processes[] = $this->startWorkerScript('send', [
                'queue_dir'   => $this->queueDir,
                'result_file' => $resultFile,
                'count'       => $perWorkerSend,
                'worker_id'   => $i,
            ]);
        }

        $this->waitForProcesses($processes);

        $allIds = [];
        for ($i = 0; $i < $workerCount; ++$i) {
            $resultFile = $this->resultDir . '/send_' . $i . '.json';
            self::assertFileExists($resultFile);
            $data = json_decode((string) file_get_contents($resultFile), true);
            self::assertIsArray($data);
            self::assertArrayHasKey('sent_ids', $data);
            self::assertCount($perWorkerSend, $data['sent_ids'], "Worker $i send count mismatch");
            foreach ($data['sent_ids'] as $id) {
                $allIds[] = $id;
            }
        }

        $unique = array_unique($allIds);
        self::assertCount(
            $expectedTotal,
            $unique,
            'Duplicate IDs produced by concurrent send()'
        );

        self::assertSame(
            $expectedTotal,
            $this->countFiles($this->queueDir, '*.message'),
            'File count on disk does not match sent IDs'
        );
    }

    /**
     * Claim → requeue → concurrent re-claim: still exactly-once semantics.
     */
    public function testConcurrentRequeueAndReclaim(): void
    {
        $messageCount = 20;
        $workerCount  = 6;

        // Seed + claim + requeue all messages to .message.retry.1 in one process
        $this->runWorkerScript('seed_claim_requeue', [
            'queue_dir' => $this->queueDir,
            'count'     => $messageCount,
        ]);

        self::assertSame(
            $messageCount,
            $this->countFiles($this->queueDir, '*.message.retry.1'),
            'After requeue every message should be .message.retry.1'
        );
        self::assertSame(0, $this->countFiles($this->queueDir, '*.processing*'));

        // Parallel workers reclaim
        $processes = [];
        for ($i = 0; $i < $workerCount; ++$i) {
            $resultFile = $this->resultDir . '/reclaim_' . $i . '.json';
            $processes[] = $this->startWorkerScript('claim', [
                'queue_dir'   => $this->queueDir,
                'result_file' => $resultFile,
                'worker_id'   => $i,
                'max_claim'   => $messageCount,
            ]);
        }

        $this->waitForProcesses($processes);

        $allClaimed = [];
        for ($i = 0; $i < $workerCount; ++$i) {
            $resultFile = $this->resultDir . '/reclaim_' . $i . '.json';
            $data = json_decode((string) file_get_contents($resultFile), true);
            foreach ($data['claimed_ids'] ?? [] as $id) {
                $allClaimed[] = $id;
            }
        }

        $unique = array_unique($allClaimed);
        self::assertCount(count($allClaimed), $unique, 'Duplicate reclaim detected');
        self::assertCount($messageCount, $unique, 'Lost messages after concurrent reclaim');

        // All should now be .processing.retry.1
        self::assertSame(0, $this->countFiles($this->queueDir, '*.message*'));
        self::assertSame($messageCount, $this->countFiles($this->queueDir, '*.processing.retry.1'));
    }

    /**
     * Stress: concurrent send + concurrent claim interleaved.
     * Final accounting: claimed + still-ready === total sent.
     */
    public function testInterleavedSendAndClaim(): void
    {
        $senderCount   = 4;
        $claimerCount  = 4;
        $perSender     = 10;
        $expectedSent  = $senderCount * $perSender;

        $processes = [];

        // Start senders
        for ($i = 0; $i < $senderCount; ++$i) {
            $resultFile = $this->resultDir . '/isend_' . $i . '.json';
            $processes[] = $this->startWorkerScript('send', [
                'queue_dir'   => $this->queueDir,
                'result_file' => $resultFile,
                'count'       => $perSender,
                'worker_id'   => $i,
                'delay_us'    => 500, // small jitter to interleave with claimers
            ]);
        }

        // Start claimers at the same time
        for ($i = 0; $i < $claimerCount; ++$i) {
            $resultFile = $this->resultDir . '/iclaim_' . $i . '.json';
            $processes[] = $this->startWorkerScript('claim', [
                'queue_dir'   => $this->queueDir,
                'result_file' => $resultFile,
                'worker_id'   => $i,
                'max_claim'   => $expectedSent,
                'loops'       => 30,       // keep trying while senders are still writing
                'loop_delay_us' => 20000,  // 20 ms between loops
            ]);
        }

        $this->waitForProcesses($processes, 60);

        // Collect sent IDs
        $sentIds = [];
        for ($i = 0; $i < $senderCount; ++$i) {
            $data = json_decode((string) file_get_contents($this->resultDir . '/isend_' . $i . '.json'), true);
            foreach ($data['sent_ids'] ?? [] as $id) {
                $sentIds[] = $id;
            }
        }
        self::assertCount($expectedSent, array_unique($sentIds));

        // Collect claimed IDs
        $claimedIds = [];
        for ($i = 0; $i < $claimerCount; ++$i) {
            $data = json_decode((string) file_get_contents($this->resultDir . '/iclaim_' . $i . '.json'), true);
            foreach ($data['claimed_ids'] ?? [] as $id) {
                $claimedIds[] = $id;
            }
        }

        // No duplicate claims
        self::assertCount(count($claimedIds), array_unique($claimedIds), 'Duplicate claims under interleaved load');

        // claimed + still ready + still processing (from claimers that did not ack) must cover all sent
        $stillReady      = $this->countFiles($this->queueDir, '*.message');
        $stillProcessing = $this->countFiles($this->queueDir, '*.processing*');

        // claimers leave files in .processing (no ack), so:
        // unique claimed === stillProcessing, and claimed + stillReady === expectedSent
        self::assertSame(
            count(array_unique($claimedIds)),
            $stillProcessing,
            'Processing file count should equal unique claims'
        );
        self::assertSame(
            $expectedSent,
            count(array_unique($claimedIds)) + $stillReady,
            'claimed + still-ready must equal total sent (no losses, no phantoms)'
        );
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Run a worker action synchronously and return decoded JSON result (if any).
     */
    private function runWorkerScript(string $action, array $params): ?array
    {
        $cmd = $this->buildWorkerCommand($action, $params);
        exec($cmd . ' 2>&1', $output, $exitCode);
        self::assertSame(0, $exitCode, "Worker '$action' failed: " . implode("\n", $output));

        if (isset($params['result_file']) && is_file($params['result_file'])) {
            return json_decode((string) file_get_contents($params['result_file']), true);
        }
        return null;
    }

    /**
     * Start a worker in the background; returns [resource, pipes] for later wait.
     *
     * @return array{resource, array<int, resource>}
     */
    private function startWorkerScript(string $action, array $params): array
    {
        $cmd = $this->buildWorkerCommand($action, $params);
        $descriptors = [
            0 => ['pipe', 'r'],
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ];
        $process = proc_open($cmd, $descriptors, $pipes);
        self::assertIsResource($process, "Failed to start worker for action '$action'");
        fclose($pipes[0]);
        // non-blocking reads later
        stream_set_blocking($pipes[1], false);
        stream_set_blocking($pipes[2], false);

        return [$process, $pipes];
    }

    /**
     * @param list<array{resource, array<int, resource>}> $processes
     */
    private function waitForProcesses(array $processes, int $timeoutSeconds = 30): void
    {
        $deadline = time() + $timeoutSeconds;
        $remaining = $processes;

        while ($remaining !== [] && time() < $deadline) {
            foreach ($remaining as $key => [$process, $pipes]) {
                $status = proc_get_status($process);
                if (!$status['running']) {
                    $stdout = stream_get_contents($pipes[1]);
                    $stderr = stream_get_contents($pipes[2]);
                    fclose($pipes[1]);
                    fclose($pipes[2]);
                    proc_close($process);

                    self::assertSame(
                        0,
                        $status['exitcode'],
                        "Worker exited with code {$status['exitcode']}\nSTDOUT: $stdout\nSTDERR: $stderr"
                    );
                    unset($remaining[$key]);
                }
            }
            if ($remaining !== []) {
                usleep(20_000);
            }
        }

        // Kill stragglers
        foreach ($remaining as [$process, $pipes]) {
            proc_terminate($process, 9);
            fclose($pipes[1]);
            fclose($pipes[2]);
            proc_close($process);
            self::fail('Worker process timed out');
        }
    }

    private function buildWorkerCommand(string $action, array $params): string
    {
        $workerScript = $this->projectRoot . '/tests/Concurrency/worker.php';
        $payload = base64_encode(json_encode([
            'action' => $action,
            'params' => $params,
            'autoload' => $this->projectRoot . '/vendor/autoload.php',
        ], JSON_THROW_ON_ERROR));

        return sprintf(
            '%s %s %s',
            escapeshellarg($this->phpBinary),
            escapeshellarg($workerScript),
            escapeshellarg($payload)
        );
    }

    private function countFiles(string $dir, string $pattern): int
    {
        $files = glob($dir . '/' . $pattern) ?: [];
        // Exclude .tmp. files
        $files = array_filter($files, static fn (string $f) => !str_contains(basename($f), '.tmp.'));
        return count($files);
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
            $file->isDir() ? @rmdir($file->getPathname()) : @unlink($file->getPathname());
        }
        @rmdir($dir);
    }
}
