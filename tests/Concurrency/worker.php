#!/usr/bin/env php
<?php

declare(strict_types=1);

/**
 * Standalone CLI worker used by ConcurrentTransportTest.
 *
 * Invoked as:  php worker.php <base64-json-payload>
 * Payload shape: { action, params, autoload }
 */

if ($argc < 2) {
    fwrite(STDERR, "Usage: worker.php <base64-payload>\n");
    exit(1);
}

$payload = json_decode(base64_decode($argv[1], true) ?: '', true);
if (!is_array($payload)) {
    fwrite(STDERR, "Invalid payload\n");
    exit(1);
}

$autoload = $payload['autoload'] ?? '';
if (!is_file($autoload)) {
    fwrite(STDERR, "Autoload not found: $autoload\n");
    exit(1);
}
require $autoload;

use Mautic\CoreBundle\Helper\CoreParametersHelper;
use MauticPlugin\FileSystemQueueBundle\Messenger\Transport\FileSystemTransport;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;

$action = $payload['action'] ?? '';
$params = $payload['params'] ?? [];

// Minimal parameters stub – values that keep tests deterministic
$coreParams = new class extends CoreParametersHelper {
    public function __construct() {}
    public function get(string $key, mixed $default = null): mixed
    {
        return match ($key) {
            'mautic.filesystem_queue_batch_size'          => 5,
            'mautic.filesystem_queue_batch_auto_shuffle'  => true,
            'mautic.filesystem_queue_max_attempts'        => 5,
            'mautic.filesystem_queue_recovery_timeout'    => 3600,
            'mautic.filesystem_queue_recovery_interval'   => 9999,
            'mautic.filesystem_queue_recovery_stuck'      => false,
            'mautic.filesystem_queue_retry_backoff_base'  => 0,
            'mautic.filesystem_queue_send_max_retries'    => 15,
            'mautic.messenger_retry_strategy_max_retries' => 2,
            'mautic.messenger_retry_strategy_delay'       => 10,
            'mautic.messenger_retry_strategy_multiplier'  => 1.0,
            'mautic.messenger_retry_strategy_max_delay'   => 0,
            default                                      => $default,
        };
    }
};

$queueDir = $params['queue_dir'] ?? '';
if ($queueDir === '' || !is_dir($queueDir)) {
    fwrite(STDERR, "Invalid queue_dir\n");
    exit(1);
}

$transport = new FileSystemTransport($queueDir, new PhpSerializer(), $coreParams);

try {
    match ($action) {
        'seed' => actionSeed($transport, $params),
        'send' => actionSend($transport, $params),
        'claim' => actionClaim($transport, $params),
        'seed_claim_requeue' => actionSeedClaimRequeue($transport, $params),
        default => throw new RuntimeException("Unknown action: $action"),
    };
} catch (Throwable $e) {
    fwrite(STDERR, $e->getMessage() . "\n" . $e->getTraceAsString() . "\n");
    exit(1);
}

exit(0);

// ---------------------------------------------------------------------------

function actionSeed(FileSystemTransport $transport, array $params): void
{
    $count = (int) ($params['count'] ?? 1);
    for ($i = 0; $i < $count; ++$i) {
        $transport->send(new Envelope(new stdClass()));
    }
}

function actionSend(FileSystemTransport $transport, array $params): void
{
    $count   = (int) ($params['count'] ?? 1);
    $delayUs = (int) ($params['delay_us'] ?? 0);
    $ids     = [];

    for ($i = 0; $i < $count; ++$i) {
        if ($delayUs > 0) {
            usleep($delayUs + random_int(0, $delayUs));
        }
        $envelope = $transport->send(new Envelope(new stdClass()));
        $stamp = $envelope->last(TransportMessageIdStamp::class);
        $ids[] = $stamp ? (string) $stamp->getId() : null;
    }

    writeResult($params, ['sent_ids' => $ids, 'worker_id' => $params['worker_id'] ?? null]);
}

function actionClaim(FileSystemTransport $transport, array $params): void
{
    $maxClaim    = (int) ($params['max_claim'] ?? 100);
    $loops       = (int) ($params['loops'] ?? 1);
    $loopDelayUs = (int) ($params['loop_delay_us'] ?? 0);
    $claimed     = [];

    for ($loop = 0; $loop < $loops; ++$loop) {
        if (count($claimed) >= $maxClaim) {
            break;
        }
        $envelopes = $transport->get();
        foreach ($envelopes as $envelope) {
            $stamp = $envelope->last(TransportMessageIdStamp::class);
            if ($stamp) {
                $claimed[] = (string) $stamp->getId();
            }
            // Intentionally do NOT ack – leave as .processing so the parent can count them
            if (count($claimed) >= $maxClaim) {
                break 2;
            }
        }
        if ($loopDelayUs > 0) {
            usleep($loopDelayUs);
        }
    }

    writeResult($params, [
        'claimed_ids' => $claimed,
        'worker_id'   => $params['worker_id'] ?? null,
    ]);
}

function actionSeedClaimRequeue(FileSystemTransport $transport, array $params): void
{
    $count = (int) ($params['count'] ?? 1);

    // Seed
    $ids = [];
    for ($i = 0; $i < $count; ++$i) {
        $envelope = $transport->send(new Envelope(new stdClass()));
        $stamp = $envelope->last(TransportMessageIdStamp::class);
        $ids[] = $stamp ? (string) $stamp->getId() : null;
    }

    // Claim all + requeue to attempt 1
    foreach ($ids as $id) {
        if ($id === null) {
            continue;
        }
        $claimed = $transport->tryClaim($id);
        if ($claimed !== null) {
            $transport->requeueWithRetry($claimed, 1);
        }
    }
}

function writeResult(array $params, array $data): void
{
    $file = $params['result_file'] ?? null;
    if ($file === null) {
        return;
    }
    $tmp = $file . '.tmp.' . getmypid();
    file_put_contents($tmp, json_encode($data, JSON_THROW_ON_ERROR));
    rename($tmp, $file);
}
