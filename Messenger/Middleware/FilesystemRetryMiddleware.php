<?php

declare(strict_types=1);

namespace MauticPlugin\FileSystemQueueBundle\Messenger\Middleware;

use Mautic\CoreBundle\Helper\CoreParametersHelper;
use MauticPlugin\FileSystemQueueBundle\Messenger\Stamp\AttemptStamp;
use MauticPlugin\FileSystemQueueBundle\Messenger\Transport\FileSystemTransport;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;

/**
 * Owns temporary retries for the filesystem transport.
 *
 * Temporary failure → rename .processing → .message.retry.N, return normally
 *                    → Worker calls ack() → no copy to failed transport.
 * Final failure     → re-throw → Worker calls reject() → failed transport.
 */
class FilesystemRetryMiddleware implements MiddlewareInterface
{
    public function __construct(
        private CoreParametersHelper $coreParametersHelper,
        private ?FileSystemTransport $emailTransport = null,
    ) {
    }

    public function handle(Envelope $envelope, StackInterface $stack): Envelope
    {
        // Only messages that were received from a transport
        if ($envelope->last(ReceivedStamp::class) === null) {
            return $stack->next()->handle($envelope, $stack);
        }

        // Only act when we have a filesystem transport instance
        if ($this->emailTransport === null) {
            return $stack->next()->handle($envelope, $stack);
        }

        try {
            return $stack->next()->handle($envelope, $stack);
        } catch (\Throwable $e) {
            $attempt = $envelope->last(AttemptStamp::class)?->attempt ?? 0;
            $max     = max(1, (int) $this->coreParametersHelper->get(
                'mautic.filesystem_queue_max_attempts',
                5
            ));

            if ($attempt + 1 < $max) {
                // Temporary – keep the same file, force Worker to ack()
                $this->emailTransport->requeueWithRetry($envelope, $attempt + 1);
                return $envelope;
            }

            // Final – let Worker reject() and send to failed transport
            throw $e;
        }
    }
}
