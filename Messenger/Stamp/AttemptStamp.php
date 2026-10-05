<?php

declare(strict_types=1);

namespace MauticPlugin\FileSystemQueueBundle\Messenger\Stamp;

use Symfony\Component\Messenger\Stamp\StampInterface;

/**
 * Carries the current attempt number inside the envelope (belt-and-suspenders with filename).
 */
final class AttemptStamp implements StampInterface
{
    public function __construct(
        public readonly int $attempt
    ) {
    }
}
