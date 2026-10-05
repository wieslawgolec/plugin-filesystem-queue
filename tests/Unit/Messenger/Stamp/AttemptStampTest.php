<?php

declare(strict_types=1);

namespace MauticPlugin\FileSystemQueueBundle\Tests\Unit\Messenger\Stamp;

use MauticPlugin\FileSystemQueueBundle\Messenger\Stamp\AttemptStamp;
use PHPUnit\Framework\TestCase;

final class AttemptStampTest extends TestCase
{
    public function testStoresAttempt(): void
    {
        $stamp = new AttemptStamp(3);
        self::assertSame(3, $stamp->attempt);
    }

    public function testReadonlyProperty(): void
    {
        $stamp = new AttemptStamp(0);
        self::assertSame(0, $stamp->attempt);
    }
}
