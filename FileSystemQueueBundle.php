<?php

declare(strict_types=1);

namespace MauticPlugin\FileSystemQueueBundle;

use Symfony\Component\DependencyInjection\Extension\ExtensionInterface;
use Symfony\Component\HttpKernel\Bundle\Bundle;

class FileSystemQueueBundle extends Bundle
{
    public function getPath(): string
    {
        return \dirname(__DIR__);
    }
}