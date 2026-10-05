<?php

declare(strict_types=1);

use MauticPlugin\FileSystemQueueBundle\Command\AdvancedEmailSendCommand;
use MauticPlugin\FileSystemQueueBundle\Command\ConsumeQueueCommand;
use MauticPlugin\FileSystemQueueBundle\Command\SupervisorEmailCommand;
use MauticPlugin\FileSystemQueueBundle\Messenger\Middleware\FilesystemRetryMiddleware;
use MauticPlugin\FileSystemQueueBundle\Messenger\Transport\FileSystemTransportFactory;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;
use Symfony\Component\DependencyInjection\Reference;

use function Symfony\Component\DependencyInjection\Loader\Configurator\service;

return static function (ContainerConfigurator $configurator): void {
    $services = $configurator->services()
        ->defaults()
        ->autowire()
        ->autoconfigure(false);

    // Transport factory
    $services->set(FileSystemTransportFactory::class)
        ->tag('messenger.transport_factory');

    // Middleware – must run around the handler
    $services->set(FilesystemRetryMiddleware::class)
        ->arg('$emailTransport', service('messenger.transport.email')->nullOnInvalid())
        ->tag('messenger.middleware', ['priority' => 100]);

    // Existing commands (keep your current registrations)
    $services->set(ConsumeQueueCommand::class)
        ->arg('$bus', new Reference('messenger.default_bus'))
        ->arg('$eventDispatcher', service('event_dispatcher')->nullOnInvalid())
        ->arg('$emailTransport', service('messenger.transport.email')->nullOnInvalid())
        ->arg('$hitTransport', service('messenger.transport.hit')->nullOnInvalid())
        ->arg('$failedTransport', service('messenger.transport.failed')->nullOnInvalid())
        ->tag('console.command');

    $services->set(AdvancedEmailSendCommand::class)
        ->arg('$bus', new Reference('messenger.default_bus'))
        ->arg('$eventDispatcher', service('event_dispatcher')->nullOnInvalid())
        ->arg('$emailTransport', service('messenger.transport.email')->nullOnInvalid())
        ->tag('console.command');

    $services->set(SupervisorEmailCommand::class)
        ->arg('$emailTransport', service('messenger.transport.email')->nullOnInvalid())
        ->tag('console.command');
};
