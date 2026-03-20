<?php

declare(strict_types=1);

use MauticPlugin\FileSystemQueueBundle\Command\ConsumeQueueCommand;
use MauticPlugin\FileSystemQueueBundle\Command\SupervisorEmailCommand;
use MauticPlugin\FileSystemQueueBundle\Command\AdvancedEmailSendCommand;
use MauticPlugin\FileSystemQueueBundle\Messenger\Transport\FileSystemTransportFactory;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;
use Symfony\Component\DependencyInjection\Reference;
use function Symfony\Component\DependencyInjection\Loader\Configurator\service;

return static function (ContainerConfigurator $configurator): void {
    $services = $configurator->services()
        ->defaults()
        ->autowire()
        ->autoconfigure(false);

    $services->set(FileSystemTransportFactory::class)->tag('messenger.transport_factory');

    $services->set(ConsumeQueueCommand::class)
        ->arg('$bus', new Reference('messenger.default_bus'))
        ->arg('$eventDispatcher', service('event_dispatcher')->nullOnInvalid())
        ->arg('$emailTransport',  service('messenger.transport.email')->nullOnInvalid())
        ->arg('$hitTransport',    service('messenger.transport.hit')->nullOnInvalid())
        ->arg('$failedTransport', service('messenger.transport.failed')->nullOnInvalid())
        ->tag('console.command');

    $services->set(AdvancedEmailSendCommand::class)
        ->arg('$bus', new Reference('messenger.default_bus'))
        ->arg('$eventDispatcher', service('event_dispatcher')->nullOnInvalid())
        ->arg('$emailTransport',  service('messenger.transport.email')->nullOnInvalid())
        ->tag('console.command');    
    
    $services->set(SupervisorEmailCommand::class)
        ->arg('$emailTransport',  service('messenger.transport.email')->nullOnInvalid())
        ->tag('console.command');
};
