<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service;

use Semitexa\Core\Discovery\ClassDiscovery;
use Semitexa\Ledger\Attribute\AsAggregateCommand;

/**
 * Holds the whitelist of command classes that may be instantiated via NATS.
 *
 * Only classes annotated with #[AsAggregateCommand] are accepted. This prevents
 * arbitrary class instantiation attacks when command_class is read from a NATS
 * message payload (VULN-002).
 */
final class CommandRegistry
{
    /** @var array<string, true> FQCNs of registered command classes */
    private array $allowed = [];

    public function __construct(ClassDiscovery $discovery)
    {
        foreach ($discovery->findClassesWithAttribute(AsAggregateCommand::class) as $class) {
            $this->allowed[$class] = true;
        }
    }

    public function isRegistered(string $commandClass): bool
    {
        return isset($this->allowed[$commandClass]);
    }
}
