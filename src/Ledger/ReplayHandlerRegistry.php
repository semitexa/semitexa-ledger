<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Ledger;

use Semitexa\Core\Discovery\ClassDiscovery;
use Semitexa\Ledger\Attribute\AsReplayHandler;
use Semitexa\Ledger\Contract\ReplayHandlerInterface;
use Semitexa\Ledger\Dto\LedgerEvent;

/**
 * Discovers and dispatches replay handlers for incoming remote events.
 *
 * Handlers are discovered via the #[AsReplayHandler] attribute and indexed
 * by (domain, event_type, version?). Resolution order:
 *
 *   1. Exact match: (domain, event_type, version)
 *   2. Version-agnostic: (domain, event_type, null)
 *
 * If no handler is registered the event is still written to the ledger but
 * main-DB application is skipped (no-op replay).
 */
final class ReplayHandlerRegistry
{
    /**
     * @var array<string, class-string<ReplayHandlerInterface>>
     * Key format: "{domain}.{event_type}" or "{domain}.{event_type}.{version}"
     */
    private array $handlers = [];

    private bool $built = false;

    public function __construct(
        private readonly ClassDiscovery $classDiscovery,
    ) {}

    public function ensureBuilt(): void
    {
        if ($this->built) {
            return;
        }

        $classes = $this->classDiscovery->findClassesWithAttribute(AsReplayHandler::class);

        foreach ($classes as $class) {
            $ref   = new \ReflectionClass($class);
            $attrs = $ref->getAttributes(AsReplayHandler::class);

            foreach ($attrs as $attr) {
                /** @var AsReplayHandler $meta */
                $meta = $attr->newInstance();

                $keyGeneric = "{$meta->domain}.{$meta->eventType}";
                $this->handlers[$keyGeneric] = $class;

                if ($meta->eventVersion !== null) {
                    $keyVersioned = "{$meta->domain}.{$meta->eventType}.{$meta->eventVersion}";
                    $this->handlers[$keyVersioned] = $class;
                }
            }
        }

        $this->built = true;
    }

    /**
     * Find and invoke the replay handler for the given event, if one exists.
     */
    public function apply(LedgerEvent $event, callable $resolve): void
    {
        $this->ensureBuilt();

        // Try versioned match first.
        $keyVersioned = "{$event->domain}.{$event->eventType}.{$event->eventVersion}";
        $handlerClass = $this->handlers[$keyVersioned]
            ?? $this->handlers["{$event->domain}.{$event->eventType}"]
            ?? null;

        if ($handlerClass === null) {
            // No handler registered — event is ledger-only, no DB projection.
            return;
        }

        /** @var ReplayHandlerInterface $handler */
        $handler = $resolve($handlerClass);
        $handler->apply($event);
    }

    /**
     * @return array<string, class-string<ReplayHandlerInterface>>
     */
    public function all(): array
    {
        $this->ensureBuilt();
        return $this->handlers;
    }
}
