<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Attribute;

use Attribute;

/**
 * Marks an event class for cross-node propagation via the SQLite Ledger + NATS.
 *
 * Events tagged with #[Propagated] are written to the local ledger on dispatch
 * and published to NATS JetStream so that other nodes can consume them.
 *
 * Events WITHOUT this attribute stay local (sync listeners, Swoole defer, or
 * queued listeners on the same node only).
 *
 * Usage:
 *
 *   #[AsEvent]
 *   #[Propagated]
 *   final class StockAdjusted { ... }
 *
 * Combine with #[OwnedAggregate] to add ownership gate enforcement.
 */
#[Attribute(Attribute::TARGET_CLASS)]
final class Propagated
{
    public function __construct(
        /**
         * NATS subject domain segment. Defaults to the lowercased PHP namespace
         * segment immediately before the event class name (i.e. the module name).
         * Override when the auto-derived value does not match your subject layout.
         *
         * Example: domain: 'inventory'
         */
        public readonly ?string $domain = null,
    ) {}
}
