<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Attribute;

use Attribute;

/**
 * Marks a class as a replay handler for a specific event type.
 *
 * Replay handlers are invoked by LedgerReplayer when a remote event
 * arrives and needs to be applied to the local main database.
 * They must be idempotent — use the event_id as the idempotency key.
 *
 * Usage:
 *
 *   #[AsReplayHandler(domain: 'inventory', eventType: 'stock_adjusted')]
 *   final class StockAdjustedReplayHandler implements ReplayHandlerInterface
 *   {
 *       public function apply(LedgerEvent $event): void { ... }
 *   }
 *
 * If multiple versions of an event exist, register one handler per version
 * or handle all versions inside a single handler by inspecting $event->eventVersion.
 */
#[Attribute(Attribute::TARGET_CLASS)]
final class AsReplayHandler
{
    public function __construct(
        /** Business domain (matches #[Propagated] domain). */
        public readonly string $domain,

        /** Snake_case event type (matches LedgerEvent::$eventType). */
        public readonly string $eventType,

        /**
         * Optional: only invoke this handler for a specific event version.
         * Null = handle all versions.
         */
        public readonly ?int $eventVersion = null,
    ) {}
}
