<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Attribute;

use Attribute;

/**
 * Declares that an event belongs to a specific aggregate and that aggregate
 * ownership must be enforced before the event can be written to the ledger.
 *
 * Rules:
 *  - The node that first creates an aggregate automatically owns it.
 *  - Only the owner node may emit events for that aggregate.
 *  - Non-owner nodes must send a command to the owner (via CommandBus).
 *
 * Usage:
 *
 *   #[AsEvent]
 *   #[Propagated]
 *   #[OwnedAggregate(type: 'product', idField: 'product_id')]
 *   final class ProductPriceUpdated { ... }
 *
 *   // Creation event — claim ownership on dispatch:
 *   #[OwnedAggregate(type: 'product', idField: 'product_id', creates: true)]
 *   final class ProductCreated { ... }
 */
#[Attribute(Attribute::TARGET_CLASS)]
final class OwnedAggregate
{
    public function __construct(
        /**
         * Stable string key for the aggregate type (e.g. "product", "order").
         * Must NOT be a PHP class name — it must survive refactoring.
         */
        public readonly string $type,

        /**
         * Name of the property on the event class that holds the aggregate UUID.
         */
        public readonly string $idField,

        /**
         * When true, this event creates the aggregate: LedgerWriter calls
         * claimOwnership() instead of asserting existing ownership.
         */
        public readonly bool $creates = false,
    ) {}
}
