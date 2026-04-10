<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Attribute;

use Attribute;

/**
 * Marks a DTO as an aggregate mutation command routable via CommandBus.
 *
 * Commands are sent by non-owner nodes to the aggregate's owner node,
 * which processes them and emits the resulting events. The owner node
 * executes commands locally without NATS round-trip.
 *
 * Usage:
 *
 *   #[AsAggregateCommand(aggregateType: 'product', aggregateIdField: 'product_id')]
 *   final readonly class UpdateProductPrice
 *   {
 *       public function __construct(
 *           public string $product_id,
 *           public float  $new_price,
 *           public string $currency,
 *       ) {}
 *   }
 *
 * Note: this attribute is for aggregate-level domain commands routed via NATS.
 * It is distinct from #[AsCommand] (Semitexa console commands).
 */
#[Attribute(Attribute::TARGET_CLASS)]
final class AsAggregateCommand
{
    public function __construct(
        /**
         * Aggregate type string — must match the value used in #[OwnedAggregate].
         */
        public readonly string $aggregateType,

        /**
         * Property name on this command DTO that contains the aggregate UUID.
         */
        public readonly string $aggregateIdField,
    ) {}
}
