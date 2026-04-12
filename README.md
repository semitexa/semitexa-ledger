# Semitexa Ledger

`semitexa/ledger` adds an append-only SQLite event ledger with NATS-based cross-node propagation.

The package is opt-in at runtime. Keeping it installed in `semitexa/ultimate` no longer forces every app to provide ledger infrastructure immediately.

## Enable It

Set these environment variables when you want the ledger to boot:

```env
LEDGER_ENABLED=1
LEDGER_NODE_ID=store-a
LEDGER_HMAC_KEY=change-me
NATS_PRIMARY_URL=nats://nats:4222
```

Optional:

```env
LEDGER_DB_PATH=/var/lib/semitexa/ledger/store-a.sqlite
LEDGER_DB_CONNECTION=default
NATS_SECONDARY_URL=nats://secondary:4222
EVENTS_DUAL_PRIMARY=nats
EVENTS_DUAL_SECONDARY=<other-transport>
```

## Propagate Events

Mark an event with `#[Propagated]` to persist it in the local ledger and publish it to other nodes.

```php
use Semitexa\Core\Attribute\AsEvent;
use Semitexa\Ledger\Attribute\Propagated;

#[AsEvent]
#[Propagated(domain: 'inventory')]
final class StockAdjusted
{
    private string $productId;
    private int $delta;

    public function getProductId(): string { return $this->productId; }
    public function getDelta(): int { return $this->delta; }
}
```

Getter/setter DTOs are supported. Ledger payload serialization uses the same getter convention as the core `PayloadSerializer`.

## Enforce Aggregate Ownership

Use `#[OwnedAggregate]` on propagated events and `#[AsAggregateCommand]` on commands that must execute on the owner node.

```php
use Semitexa\Ledger\Attribute\AsAggregateCommand;
use Semitexa\Ledger\Attribute\OwnedAggregate;
use Semitexa\Ledger\Attribute\Propagated;

#[Propagated(domain: 'inventory')]
#[OwnedAggregate(type: 'product', idField: 'product_id', creates: true)]
final class ProductCreated {}

#[AsAggregateCommand(aggregateType: 'product', aggregateIdField: 'product_id')]
final readonly class UpdateProductPrice
{
    public function __construct(
        public string $product_id,
        public float $new_price,
    ) {}
}
```

## Replay Remote Events

Register an idempotent replay handler for events that must update the local main database.

```php
use Semitexa\Ledger\Attribute\AsReplayHandler;
use Semitexa\Ledger\Contract\ReplayHandlerInterface;
use Semitexa\Ledger\Dto\LedgerEvent;

#[AsReplayHandler(domain: 'inventory', eventType: 'stock_adjusted')]
final class StockAdjustedReplayHandler implements ReplayHandlerInterface
{
    public function apply(LedgerEvent $event): void
    {
        // Update local projections idempotently using $event->eventId.
    }
}
```

## Current Example In This Repo

`packages/semitexa-demo/src/Application/Payload/Event/DemoItemCreated.php` and `DemoNotificationEvent.php` are marked with `#[Propagated(domain: 'demo')]` as the first live integration inside this workspace.
