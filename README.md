# Semitexa Ledger

`semitexa/ledger` adds an append-only SQLite event ledger with NATS-based cross-node propagation.

The package is opt-in at runtime. Keeping it installed in `semitexa/ultimate` no longer forces every app to provide ledger infrastructure immediately.

## Install

Included in every project created by the installer (https://semitexa.com/install.sh).

## Enable It

Set these environment variables when you want the ledger to boot. `EVENTS_ASYNC=1` is required: it turns on async event dispatch and makes `bin/semitexa server:start` add the `docker-compose.nats.yml` overlay, so the NATS server the ledger publishes to actually runs (the default is `EVENTS_ASYNC=0`). Restart the server after changing `.env`.

```env
EVENTS_ASYNC=1
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

`EVENTS_TRANSPORT=dual` publishes to both transports and consumes from the primary. It holds a retry back when the primary can (NATS, database); a secondary that cannot gets the message at once.

## What The Signature Covers

Every event carries a hash that chains it to its origin's previous event, and an HMAC of that hash under `LEDGER_HMAC_KEY`. Since hash version 2 the hash covers the whole envelope: origin, sequence, domain, event type and version, aggregate, payload, metadata and creation time. A node quarantines an event whose envelope was changed after signing, and never applies it.

Version 1, the first format, covered the event id and the payload only. Events written under it keep verifying under it, in the stored ledger (`ledger:verify`) and on the wire, and stay as weak as they were written. The version is the envelope's `hash_version` field (absent means 1) and the `events.hash_version` column. A version 2 signature is not accepted as a version 1 signature, so relabelling a new event as an old one does not verify.

**Upgrading a running cluster.** A node that predates version 2 cannot verify version 2 events: it quarantines them and then waits on that origin's sequence. Either stop every node and upgrade them together, or upgrade one node at a time with `LEDGER_HASH_VERSION=1` set on each (it then reads both versions and still writes version 1) and remove the variable once every node runs the new code.

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

Getter/setter DTOs are supported. Ledger payload serialization uses the same getter convention as the core `PayloadSerializer`. An event that keeps its data in public properties would serialise to an empty payload, so the writer refuses it.

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
use Semitexa\Ledger\Domain\Contract\ReplayHandlerInterface;
use Semitexa\Ledger\Domain\Model\LedgerEvent;

#[AsReplayHandler(domain: 'inventory', eventType: 'stock_adjusted')]
final class StockAdjustedReplayHandler implements ReplayHandlerInterface
{
    public function apply(LedgerEvent $event): void
    {
        // Update local projections idempotently using $event->eventId.
    }
}
```

## Check Replication

With the server running on every node:

```bash
bin/semitexa ledger:probe            # on node A: append a probe event
bin/semitexa ledger:status           # on node B: events per origin, pending, unapplied, quarantined
bin/semitexa ledger:status --probe=<probe_id> --json
bin/semitexa system:doctor           # ledger.multi-node: node-local defaults that break a cluster
```

`tests/Harness/two-node/run.sh` boots two full servers (own database, Redis and ledger each) against one JetStream server and checks both directions plus a burst.

The publisher, replayer and command listener run in worker 0 of each server. Set `LEDGER_STREAM` and `LEDGER_SUBJECT_PREFIX` per project when several projects share one NATS server.

`CommandBus` is not registered in the container yet: owner-routed commands wait for the ownership design.

## Example

In `semitexa/demo`, `DemoItemCreated` and `DemoNotificationEvent` (`src/Application/Payload/Event/`) are marked with `#[Propagated(domain: 'demo')]`.

Docs: https://semitexa.com/docs/events/ledger
