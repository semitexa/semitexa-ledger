<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service;

use Semitexa\Core\Log\StaticLoggerBridge;

/**
 * The EventDispatcher post-dispatch hook that appends every dispatched domain
 * event to the local ledger (the durable event store).
 *
 * Failure handling is deliberate. The ledger is decoupled from the dispatch
 * path (see {@see LedgerWriter}) so a ledger hiccup must NOT fail the user's
 * operation — the hook stays non-fatal. But an append failure means the event
 * was dropped from the event store, and there is nothing downstream to recover
 * it: LedgerPublisher only retries events already persisted. The old inline
 * hook buried that in a bare `error_log()`, which flattened a genuine
 * durability loss and a programming error (an OwnershipViolation means the
 * event was mis-routed instead of sent to the owner via CommandBus) into an
 * unstructured stderr line. This logs it LOUDLY and structured instead, so the
 * loss is diagnosable through the normal channel.
 */
final class LedgerDispatchHook
{
    /** @var \Closure(object): mixed */
    private readonly \Closure $append;

    /** @param \Closure(object): mixed $append the ledger append operation (e.g. LedgerWriter::append(...)) */
    public function __construct(\Closure $append)
    {
        $this->append = $append;
    }

    public function __invoke(object $event): void
    {
        try {
            ($this->append)($event);
        } catch (\Throwable $e) {
            StaticLoggerBridge::error(
                'ledger',
                'LedgerWriter::append failed — domain event dropped from the event store',
                [
                    'event'     => $event::class,
                    'exception' => $e::class,
                    'message'   => $e->getMessage(),
                ],
            );
        }
    }
}
