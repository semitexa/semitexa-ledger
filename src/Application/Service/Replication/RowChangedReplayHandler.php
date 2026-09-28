<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service\Replication;

use Semitexa\Ledger\Attribute\AsReplayHandler;
use Semitexa\Ledger\Domain\Contract\ReplayHandlerInterface;
use Semitexa\Ledger\Domain\Model\LedgerEvent;
use Semitexa\Orm\Adapter\DatabaseAdapterInterface;

/**
 * Applies replicated row changes from other nodes, each in one transaction of
 * the application database (ADR 0001). Built by LedgerBootstrap, which hands
 * it the transaction runner — it has no container-resolvable constructor.
 */
#[AsReplayHandler(domain: ReplicationCaptureService::EVENT_DOMAIN, eventType: ReplicationCaptureService::EVENT_TYPE)]
final class RowChangedReplayHandler implements ReplayHandlerInterface
{
    /**
     * @param \Closure(callable(DatabaseAdapterInterface): void): mixed $inTransaction
     */
    public function __construct(
        private readonly \Closure $inTransaction,
        private readonly RowChangeApplier $applier = new RowChangeApplier(),
    ) {}

    public function apply(LedgerEvent $event): void
    {
        ($this->inTransaction)(fn (DatabaseAdapterInterface $db) => $this->applier->apply($event->payload, $db));
    }
}
