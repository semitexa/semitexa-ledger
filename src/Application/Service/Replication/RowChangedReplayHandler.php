<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service\Replication;

use Semitexa\Core\Discovery\ClassDiscovery;
use Semitexa\Ledger\Attribute\AsReplayHandler;
use Semitexa\Ledger\Domain\Contract\ReplayHandlerInterface;
use Semitexa\Ledger\Domain\Model\LedgerEvent;
use Semitexa\Orm\Adapter\DatabaseAdapterInterface;
use Semitexa\Orm\Application\Service\Connection\ConnectionRegistry;

/**
 * Applies replicated row changes from other nodes, each in one transaction of
 * the application database (ADR 0001).
 *
 * Built by the container's resolve() — the server's replayer and the
 * `ledger:replay` command both reach it that way — so its dependencies arrive
 * through the constructor.
 */
#[AsReplayHandler(domain: ReplicationCaptureService::EVENT_DOMAIN, eventType: ReplicationCaptureService::EVENT_TYPE)]
final class RowChangedReplayHandler implements ReplayHandlerInterface
{
    private ?RowChangeApplier $applier = null;

    public function __construct(
        private readonly ConnectionRegistry $connections,
        private readonly ClassDiscovery $discovery,
    ) {}

    public function apply(LedgerEvent $event): void
    {
        $applier = $this->applier ??= new RowChangeApplier(ReplicatedTables::discover($this->discovery));

        $this->connections->manager((string) (getenv('LEDGER_DB_CONNECTION') ?: 'default'))
            ->getTransactionManager()
            ->run(static fn (DatabaseAdapterInterface $db) => $applier->apply($event->payload, $db));
    }
}
