<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service\Replication;

use Semitexa\Core\Discovery\ClassDiscovery;
use Semitexa\Core\Event\EventDispatcherInterface;
use Semitexa\Core\Log\StaticLoggerBridge;
use Semitexa\Ledger\Application\Payload\Event\ReplicationConflictDetected;
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
 *
 * A change that broke a constraint here is announced as
 * ReplicationConflictDetected only after its transaction commits: a listener
 * must never see a conflict the rollback then took back.
 */
#[AsReplayHandler(domain: ReplicationCaptureService::EVENT_DOMAIN, eventType: ReplicationCaptureService::EVENT_TYPE)]
final class RowChangedReplayHandler implements ReplayHandlerInterface
{
    private ?RowChangeApplier $applier = null;

    public function __construct(
        private readonly ConnectionRegistry $connections,
        private readonly ClassDiscovery $discovery,
        private readonly ?EventDispatcherInterface $events = null,
    ) {}

    public function apply(LedgerEvent $event): void
    {
        $applier = $this->applier ??= new RowChangeApplier(ReplicatedTables::discover($this->discovery));

        /** @var list<\Semitexa\Ledger\Domain\Model\ReplicationConflict> $conflicts */
        $conflicts = $this->connections->manager((string) (getenv('LEDGER_DB_CONNECTION') ?: 'default'))
            ->getTransactionManager()
            ->run(static fn (DatabaseAdapterInterface $db): array => $applier->apply($event->payload, $db));

        foreach ($conflicts as $conflict) {
            try {
                $this->events?->dispatch(ReplicationConflictDetected::of($conflict));
            } catch (\Throwable $e) {
                // The change is applied and the conflict journaled; a failing
                // listener must not make the replayer apply it again.
                StaticLoggerBridge::error('ledger', 'A ReplicationConflictDetected listener failed', [
                    'table' => $conflict->table,
                    'row'   => $conflict->rowKey,
                    'error' => $e->getMessage(),
                ]);
            }
        }
    }
}
