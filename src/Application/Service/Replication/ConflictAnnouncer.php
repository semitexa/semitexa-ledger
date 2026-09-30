<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service\Replication;

use Semitexa\Core\Event\EventDispatcherInterface;
use Semitexa\Core\Log\StaticLoggerBridge;
use Semitexa\Ledger\Application\Payload\Event\ReplicationConflictDetected;
use Semitexa\Ledger\Domain\Model\ReplicationConflict;

/**
 * Dispatches ReplicationConflictDetected for conflicts journaled for the first
 * time — only after the transaction that journaled them has committed.
 */
final class ConflictAnnouncer
{
    /** @param list<ReplicationConflict> $conflicts */
    public static function announce(?EventDispatcherInterface $events, array $conflicts): void
    {
        foreach ($conflicts as $conflict) {
            try {
                $events?->dispatch(ReplicationConflictDetected::of($conflict));
            } catch (\Throwable $e) {
                // The change is applied and the conflict journaled; a failing
                // listener must not make the replayer apply it again. What this
                // hides: the conflict is never announced again — its key is
                // journaled, so every replay and retry finds it known. It stays
                // findable as an open row: `replication_conflict` WHERE
                // resolved_at IS NULL, and this error in the log.
                StaticLoggerBridge::error('ledger', 'A ReplicationConflictDetected listener failed', [
                    'table' => $conflict->table,
                    'row'   => $conflict->rowKey,
                    'error' => $e->getMessage(),
                ]);
            }
        }
    }
}
