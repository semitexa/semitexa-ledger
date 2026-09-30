<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service\Replication;

use Semitexa\Core\Attribute\AsService;
use Semitexa\Core\Attribute\InjectAsReadonly;
use Semitexa\Core\Discovery\ClassDiscovery;
use Semitexa\Core\Event\EventDispatcherInterface;
use Semitexa\Ledger\Domain\Model\ConflictRetryOutcome;
use Semitexa\Ledger\Domain\Model\ReplicationConflict;
use Semitexa\Orm\Adapter\DatabaseAdapterInterface;
use Semitexa\Orm\Application\Service\Connection\ConnectionRegistry;

/**
 * Application code's handle on the conflict journal (ADR 0001 §5).
 *
 * A remote change that broke a constraint here is kept out until something
 * changes. When the next change of THAT row arrives, the applier tries it
 * again on its own. But a conflict is usually resolved by writing ANOTHER row
 * here — renaming the account that held the address — and nothing about that
 * write reaches the row that was kept out. After resolving, call retry(): the
 * journaled change is applied again exactly as the replayer would, so a field
 * a newer write has since taken still loses by its clock.
 */
#[AsService]
final class ReplicationConflicts
{
    #[InjectAsReadonly]
    protected ConnectionRegistry $connections;

    #[InjectAsReadonly]
    protected ClassDiscovery $discovery;

    #[InjectAsReadonly]
    protected EventDispatcherInterface $events;

    private ?RowChangeApplier $applier = null;

    /**
     * @param string $conflictKey ReplicationConflict::key(), as the event carried it
     *        or as `replication_conflict.conflict_key` holds it
     */
    public function retry(string $conflictKey): ConflictRetryOutcome
    {
        $applier = $this->applier ??= new RowChangeApplier(ReplicatedTables::discover($this->discovery));

        /** @var array{0: ConflictRetryOutcome, 1: list<ReplicationConflict>} $result */
        $result = $this->connections->manager((string) (getenv('LEDGER_DB_CONNECTION') ?: 'default'))
            ->getTransactionManager()
            ->run(static fn (DatabaseAdapterInterface $db): array => self::retryOn($db, $applier, $conflictKey));

        // A retry can meet a DIFFERENT conflict (another field, another key) —
        // that one is new, and announced like any other. The same one is not.
        ConflictAnnouncer::announce($this->events, $result[1]);

        return $result[0];
    }

    /**
     * The retry itself, on the caller's transaction.
     *
     * @return array{0: ConflictRetryOutcome, 1: list<ReplicationConflict>} the outcome, and
     *         conflicts journaled for the first time by applying the change again
     */
    public static function retryOn(DatabaseAdapterInterface $db, RowChangeApplier $applier, string $conflictKey): array
    {
        $journaled = ConflictJournal::find($db, $conflictKey);
        if ($journaled === null) {
            return [ConflictRetryOutcome::Unknown, []];
        }
        if ($journaled['resolved']) {
            return [ConflictRetryOutcome::AlreadyResolved, []];
        }
        if ($journaled['payload'] === null) {
            return [ConflictRetryOutcome::NotRetryable, []];
        }

        // The applier settles the journal as it goes: resolved_at is set here
        // when the row or its fields land, or a newer write has overtaken them.
        $new = $applier->apply($journaled['payload'], $db);

        $after = ConflictJournal::find($db, $conflictKey);

        return [
            ($after['resolved'] ?? false) ? ConflictRetryOutcome::Resolved : ConflictRetryOutcome::StillOpen,
            $new,
        ];
    }
}
