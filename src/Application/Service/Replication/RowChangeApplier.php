<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service\Replication;

use Semitexa\Core\Log\StaticLoggerBridge;
use Semitexa\Core\Support\Row;
use Semitexa\Ledger\Application\Service\HybridLogicalClock;
use Semitexa\Ledger\Domain\Model\FieldStamp;
use Semitexa\Ledger\Domain\Model\HlcTimestamp;
use Semitexa\Ledger\Domain\Model\ReplicationConflict;
use Semitexa\Ledger\Domain\Model\RowChangePayload;
use Semitexa\Orm\Adapter\DatabaseAdapterInterface;
use Semitexa\Orm\Application\Service\Persistence\ReplicatedWriteGuard;
use Semitexa\Orm\Adapter\ServerCapability;
use Semitexa\Orm\Adapter\SqlIdentifier;
use Semitexa\Orm\Exception\ConstraintViolationException;

/**
 * Merges a replicated row change from another node into this one (ADR 0001).
 *
 * Every field — and the row's existence, `__exists` — is decided on its own:
 * the incoming value is taken only if its (clock, node) beats the clock stored
 * here. The merge is idempotent, commutative and associative, so nodes that
 * received the same changes hold the same rows in whatever order they came.
 *
 * Writes go straight to the table on the caller's transaction, never through
 * the ORM engine: a merged change must not be captured again and echoed back.
 *
 * A change that breaks a unique or CHECK constraint here is taken in part: the
 * fields that break it are left as they are and journaled with both versions
 * (ADR 0001 §5), every other field it won is applied. Throwing instead rolled
 * the whole change back, the replayer retried it forever, and every later
 * change from its origin queued behind it.
 */
final class RowChangeApplier
{
    /** @var array<string, list<string>> table => its columns; only tables that exist */
    private array $columnsByTable = [];

    /**
     * @param array<string, string> $replicatedTables table => primary-key column, for the
     *        resources this node's code marks #[Replicated] (see ReplicatedTables). The table
     *        and key in an event come off the wire; nothing outside this map is ever written.
     */
    public function __construct(
        private readonly array $replicatedTables,
        private readonly HybridLogicalClock $clock = new HybridLogicalClock(),
    ) {}

    /**
     * @param array<mixed> $payload a ReplicationCaptureService event payload, as received
     * @return list<ReplicationConflict> conflicts journaled for the first time by this
     *         apply — for the caller to announce once its transaction has committed
     */
    public function apply(array $payload, DatabaseAdapterInterface $db): array
    {
        // The applier is the other writer a #[Replicated] table accepts: it
        // writes rows from other nodes, which must not be captured again.
        /** @var list<ReplicationConflict> $conflicts */
        $conflicts = ReplicatedWriteGuard::permit(fn (): array => $this->merge($payload, $db));

        return $conflicts;
    }

    /**
     * @param array<mixed> $payload
     * @return list<ReplicationConflict>
     */
    private function merge(array $payload, DatabaseAdapterInterface $db): array
    {
        try {
            $change = RowChangePayload::fromArray($payload);
            // Decode the key and every value before anything is read or
            // written: a bad binary value found halfway would abort the write
            // and be retried forever, holding back the origin's later events.
            $pk      = RowCodec::decodeKey($change->rowKey);
            $decoded = [];
            foreach ($change->fields as $column => $field) {
                $decoded[$column] = RowCodec::decodeValue($field->value);
            }
        } catch (\UnexpectedValueException|\InvalidArgumentException $e) {
            // Signed by a peer but not a row change: retrying cannot fix it, and
            // half of it is not applied. Refused here, and the event stays in
            // the ledger for inspection.
            StaticLoggerBridge::error('ledger', 'Replicated change refused: malformed payload', ['error' => $e->getMessage()]);
            return [];
        }

        $table    = $change->table;
        $pkColumn = $change->pkColumn;
        $rowKey   = $change->rowKey;
        /** @var array<string, FieldStamp> $incoming */
        $incoming = $change->fields + [ReplicationCaptureService::EXISTS => $change->exists];

        // A node whose clock runs far ahead would win every conflict for as
        // long as its lead lasted; its changes wait (the caller retries)
        // until this node's own time is within the allowed drift. Every clock
        // in the change counts — one future field stamp would otherwise win
        // that field against every ordinary write after it.
        $this->clock->observe($change->latestClock());

        // The table and key come off the wire. Only a table this node's own
        // code marks #[Replicated], keyed by its declared primary key, is
        // written — a change naming any other table (the clock table, a
        // permissions table) is refused, whoever signed it.
        if (($this->replicatedTables[$table] ?? null) !== $pkColumn) {
            StaticLoggerBridge::error('ledger', 'Replicated change refused: not a #[Replicated] table here', [
                'table'     => $table,
                'pk_column' => $pkColumn,
                'origin'    => $change->node,
            ]);
            return [];
        }

        $columns = $this->columnsOf($db, $table);
        if ($columns === null) {
            // Declared but not created yet (the migration has not run). Nothing
            // is stored for it and nothing is cached, so `ledger:replay` after
            // the migration applies it.
            StaticLoggerBridge::warning('ledger', 'Replicated change for a table not created yet; kept in the ledger only', ['table' => $table]);
            return [];
        }

        $clocks = FieldClocks::lockAndRead($db, $table, $rowKey);
        $row    = $this->lockRow($db, $table, $pkColumn, $pk);
        $conflicts = [];

        $won = [];
        foreach ($incoming as $column => $field) {
            if ($column !== ReplicationCaptureService::EXISTS && !in_array($column, $columns, true)) {
                continue; // not here yet; its clock is not stored, so a later replay still applies it
            }
            [$hlc, $node] = $clocks->of($column) ?? [ReplicationCaptureService::ZERO_HLC, ''];
            if ($field->beats(HlcTimestamp::fromString($hlc), $node)) {
                $won[$column] = $field;
            }
        }

        if ($won === []) {
            // Nothing newer than what is here — including a repeat of this very
            // change, or a retry whose unapplied field a newer write overtook.
            ConflictJournal::settle($db, $table, $rowKey, $row !== null);

            return [];
        }

        $exists = isset($won[ReplicationCaptureService::EXISTS])
            ? (bool) $won[ReplicationCaptureService::EXISTS]->value
            : $row !== null;

        $values = [];
        foreach ($won as $column => $field) {
            if ($column !== ReplicationCaptureService::EXISTS) {
                $values[$column] = $decoded[$column];
            }
        }

        if (!$exists) {
            if ($row !== null) {
                Tombstones::bury($db, $table, $rowKey, array_replace($row, $values));
                $this->delete($db, $table, $pkColumn, $pk);
            } elseif ($values !== []) {
                // Already gone here: keep the newer values for a later return.
                $base = Tombstones::exhume($db, $table, $rowKey) ?? [];
                Tombstones::bury($db, $table, $rowKey, array_replace($base, $values));
            }
        } elseif ($row !== null) {
            [$unapplied, $reason] = $this->updateAroundConflicts($db, $table, $pkColumn, $pk, $values);
            if ($unapplied !== []) {
                // Their clocks are not stored: the value here stays what it was
                // and a later change still carrying them is tried again.
                $won = array_diff_key($won, array_flip($unapplied));
                $conflicts[] = $this->conflict($change, $decoded, $unapplied, $row, $clocks, $reason);
            }
        } else {
            // Created, or back from a delete. Fields this change did not win
            // come from what this node last had (the tombstone), else from the
            // change itself — whose clocks for them are then the right ones.
            $base = Tombstones::exhume($db, $table, $rowKey);
            $full = [];
            foreach ($change->fields as $column => $field) {
                if (in_array($column, $columns, true)) {
                    $full[$column] = array_key_exists($column, $values)
                        ? $values[$column]
                        : ($base !== null && array_key_exists($column, $base) ? $base[$column] : $decoded[$column]);
                    if ($base === null && !isset($won[$column]) && $clocks->of($column) === null) {
                        $won[$column] = $field;
                    }
                }
            }
            $full[$pkColumn] = $pk;
            try {
                $this->insert($db, $table, $full);
            } catch (\Throwable $e) {
                if (!self::isConflict($e)) {
                    throw $e;
                }
                // A row cannot exist in part. Nothing of it is kept here — no
                // clocks either, so a later change of it tries the insert again
                // once the conflicting value is gone.
                return $this->journal($db, $payload, [$this->conflict($change, $decoded, [], null, $clocks, $e->getMessage())]);
            }
            Tombstones::remove($db, $table, $rowKey);
        }

        FieldClocks::writeEach(
            $db,
            $table,
            $rowKey,
            array_map(static fn (FieldStamp $field): array => [$field->clock->toString(), $field->node], $won),
        );

        $new = $this->journal($db, $payload, $conflicts);
        // After the journal: a conflict this very change recorded is still open.
        ConflictJournal::settle($db, $table, $rowKey, $exists);

        return $new;
    }

    /**
     * Update the row; if that breaks a constraint, apply the fields one at a
     * time and leave out those that break it.
     *
     * A failed statement is undone on its own — the transaction stays usable —
     * so the fields that do fit still land in this same transaction.
     *
     * @param array<string, mixed> $values
     * @return array{0: list<string>, 1: string} the fields left unapplied, and why
     */
    private function updateAroundConflicts(DatabaseAdapterInterface $db, string $table, string $pkColumn, string $pk, array $values): array
    {
        try {
            $this->update($db, $table, $pkColumn, $pk, $values);

            return [[], ''];
        } catch (\Throwable $e) {
            if (!self::isConflict($e)) {
                throw $e;
            }
            $reason = $e->getMessage();
        }

        $unapplied = [];
        foreach ($values as $column => $value) {
            try {
                $this->update($db, $table, $pkColumn, $pk, [$column => $value]);
            } catch (\Throwable $e) {
                if (!self::isConflict($e)) {
                    throw $e;
                }
                $unapplied[] = $column;
                $reason = $e->getMessage();
            }
        }

        return [$unapplied, $reason];
    }

    /**
     * Only what two nodes can each write legitimately and still disagree on: a
     * unique value, a CHECK bound. A missing parent or a NOT NULL column is not
     * a conflict — the first is delivery order, the second a schema gap — and
     * keeps failing the apply, as before.
     */
    private static function isConflict(\Throwable $e): bool
    {
        if ($e instanceof ConstraintViolationException) {
            // 1062 duplicate entry, 1586 duplicate entry for a named key.
            return in_array($e->driverCode, [1062, 1586], true);
        }

        // MySQL reports a CHECK violation as HY000, outside the 23xxx class the
        // ORM turns into ConstraintViolationException.
        return $e instanceof \PDOException && (int) ($e->errorInfo[1] ?? 0) === 3819;
    }

    /**
     * @param array<string, mixed> $decoded the change's values
     * @param list<string> $columns
     * @param array<string, mixed>|null $row
     */
    private function conflict(RowChangePayload $change, array $decoded, array $columns, ?array $row, FieldClocks $clocks, string $reason): ReplicationConflict
    {
        $incomingClocks = array_map(static fn (FieldStamp $f): array => [$f->clock->toString(), $f->node], $change->fields);
        $localClocks = [];
        foreach (array_keys($row ?? []) as $column) {
            $clock = $clocks->of((string) $column);
            if ($clock !== null) {
                $localClocks[(string) $column] = $clock;
            }
        }

        return new ReplicationConflict(
            table: $change->table,
            rowKey: $change->rowKey,
            columns: $columns,
            incoming: $decoded,
            incomingClocks: $incomingClocks,
            local: $row,
            localClocks: $localClocks,
            originNode: $change->node,
            reason: $reason,
        );
    }

    /**
     * @param array<mixed> $payload the change as received, kept so it can be retried
     * @param list<ReplicationConflict> $conflicts
     * @return list<ReplicationConflict> the ones recorded for the first time
     */
    private function journal(DatabaseAdapterInterface $db, array $payload, array $conflicts): array
    {
        $new = [];
        foreach ($conflicts as $conflict) {
            StaticLoggerBridge::warning('ledger', 'Replicated change applied in part: it breaks a constraint here', [
                'table'   => $conflict->table,
                'row'     => $conflict->rowKey,
                'columns' => $conflict->columns,
                'origin'  => $conflict->originNode,
            ]);
            if (ConflictJournal::record($db, $conflict, $payload)) {
                $new[] = $conflict;
            }
        }

        return $new;
    }

    /** @return list<string>|null */
    private function columnsOf(DatabaseAdapterInterface $db, string $table): ?array
    {
        if (isset($this->columnsByTable[$table])) {
            return $this->columnsByTable[$table];
        }

        $rows = $db->execute(
            'SELECT COLUMN_NAME AS name FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :t',
            ['t' => $table],
        )->rows;
        if ($rows === []) {
            return null; // not cached: the migration may create it later
        }

        return $this->columnsByTable[$table] = array_values(array_map(static fn (array $r): string => Row::of($r)->string('name'), $rows));
    }

    /** @return array<string, mixed>|null */
    private function lockRow(DatabaseAdapterInterface $db, string $table, string $pkColumn, string $pk): ?array
    {
        return $db->execute(
            sprintf(
                'SELECT * FROM %s WHERE %s = :pk LIMIT 1%s',
                SqlIdentifier::quote($table),
                SqlIdentifier::quote($pkColumn),
                $db->supports(ServerCapability::LockingReads) ? ' FOR UPDATE' : '',
            ),
            ['pk' => $pk],
        )->rows[0] ?? null;
    }

    /** @param array<string, mixed> $values */
    private function update(DatabaseAdapterInterface $db, string $table, string $pkColumn, string $pk, array $values): void
    {
        unset($values[$pkColumn]);
        if ($values === []) {
            return;
        }

        $params = ['__pk' => $pk];
        $sets   = [];
        $i      = 0;
        foreach ($values as $column => $value) {
            $sets[] = sprintf('%s = :v%d', SqlIdentifier::quote($column), $i);
            $params["v{$i}"] = $value;
            $i++;
        }

        $db->execute(
            sprintf('UPDATE %s SET %s WHERE %s = :__pk', SqlIdentifier::quote($table), implode(', ', $sets), SqlIdentifier::quote($pkColumn)),
            $params,
        );
    }

    /** @param array<string, mixed> $values */
    private function insert(DatabaseAdapterInterface $db, string $table, array $values): void
    {
        $columns = [];
        $holders = [];
        $params  = [];
        $i       = 0;
        foreach ($values as $column => $value) {
            $columns[] = SqlIdentifier::quote($column);
            $holders[] = ":v{$i}";
            $params["v{$i}"] = $value;
            $i++;
        }

        $db->execute(
            sprintf('INSERT INTO %s (%s) VALUES (%s)', SqlIdentifier::quote($table), implode(', ', $columns), implode(', ', $holders)),
            $params,
        );
    }

    private function delete(DatabaseAdapterInterface $db, string $table, string $pkColumn, string $pk): void
    {
        $db->execute(
            sprintf('DELETE FROM %s WHERE %s = :pk', SqlIdentifier::quote($table), SqlIdentifier::quote($pkColumn)),
            ['pk' => $pk],
        );
    }
}
